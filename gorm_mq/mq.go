// Copyright 2021 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gorm_mq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-sql/gorm_mq/balancer/equal_divide"
	"github.com/ecodeclub/mq-sql/gorm_mq/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Mq struct {
	Db               *gorm.DB
	producerGetter   NewProducerGetter
	consumerBalancer ConsumerBalancer
	topics           syncx.Map[string, *Topic]
	// 每次至多消费多少
	limit int
	// 抢占超时时间
	timeout time.Duration
	// 续约时间
	interval time.Duration
}

type MqOption func(m *Mq)

type Topic struct {
	Name           string
	partitionNum   int
	lock           sync.RWMutex
	producerGetter ProducerGetter
	partitionList  []string
	//	消费组
	consumerGroups map[string][]*MqConsumer
	closeChs       []chan struct{}
	msgCh          []chan *mq.Message
	once           sync.Once
}

func (m *Mq) Topic(name string, partition int) error {
	partitionList := make([]string, 0, partition)
	for i := 0; i < partition; i++ {
		tableName, err := m.genPartition(name, i)
		if err != nil {
			return err
		}
		partitionList = append(partitionList, tableName)
	}
	t := &Topic{
		Name:           name,
		partitionList:  partitionList,
		partitionNum:   partition,
		closeChs:       make([]chan struct{}, 0, 16),
		msgCh:          make([]chan *mq.Message, 0, 16),
		lock:           sync.RWMutex{},
		producerGetter: m.producerGetter(partition),
		consumerGroups: make(map[string][]*MqConsumer, 32),
	}
	m.topics.Store(name, t)
	return nil
}

func (tp *Topic) Close() error {
	tp.once.Do(func() {
		tp.lock.Lock()
		for _, ch := range tp.msgCh {
			close(ch)
		}
		for _, ch := range tp.closeChs {
			close(ch)
		}
		tp.lock.Unlock()
	})
	return nil
}

func (m *Mq) genPartition(name string, partition int) (tableName string, err error) {
	tableName = fmt.Sprintf("%s_%d", name, partition)
	err = m.Db.Table(tableName).AutoMigrate(&domain.Partition{})
	return tableName, err
}

func (m *Mq) Close() error {
	m.topics.Range(func(key string, value *Topic) bool {
		err := value.Close()
		return err == nil
	})
	return nil
}

func (m *Mq) Producer(topic string) (mq.Producer, error) {
	tp, ok := m.topics.Load(topic)
	if !ok {
		return nil, errors.New("topic 不存在")
	}
	return &MqProducer{
		Topic:  tp,
		DB:     m.Db,
		getter: tp.producerGetter,
	}, nil
}

func (m *Mq) Consumer(topic string, id string) (mq.Consumer, error) {
	tp, ok := m.topics.Load(topic)
	if !ok {
		return nil, errors.New("topic 不存在")
	}
	// 查看有没有之前创建过的消费组
	msgCh := make(chan *mq.Message, 100)
	mqConsumer := &MqConsumer{
		topic:      tp,
		db:         m.Db,
		partitions: make([]int, 0, 32),
		msgCh:      msgCh,
		groupId:    id,
		name:       uuid.New().String(),
		limit:      m.limit,
		timeout:    m.timeout,
		interval:   m.interval,
	}
	// 重新分配consumer对应的分区
	tp.lock.Lock()
	tp.msgCh = append(tp.msgCh, msgCh)
	closeCh := make(chan struct{})
	consumers, ok := tp.consumerGroups[id]
	if !ok {
		consumers = make([]*MqConsumer, 0, 16)
	}
	res := m.consumerBalancer.Balance(tp.partitionNum, len(consumers)+1)
	for i := 0; i < len(consumers); i++ {
		consumers[i].partitions = res[i]
	}
	mqConsumer.partitions = res[len(consumers)]
	consumers = append(consumers, mqConsumer)
	tp.consumerGroups[id] = consumers
	tp.closeChs = append(tp.closeChs, closeCh)
	tp.lock.Unlock()
	// 启动一个goroutine轮询表中数据
	go func() {
		timer := time.NewTicker(2 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
				msgs, err := mqConsumer.getMsgFromDB(ctx)
				if err != nil {
					cancel()
					log.Println(err)
					continue
				}
				cancel()
				for _, msg := range msgs {
					msgCh <- msg
				}
			case <-closeCh:
				return
			}
		}
	}()
	return mqConsumer, nil
}

func NewMq(Db *gorm.DB, opts ...MqOption) (mq.MQ, error) {
	err := Db.AutoMigrate(&domain.Cursors{})
	if err != nil {
		return nil, err
	}
	m := &Mq{
		Db:               Db,
		topics:           syncx.Map[string, *Topic]{},
		consumerBalancer: equal_divide.NewBalancer(),
		producerGetter:   NewGetter,
		limit:            20,
		timeout:          10 * time.Second,
		interval:         2 * time.Second,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m, nil
}

func WithLimit(limit int) MqOption {
	return func(m *Mq) {
		m.limit = limit
	}
}

func WithTimeout(timeout time.Duration) MqOption {
	return func(m *Mq) {
		m.timeout = timeout
	}
}

func WithInterval(interval time.Duration) MqOption {
	return func(m *Mq) {
		m.interval = interval
	}
}