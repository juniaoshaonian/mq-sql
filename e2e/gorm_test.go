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

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-sql/gorm_mq"
	"github.com/ecodeclub/mq-sql/gorm_mq/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type GormMQSuite struct {
	suite.Suite
	dsn string
	db  *gorm.DB
}

//func (g *GormMQSuite) SetupTest() {
//	db, err := gorm.Open(mysql.Open(g.dsn), &gorm.Config{
//		//Logger: logger.Default.LogMode(logger.Info),
//	})
//	require.NoError(g.T(), err)
//	g.db = db
//}

//func (g *GormMQSuite) TearDownTest() {
//	sqlDB, err := g.db.DB()
//	require.NoError(g.T(), err)
//	_, err = sqlDB.Exec("DROP DATABASE IF EXISTS `test`")
//	require.NoError(g.T(), err)
//	_, err = sqlDB.Exec("CREATE DATABASE  `test`")
//	require.NoError(g.T(), err)
//
//}

func (g *GormMQSuite) TestTopic() {
	testcases := []struct {
		name       string
		topic      string
		input      int
		wantVal    []string
		beforeFunc func()
		afterFunc  func()
	}{
		{
			name:    "建立含有4个分区的topic",
			topic:   "test_topic",
			input:   4,
			wantVal: []string{"test_topic_0", "test_topic_1", "test_topic_2", "test_topic_3"},
			beforeFunc: func() {
				db, err := gorm.Open(mysql.Open(g.dsn), &gorm.Config{
					//Logger: logger.Default.LogMode(logger.Info),
				})
				require.NoError(g.T(), err)
				g.db = db
			},
			afterFunc: func() {
				sqlDB, err := g.db.DB()
				require.NoError(g.T(), err)
				_, err = sqlDB.Exec("DROP DATABASE IF EXISTS `test`")
				require.NoError(g.T(), err)
				_, err = sqlDB.Exec("CREATE DATABASE  `test`")
				require.NoError(g.T(), err)
			},
		},
	}
	for _, tc := range testcases {
		g.T().Run(tc.name, func(t *testing.T) {
			tc.beforeFunc()
			mq, err := gorm_mq.NewMq(g.db)
			require.NoError(t, err)
			err = mq.Topic(tc.topic, 4)
			require.NoError(t, err)
			tables, err := g.getTables(tc.topic)
			require.NoError(t, err)
			assert.Equal(t, tc.wantVal, tables)
			tc.afterFunc()
		})
	}

}

func (g *GormMQSuite) TestConsumer() {
	testcases := []struct {
		name       string
		topic      string
		input      []*mq.Message
		partitions int64
		consumers  func(mq mq.MQ) []mq.Consumer
		// 处理消息
		consumerFunc func(c mq.Consumer) []*mq.Message
		wantVal      []*mq.Message
		beforeFunc   func()
		afterFunc    func()
	}{
		{
			name:       "一个消费组内多个消费者",
			topic:      "test_topic",
			partitions: 2,
			input: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
				},
			},
			consumers: func(mqm mq.MQ) []mq.Consumer {
				c11, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c12, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c13, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				return []mq.Consumer{
					c11,
					c12,
					c13,
				}
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(g.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
			wantVal: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic",
				},
			},
			beforeFunc: func() {
				db, err := gorm.Open(mysql.Open(g.dsn), &gorm.Config{
					//Logger: logger.Default.LogMode(logger.Info),
				})
				require.NoError(g.T(), err)
				g.db = db
			},
			afterFunc: func() {
				sqlDB, err := g.db.DB()
				require.NoError(g.T(), err)
				_, err = sqlDB.Exec("DROP DATABASE IF EXISTS `test`")
				require.NoError(g.T(), err)
				_, err = sqlDB.Exec("CREATE DATABASE  `test`")
				require.NoError(g.T(), err)
			},
		},
		{
			name:       "多个消费组，多个消费者",
			topic:      "test_topic",
			partitions: 3,
			input: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
				},
			},
			consumers: func(mqm mq.MQ) []mq.Consumer {
				c11, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c12, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c13, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c21, err := mqm.Consumer("test_topic", "c2")
				require.NoError(g.T(), err)
				c22, err := mqm.Consumer("test_topic", "c2")
				require.NoError(g.T(), err)
				c23, err := mqm.Consumer("test_topic", "c2")
				require.NoError(g.T(), err)
				return []mq.Consumer{
					c11,
					c12,
					c13,
					c21,
					c22,
					c23,
				}
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(g.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
			wantVal: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic",
				},
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic",
				},
			},
			beforeFunc: func() {
				db, err := gorm.Open(mysql.Open(g.dsn), &gorm.Config{
					//Logger: logger.Default.LogMode(logger.Info),
				})
				require.NoError(g.T(), err)
				g.db = db
			},
			afterFunc: func() {
				sqlDB, err := g.db.DB()
				require.NoError(g.T(), err)
				_, err = sqlDB.Exec("DROP DATABASE IF EXISTS `test`")
				require.NoError(g.T(), err)
				_, err = sqlDB.Exec("CREATE DATABASE  `test`")
				require.NoError(g.T(), err)
			},
		},
	}
	for _, tc := range testcases {
		g.T().Run(tc.name, func(t *testing.T) {
			tc.beforeFunc()
			gormMq, err := gorm_mq.NewMq(g.db)
			require.NoError(t, err)
			err = gormMq.Topic(tc.topic, int(tc.partitions))
			require.NoError(t, err)
			p, err := gormMq.Producer(tc.topic)
			require.NoError(t, err)
			consumers := tc.consumers(gormMq)
			ans := make([]*mq.Message, 0, len(tc.wantVal))
			var wg sync.WaitGroup
			locker := sync.RWMutex{}
			for _, c := range consumers {
				newc := c
				wg.Add(1)
				go func() {
					defer wg.Done()
					msgs := tc.consumerFunc(newc)
					locker.Lock()
					ans = append(ans, msgs...)
					locker.Unlock()
				}()
			}
			for _, msg := range tc.input {
				_, err := p.Produce(context.Background(), msg)
				require.NoError(t, err)
			}
			time.Sleep(5 * time.Second)
			err = gormMq.Close()
			require.NoError(t, err)
			wg.Wait()
			assert.ElementsMatch(t, tc.wantVal, ans)
			// 清理测试环境
			tc.afterFunc()
		})
	}
}

// 测试一个分区内的消息是有序的
func (g *GormMQSuite) TestConsumer_Sort() {
	testcases := []struct {
		name       string
		topic      string
		input      []*mq.Message
		partitions int64
		consumers  func(mq mq.MQ) []mq.Consumer
		// 处理消息
		consumerFunc func(c mq.Consumer) []*mq.Message
		wantVal      []*mq.Message
		beforeFunc   func()
		afterFunc    func()
	}{
		{
			name:       "消息有序",
			topic:      "test_topic",
			partitions: 3,
			input: []*mq.Message{
				{
					Key:   []byte("1"),
					Value: []byte("1"),
				},
				{
					Key:   []byte("1"),
					Value: []byte("2"),
				},
				{
					Key:   []byte("1"),
					Value: []byte("3"),
				},
				{
					Key:   []byte("1"),
					Value: []byte("4"),
				},
				{
					Key:   []byte("4"),
					Value: []byte("1"),
				},
				{
					Key:   []byte("4"),
					Value: []byte("2"),
				},
				{
					Key:   []byte("4"),
					Value: []byte("3"),
				},
				{
					Key:   []byte("4"),
					Value: []byte("4"),
				},
			},
			consumers: func(mqm mq.MQ) []mq.Consumer {
				c11, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c12, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c13, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				return []mq.Consumer{
					c11,
					c12,
					c13,
				}
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(g.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
			wantVal: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("2"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("3"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("4"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("1"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("2"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("3"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
			},
			beforeFunc: func() {
				db, err := gorm.Open(mysql.Open(g.dsn), &gorm.Config{
					//Logger: logger.Default.LogMode(logger.Info),
				})
				require.NoError(g.T(), err)
				g.db = db
			},
			afterFunc: func() {
				sqlDB, err := g.db.DB()
				require.NoError(g.T(), err)
				_, err = sqlDB.Exec("DROP DATABASE IF EXISTS `test`")
				require.NoError(g.T(), err)
				_, err = sqlDB.Exec("CREATE DATABASE  `test`")
				require.NoError(g.T(), err)
			},
		},
	}
	for _, tc := range testcases {
		g.T().Run(tc.name, func(t *testing.T) {
			tc.beforeFunc()
			gormMq, err := gorm_mq.NewMq(g.db)
			require.NoError(t, err)
			err = gormMq.Topic(tc.topic, int(tc.partitions))
			require.NoError(t, err)
			p, err := gormMq.Producer(tc.topic)
			require.NoError(t, err)
			consumers := tc.consumers(gormMq)
			ans := make([]*mq.Message, 0, len(tc.wantVal))
			var wg sync.WaitGroup
			locker := sync.RWMutex{}
			for _, c := range consumers {
				newc := c
				wg.Add(1)
				go func() {
					defer wg.Done()
					msgs := tc.consumerFunc(newc)
					locker.Lock()
					ans = append(ans, msgs...)
					locker.Unlock()
				}()
			}
			for _, msg := range tc.input {
				_, err := p.Produce(context.Background(), msg)
				require.NoError(t, err)
			}
			time.Sleep(5 * time.Second)
			err = gormMq.Close()
			require.NoError(t, err)
			wg.Wait()
			wantMap := getMsgMap(tc.wantVal)
			actualMap := getMsgMap(ans)
			assert.Equal(t, wantMap, actualMap)
			// 清理测试环境
			tc.afterFunc()
		})
	}
}

func getMsgMap(msgs []*mq.Message) map[string][]*mq.Message {
	wantMap := make(map[string][]*mq.Message, 10)
	for _, val := range msgs {
		_, ok := wantMap[string(val.Key)]
		if !ok {
			wantMap[string(val.Key)] = append([]*mq.Message{}, val)
		} else {
			wantMap[string(val.Key)] = append(wantMap[string(val.Key)], val)
		}
	}
	return wantMap
}

func (g *GormMQSuite) getTables(prefix string) ([]string, error) {
	sqlDB, err := g.db.DB()
	if err != nil {
		return nil, err
	}
	tableNames := make([]string, 0, 32)
	rows, err := sqlDB.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE ?", "test", fmt.Sprintf("%s%%", prefix))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, nil
}

func (g *GormMQSuite) getValues(topic string, partition int) ([]*domain.Partition, error) {
	ans := make([]*domain.Partition, 0, 32)
	for i := 0; i < partition; i++ {
		tableName := fmt.Sprintf("%s_%d", topic, i)
		var ps []*domain.Partition
		g.db.Table(tableName).Find(&ps)

		ans = append(ans, ps...)
	}
	return ans, nil
}

func TestMq(t *testing.T) {
	suite.Run(t, &GormMQSuite{
		dsn: "root:root@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local",
	})
}
