package repository

import (
	"context"
	"encoding/json"
	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/mq-api"
	"mq-sql/gorm_mq/internal/repository/dao"
)

type MqRepository interface {
	// CreateTopic 创建分区表
	CreateTopic(ctx context.Context, topic string, partitions int) error
	// CreateConsumerGroup 创建消费组
	CreateConsumerGroup(ctx context.Context, groupName string, topic string) error
	// AddMessage 插入消息
	AddMessage(ctx context.Context, msg *mq.Message) error
	// FindMessages 消费组查找一个分区的消息
	FindMessages(ctx context.Context, groupId, topic string, partitionId, limit int64) ([]mq.Message, error)
	// UpdateCursor 更新游标
	UpdateCursor(ctx context.Context, groupId, topic string, partitionId, offset int64) error
}

type MqRepo struct {
	mqDao dao.MqDAO
}

func (m *MqRepo) CreateTopic(ctx context.Context, topic string, partitions int) error {
	return m.mqDao.CreateTopic(ctx, topic, partitions)
}

func (m *MqRepo) CreateConsumerGroup(ctx context.Context, groupName string, topic string) error {
	return m.mqDao.CreateConsumerGroup(ctx, groupName, topic)
}

func (m *MqRepo) AddMessage(ctx context.Context, msg *mq.Message) error {
	return m.mqDao.AddMessage(ctx, m.toEntity(msg))
}

func (m *MqRepo) FindMessages(ctx context.Context, groupId, topic string, partitionId, limit int64) ([]mq.Message, error) {
	msgs, err := m.mqDao.FindMessages(ctx, groupId, topic, partitionId, limit)
	if err != nil {
		return nil, err
	}
	res := slice.Map(msgs, func(idx int, src dao.Message) mq.Message {
		return m.toDomain(src)
	})
	return res, nil
}

func (m *MqRepo) UpdateCursor(ctx context.Context, groupId, topic string, partitionId, offset int64) error {
	return m.mqDao.UpdateCursor(ctx, groupId, topic, partitionId, offset)
}

func (m *MqRepo) toEntity(msg *mq.Message) dao.Message {
	headerByte, _ := json.Marshal(msg.Header)
	return dao.Message{
		Value:       msg.Value,
		Key:         msg.Key,
		Header:      string(headerByte),
		Topic:       msg.Topic,
		PartitionID: msg.Partition,
	}
}

func (m *MqRepo) toDomain(msg dao.Message) mq.Message {
	var header mq.Header
	_ = json.Unmarshal([]byte(msg.Header), &header)
	return mq.Message{
		Offset:    msg.ID,
		Value:     msg.Value,
		Key:       msg.Key,
		Header:    header,
		Topic:     msg.Topic,
		Partition: msg.PartitionID,
	}
}
