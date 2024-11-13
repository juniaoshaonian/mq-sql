package dao

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type MqDAO interface {
	// CreateTopic 创建分区表
	CreateTopic(ctx context.Context, topic string, partitions int) error
	// CreateConsumerGroup 创建消费组
	CreateConsumerGroup(ctx context.Context, groupName string, topic string) error
	// AddMessage 插入消息
	AddMessage(ctx context.Context, msg *Message) error
	// FindMessages 消费组查找一个分区的消息
	FindMessages(ctx context.Context, groupId, topic string, partitionId, limit int64) ([]Message, error)
	// UpdateCursor 更新游标
	UpdateCursor(ctx context.Context, groupId, topic string, partitionId, offset int64) error
}

type mqDAO struct {
	db *gorm.DB
}

func (m *mqDAO) UpdateCursor(ctx context.Context, groupId, topic string, partitionId, offset int64) error {
	return m.db.WithContext(ctx).
		Where("topic = ? and partition = ? and consumer_group = ?",
			topic, partitionId, groupId).Update("offset", offset).Error
}

func (m *mqDAO) CreateTopic(ctx context.Context, topic string, partitions int) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		for i := 0; i < partitions; i++ {
			tableName := m.getTableName(topic, int64(i))
			err := tx.WithContext(ctx).Table(tableName).AutoMigrate(&Message{})
			if err != nil {
				return err
			}
		}
		return tx.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).
			Create(&Topic{
				Name:           topic,
				PartitionCount: partitions,
			}).Error
	})

}

func (m *mqDAO) CreateConsumerGroup(ctx context.Context, groupName string, topic string) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		var topicModel Topic
		err := tx.WithContext(ctx).Model(&Topic{}).
			Where("name = ?", topic).First(&topicModel).Error
		if err != nil {
			return err
		}
		now := time.Now()
		ctime := now.UnixMilli()
		utime := now.UnixMilli()
		cursors := make([]Cursor, 0, topicModel.PartitionCount)
		for i := 0; i < topicModel.PartitionCount; i++ {
			cursors = append(cursors, Cursor{
				Topic:         topic,
				Partition:     int64(i),
				Offset:        0,
				ConsumerGroup: groupName,
				Ctime:         ctime,
				UTime:         utime,
			})
		}
		return tx.WithContext(ctx).Model(&Topic{}).Create(cursors).Error
	})
}

func (m *mqDAO) AddMessage(ctx context.Context, msg *Message) error {
	now := time.Now().UnixMilli()
	msg.Ctime = now
	msg.UTime = now
	tableName := m.getTableName(msg.Topic, msg.PartitionID)
	return m.db.WithContext(ctx).Table(tableName).Create(msg).Error
}

func (m *mqDAO) FindMessages(ctx context.Context, groupId, topic string, partitionId, limit int64) ([]Message, error) {
	// 寻找游标
	var cursor Cursor
	err := m.db.WithContext(ctx).
		Model(&Cursor{}).
		Where("topic = ? and partition = ? and consumer_group = ? ", topic, partitionId, groupId).
		First(&cursor).Error
	if err != nil {
		return nil, err
	}
	var msgs []Message
	err = m.db.WithContext(ctx).
		Model(&Message{}).
		Where("topic = ? and partition = ? and id > ?", topic, partitionId, cursor.Offset).
		Order("id asc").
		Limit(int(limit)).Find(&msgs).Error
	return msgs, err
}

func (m *mqDAO) getTableName(topic string, partitionId int64) string {
	return fmt.Sprintf("%s_%d", topic, partitionId)
}
