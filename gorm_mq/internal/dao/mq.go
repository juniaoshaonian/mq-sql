package dao

import (
	"context"
	"fmt"
	"github.com/ecodeclub/mq-api"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type MqDAO interface {
	// CreateTopic 创建分区表
	CreateTopic(ctx context.Context, topic string, partitions int) error
	// CreateConsumerGroup 创建消费组
	CreateConsumerGroup(ctx context.Context, groupName string, topic string) error
	// AddMessages 插入消息
	AddMessages(ctx context.Context, msgs []mq.Message) error
	// FindMessages 查找一个分区的消息
	FindMessages(ctx context.Context, topic string, partitionId int64) ([]mq.Message, error)
}

type mqDAO struct {
	db *gorm.DB
}

func (m *mqDAO) CreateTopic(ctx context.Context, topic string, partitions int) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		for i := 0; i < partitions; i++ {
			tableName := fmt.Sprintf("%s_%d", topic, i)
			err := tx.WithContext(ctx).Table(tableName).AutoMigrate(&Partition{})
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
				Cursor:        0,
				ConsumerGroup: groupName,
				Ctime:         ctime,
				UTime:         utime,
			})
		}
		return tx.WithContext(ctx).Model(&Topic{}).Create(cursors).Error
	})
}

func (m *mqDAO) AddMessages(ctx context.Context, msgs []mq.Message) error {
	//TODO implement me
	panic("implement me")
}

func (m *mqDAO) FindMessages(ctx context.Context, topic string, partitionId int64) ([]mq.Message, error) {
	//TODO implement me
	panic("implement me")
}
