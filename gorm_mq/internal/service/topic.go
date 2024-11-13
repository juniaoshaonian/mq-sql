package service

import (
	"context"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/memory"
	"mq-sql/gorm_mq/internal/errs"
	"mq-sql/gorm_mq/internal/repository"
)

type Topic struct {
	repo                      repository.MqRepository
	partitionCount            int
	producerPartitionIDGetter memory.PartitionIDGetter
	consumerPartitionAssigner memory.ConsumerPartitionAssigner
}

func (t *Topic) addMessage(ctx context.Context, msg *mq.Message) error {
	partitionID := t.producerPartitionIDGetter.PartitionID(string(msg.Key))
	return t.addMessageWithPartition(ctx, msg, partitionID)
}

func (t *Topic) addMessageWithPartition(ctx context.Context, msg *mq.Message, partitionID int64) error {
	if partitionID < 0 || int(partitionID) >= t.partitionCount {
		return errs.ErrInvalidPartition
	}
	msg.Partition = partitionID
	return t.repo.AddMessage(ctx, msg)
}
