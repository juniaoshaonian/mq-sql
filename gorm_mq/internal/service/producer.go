package service

import (
	"context"
	"github.com/ecodeclub/mq-api"
	"mq-sql/gorm_mq/internal/errs"
	"sync"
)

type Producer struct {
	t      *Topic
	closed bool
	mu     sync.RWMutex
}

func (p *Producer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errs.ErrProducerIsClosed
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	err := p.t.addMessage(ctx, m)
	return &mq.ProducerResult{}, err
}

func (p *Producer) ProduceWithPartition(ctx context.Context, m *mq.Message, partition int) (*mq.ProducerResult, error) {p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errs.ErrProducerIsClosed
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	err := p.t.addMessageWithPartition(ctx,m, int64(partition))
	return &mq.ProducerResult{}, err
}

func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}
