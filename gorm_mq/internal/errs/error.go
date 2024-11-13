package errs

import "github.com/pkg/errors"

var (
	ErrConsumerIsClosed = errors.New("消费者已经关闭")
	ErrProducerIsClosed = errors.New("生产者已经关闭")
	ErrMQIsClosed       = errors.New("mq已经关闭")
	ErrInvalidTopic     = errors.New("topic非法")
	ErrInvalidPartition = errors.New("partition非法")
)
