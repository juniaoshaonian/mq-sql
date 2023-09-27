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
	"github.com/ecodeclub/mq-sql/gorm_mq/getter/poll"
)

type ProducerGetter interface {
	Get(key string) int64
}

type ConsumerBalancer interface {
	// Balance 返回的是每个消费者他的分区
	Balance(partition int, consumers int) [][]int
}

type NewProducerGetter func(partition int) ProducerGetter

func NewGetter(partition int) ProducerGetter {
	return &poll.Getter{
		Partition: partition,
	}
}
