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
	"encoding/json"

	"github.com/ecodeclub/mq-api"

	"github.com/ecodeclub/mq-sql/gorm_mq/domain"
	"gorm.io/gorm"
)

type MqProducer struct {
	*Topic
	DB     *gorm.DB
	getter ProducerGetter
}

func (m2 *MqProducer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	tableName := m2.Topic.partitionList[m2.getter.Get(string(m.Key))]
	newMsg, err := NewMessage(m)
	if err != nil {
		return nil, err
	}
	newMsg.Topic = m2.Topic.Name
	err = m2.DB.Table(tableName).WithContext(ctx).Create(newMsg).Error
	return nil, err
}

func NewMessage(m *mq.Message) (*domain.Partition, error) {
	val, err := json.Marshal(m.Header)
	if err != nil {
		return nil, err
	}
	return &domain.Partition{
		Value:  string(m.Value),
		Key:    string(m.Key),
		Topic:  m.Topic,
		Header: string(val),
	}, nil
}
