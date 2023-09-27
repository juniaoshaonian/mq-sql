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

package domain

import (
	"gorm.io/gorm"
)

type Partition struct {
	gorm.Model
	Value  string `gorm:"column:value;not null"`
	Key    string `gorm:"column:key;not null"`
	Header string `gorm:"column:header;not null"`
	Topic  string `gorm:"column:topic;not null"`
}

type Cursors struct {
	gorm.Model
	Table         string `gorm:"column:table;type:varchar(255);not null"`
	Cursor        int64  `gorm:"column:cursor;type:int(11);not null"`
	ConsumerGroup string `gorm:"column:consumer_group;type:varchar(255);not null"`
	Occupant      string `gorm:"column:occupant;type:varchar(255);not null"`
}

func (c *Cursors) TableName() string {
	return "cursors"
}
