package dao

type Partition struct {
	ID     int64  `gorm:"primarykey,autoIncrement"`
	Value  string `gorm:"column:value;not null"`
	Key    string `gorm:"column:key;not null"`
	Header string `gorm:"column:header;not null"`
	Topic  string `gorm:"column:topic;not null"`
	Ctime  int64
	UTime  int64
}

type Cursors struct {
	ID            int64  `gorm:"primarykey,autoIncrement"`
	Topic         string `gorm:"column:topic;type:varchar(255);not null"`
	Partition     int64  `gorm:"column:partition;type:int(11);not null"`
	Cursor        int64  `gorm:"column:cursor;type:int(11);not null"`
	ConsumerGroup string `gorm:"column:consumer_group;type:varchar(255);not null"`
	Ctime         int64
	UTime         int64
}

func (c *Cursors) TableName() string {
	return "cursors"
}
