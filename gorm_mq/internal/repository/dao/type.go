package dao

type Message struct {
	ID     int64  `gorm:"primarykey,autoIncrement"`
	Value  []byte `gorm:"type=BLOB"`
	Key    []byte `gorm:"type=BLOB"`
	Header string `gorm:"column:header;not null"`
	Topic       string `gorm:"column:topic;not null"`
	PartitionID int64  `gorm:"partitionId"`
	Ctime       int64
	UTime  int64
}

// 记录所有topic的信息
type Topic struct {
	ID             int64  `gorm:"primarykey,autoIncrement"`
	Name           string `gorm:"column:name"`
	PartitionCount int    `gorm:"column:partition_count"`
	Ctime          int64
	UTime          int64
}

type Cursor struct {
	ID            int64  `gorm:"primarykey,autoIncrement"`
	Topic         string `gorm:"column:topic;type:varchar(255);not null;uniqueIndex:idx_topic_partition_consumer"`
	Partition     int64  `gorm:"column:partition;type:int(11);not null;uniqueIndex:idx_topic_partition_consumer"`
	Offset        int64  `gorm:"column:offset;type:int(11);not null"`
	ConsumerGroup string `gorm:"column:consumer_group;type:varchar(255);not null;uniqueIndex:idx_topic_partition_consumer"`
	Ctime         int64
	UTime         int64
}
