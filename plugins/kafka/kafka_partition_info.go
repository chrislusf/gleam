package kafka

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/chrislusf/gleam/gio"
)

type KafkaPartitionInfo struct {
	Brokers        []string
	Topic          string
	Group          string
	TimeoutSeconds int
	PartitionId    int32
}

var (
	MapperReadShard = gio.RegisterMapper(readShard)
)

func init() {
	gob.Register(KafkaPartitionInfo{})
}

func readShard(row []interface{}) error {
	encodedShardInfo := row[0].([]byte)
	return decodeShardInfo(encodedShardInfo).ReadSplit()
}

func (s *KafkaPartitionInfo) ReadSplit() error {

	// println("brokers:", s.Brokers)
	config := sarama.NewConfig()
	config.Net.DialTimeout = time.Duration(s.TimeoutSeconds) * time.Second
	config.Net.ReadTimeout = time.Duration(s.TimeoutSeconds) * time.Second
	config.Net.WriteTimeout = time.Duration(s.TimeoutSeconds) * time.Second

	c, err := sarama.NewClient(s.Brokers, config)
	if err != nil {
		log.Printf("Kafka NewClient brokers %s: %v", s.Brokers, err)
		return err
	}
	defer c.Close()

	offsetManager, err := sarama.NewOffsetManagerFromClient(s.Group, c)
	if err != nil {
		log.Printf("Kafka NewOffsetManagerFromClient error: %v", err)
		return err
	}
	defer offsetManager.Close()

	partitionOffsetManager, err := offsetManager.ManagePartition(s.Topic, s.PartitionId)
	if err != nil {
		log.Printf("Kafka ManagePartition error: %v", err)
		return err
	}
	defer partitionOffsetManager.Close()

	consumer, err := sarama.NewConsumerFromClient(c)
	if err != nil {
		log.Printf("Kafka NewConsumerFromClient error: %v", err)
		return err
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(s.Topic, s.PartitionId, sarama.OffsetOldest)
	if err != nil {
		log.Printf("Kafka Partition %d, error: %v", s.PartitionId, err)
		return err
	}
	defer pc.Close()

	for msg := range pc.Messages() {
		if msg == nil {
			continue
		}

		partitionOffsetManager.MarkOffset(msg.Offset, "")
		ts := msg.Timestamp.UnixNano() / int64(time.Millisecond)
		gio.TsEmit(ts, msg.Value)
	}

	return nil

}

func decodeShardInfo(encodedShardInfo []byte) *KafkaPartitionInfo {
	network := bytes.NewBuffer(encodedShardInfo)
	dec := gob.NewDecoder(network)
	var p KafkaPartitionInfo
	if err := dec.Decode(&p); err != nil {
		log.Fatal("decode shard info", err)
	}
	return &p
}

func encodeShardInfo(shardInfo *KafkaPartitionInfo) []byte {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(shardInfo); err != nil {
		log.Fatal("encode shard info:", err)
	}
	return network.Bytes()
}
