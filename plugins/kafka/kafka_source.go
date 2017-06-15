package kafka

import (
	"io"
	"time"

	"github.com/Shopify/sarama"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

type KafkaSource struct {
	Brokers        []string
	Topic          string
	TimeoutSeconds int
	Concurrency    int
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *KafkaSource) Generate(f *flow.Flow) *flow.Dataset {
	return s.genShardInfos(f).RoundRobin(s.Concurrency).Mapper(MapperReadShard)
}

func (s *KafkaSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(func(writer io.Writer) error {

		config := sarama.NewConfig()
		config.Net.DialTimeout = time.Duration(s.TimeoutSeconds) * time.Second
		config.Net.ReadTimeout = time.Duration(s.TimeoutSeconds) * time.Second
		config.Net.WriteTimeout = time.Duration(s.TimeoutSeconds) * time.Second

		c, err := sarama.NewClient(s.Brokers, config)
		if err != nil {
			return err
		}
		defer c.Close()

		// the partition ids for a topic
		partitionIds, err := c.Partitions(s.Topic)
		if err != nil {
			return err
		}

		for _, pid := range partitionIds {
			util.WriteRow(writer, encodeShardInfo(&KafkaPartitionInfo{
				Brokers:        s.Brokers,
				Topic:          s.Topic,
				TimeoutSeconds: s.TimeoutSeconds,
				PartitionId:    pid,
			}))
		}

		return nil
	})
}
