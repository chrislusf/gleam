package kafka

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

type KafkaSource struct {
	Brokers        []string
	Group          string
	Topic          string
	TimeoutSeconds int

	prefix string
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *KafkaSource) Generate(f *flow.Flow) *flow.Dataset {
	partitionIds, err := s.fetchPartitionIds()
	if err != nil {
		log.Printf("KafkaSource failed to fetch kafka partitions: %v", err)
		return nil
	}
	return s.genShardInfos(f, partitionIds).
		RoundRobin(s.prefix, len(partitionIds)).
		Map(s.prefix+".Read", MapperReadShard)
}

func (s *KafkaSource) fetchPartitionIds() ([]int32, error) {
	config := sarama.NewConfig()
	config.Net.DialTimeout = time.Duration(s.TimeoutSeconds) * time.Second
	config.Net.ReadTimeout = time.Duration(s.TimeoutSeconds) * time.Second
	config.Net.WriteTimeout = time.Duration(s.TimeoutSeconds) * time.Second

	c, err := sarama.NewClient(s.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to %v: %v", s.Brokers, err)
	}
	defer c.Close()

	// the partition ids for a topic
	partitionIds, err := c.Partitions(s.Topic)
	if err != nil {
		return nil, fmt.Errorf("Failed to list partitions of %v: %v", s.Topic, err)
	}

	return partitionIds, nil
}

func (s *KafkaSource) genShardInfos(f *flow.Flow, partitionIds []int32) *flow.Dataset {
	return f.Source(s.prefix+".list", func(writer io.Writer, stats *pb.InstructionStat) error {

		stats.InputCounter++

		for _, pid := range partitionIds {
			stats.OutputCounter++
			util.NewRow(util.Now(), encodeShardInfo(&KafkaPartitionInfo{
				Brokers:        s.Brokers,
				Topic:          s.Topic,
				Group:          s.Group,
				TimeoutSeconds: s.TimeoutSeconds,
				PartitionId:    pid,
			})).WriteTo(writer)
		}

		return nil
	})
}
