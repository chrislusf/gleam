package kafka

import (
	"os"
	"path/filepath"
)

/*
This file is only for the builder API.
*/

func New(brokers []string, group, topic string) *KafkaSource {
	if group == "" {
		group = filepath.Base(os.Args[0])
	}
	return &KafkaSource{
		Brokers:        brokers,
		Topic:          topic,
		Group:          group,
		TimeoutSeconds: 16,
	}
}

func (s *KafkaSource) Timeout(seconds int) *KafkaSource {
	s.TimeoutSeconds = seconds
	return s
}
