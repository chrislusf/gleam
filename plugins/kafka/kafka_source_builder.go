package kafka

/*
This file is only for the builder API.
*/

func Brokers(brokers []string, topic string) *KafkaSource {
	return &KafkaSource{
		Brokers:        brokers,
		Topic:          topic,
		TimeoutSeconds: 16,
		Concurrency:    8,
	}
}

func (s *KafkaSource) Timeout(seconds int) *KafkaSource {
	s.TimeoutSeconds = seconds
	return s
}

func (s *KafkaSource) Concurrent(concurrent int) *KafkaSource {
	s.Concurrency = concurrent
	return s
}
