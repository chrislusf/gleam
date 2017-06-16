package kafka

/*
This file is only for the builder API.
*/

func New(brokers []string, topic string) *KafkaSource {
	return &KafkaSource{
		Brokers:        brokers,
		Topic:          topic,
		TimeoutSeconds: 16,
	}
}

func (s *KafkaSource) Timeout(seconds int) *KafkaSource {
	s.TimeoutSeconds = seconds
	return s
}
