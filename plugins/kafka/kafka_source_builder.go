package kafka

/*
This file is only for the builder API.
*/

func New(brokers []string, topic, group string) *KafkaSource {
	return &KafkaSource{
		Brokers:        brokers,
		Topic:          topic,
		Group:          group,
		TimeoutSeconds: 16,

		prefix: topic,
	}
}

func (s *KafkaSource) Timeout(seconds int) *KafkaSource {
	s.TimeoutSeconds = seconds
	return s
}
