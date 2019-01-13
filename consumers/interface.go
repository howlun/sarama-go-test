package consumers

type ConsumerService interface {
	NewConsumer(brokers []string) error

	RegisterMessageHandler(topic string, callback func(data []byte)) error

	Subscribe() error

	Close() error
}
