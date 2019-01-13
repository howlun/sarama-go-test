package consumer

import (
	"fmt"
	"log"
	"sync"

	"github.com/iknowhtml/sarama-test/consumers"
	sarama "gopkg.in/Shopify/sarama.v1"
)

type NonGroupConsumerService struct {
	client                sarama.Client
	consumer              sarama.Consumer
	partitionConsumerList map[string][]sarama.PartitionConsumer
	currentOffsetList     map[string]int64
	messagehandlerList    map[string]*consumers.MessageHandler
	Wg                    *sync.WaitGroup
}

func (c *NonGroupConsumerService) NewConsumer(brokers []string) error {
	var err error

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true

	// Start with a client
	log.Printf("Starting up Kafka Client: %v\n", brokers)
	c.client, err = sarama.NewClient(brokers, config)
	if err != nil {
		log.Printf("Error starting up Kafka Client=%v: %v\n", brokers, err)
		return err
	}

	log.Printf("Starting up Non-Group Kafka Consumer from Client: %v\n", c.client.Brokers())
	c.consumer, err = sarama.NewConsumerFromClient(c.client)
	if err != nil {
		log.Printf("Error starting up Non-Group Kafka Consumer from Client=%v: %v\n", c.client.Brokers(), err)
		return err
	}

	c.partitionConsumerList = make(map[string][]sarama.PartitionConsumer)
	c.currentOffsetList = make(map[string]int64)
	c.messagehandlerList = make(map[string]*consumers.MessageHandler)

	log.Printf("Kafka Consumer Service started: %v\n", brokers)
	return nil
}

func (c *NonGroupConsumerService) Close() error {
	log.Printf("Closing Non-Group Kafka Consumer Service...\n")

	if len(c.partitionConsumerList) > 0 {
		log.Printf("Closing Partition Consumers...\n")
		for t, pcVal := range c.partitionConsumerList {
			for _, pcTVal := range pcVal {
				pcTVal.AsyncClose()
			}
			delete(c.partitionConsumerList, t)
		}
	}

	if c.consumer != nil {
		log.Printf("Closing Kafka Consumer...\n")
		err := c.consumer.Close()
		if err != nil {
			log.Printf("Error closing Kafka Consumer: %v\n", err)
			return err
		}
	}

	if c.client != nil && !c.client.Closed() {
		log.Printf("Closing Kafka Client: %v\n", c.client.Brokers())
		err := c.client.Close()
		if err != nil {
			log.Printf("Error closing Kafka Client: %v\n", err)
			return err
		}
	}

	return nil
}

func (c *NonGroupConsumerService) RegisterMessageHandler(topic string, callback func(data []byte)) error {
	var msgHandler *consumers.MessageHandler
	var ok bool

	if callback == nil {
		return consumers.ErrInvalidCallback
	}

	if msgHandler, ok = c.messagehandlerList[topic]; !ok {
		msgHandler = consumers.NewMessageHandler()
	}

	msgHandler.Handlers = append(msgHandler.Handlers, callback)
	c.messagehandlerList[topic] = msgHandler

	return nil
}

func (c *NonGroupConsumerService) Subscribe() error {
	for t, mh := range c.messagehandlerList {
		pcL := []sarama.PartitionConsumer{}

		if _, ok := c.currentOffsetList[t]; !ok {
			c.currentOffsetList[t] = sarama.OffsetNewest
		}

		pL, err := retrievePartitions(t, c.consumer, c.currentOffsetList[t])
		if err != nil {
			return err
		}

		if len(pL) == 0 {
			log.Printf("No Partition returned for topic: %s\n", t)
		} else {
			for _, p := range pL {
				pc, err := consumePartition(t, c.consumer, p, c.currentOffsetList[t], mh, c.Wg)
				if err != nil {
					log.Printf("Error consuming Partition for topic: %s\n", t)
				} else {
					pcL = append(pcL, pc)
				}
			}
			c.partitionConsumerList[t] = pcL
		}
	}

	return nil
}

func retrievePartitions(topic string, consumer sarama.Consumer, offset int64) ([]int32, error) {
	log.Printf("Start processing partitions for topic: %s\n", topic)

	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		log.Printf("Error processing partitions for topic: %s: %v\n", topic, err)
		return nil, err
	}

	log.Printf("Partitions for topic: %s: %v\n", topic, partitionList)
	return partitionList, nil
}

func consumePartition(topic string, consumer sarama.Consumer, p int32, offset int64, mh *consumers.MessageHandler, wg *sync.WaitGroup) (sarama.PartitionConsumer, error) {
	log.Printf("Start consuming Partitions for topic=%s: partition=%d: offset=%d\n", topic, p, offset)

	pc, err := consumer.ConsumePartition(topic, p, offset)
	if err != nil {
		log.Printf("Error consuming Partitions for topic=%s: partition=%d: offset=%d: %v\n", topic, p, offset, err)
		return nil, err
	}

	wg.Add(1)
	go consumeMessages(pc, mh, wg)

	return pc, nil
}

func consumeMessages(pc sarama.PartitionConsumer, mh *consumers.MessageHandler, wg *sync.WaitGroup) {
	var err error

	for {
		select {
		case err = <-pc.Errors():
			fmt.Printf("Error consuming message: %v\n", err)
			break
		case msg := <-pc.Messages():
			messageReceived(msg)
			if mh != nil && len(mh.Handlers) > 0 {
				for _, handler := range mh.Handlers {
					handler(msg.Value)
				}
			}
		}
	}

	wg.Done()
}

func messageReceived(message *sarama.ConsumerMessage) {
	log.Printf("Message: %v\n", message)
	//log.Printf("Message Key: %s - Value: %s\n", message.Key, message.Value)
}
