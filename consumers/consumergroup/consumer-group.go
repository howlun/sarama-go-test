package consumergroup

import (
	"log"
	"sync"
	"time"

	"github.com/howlun/sarama-go-test/consumers"
	"github.com/wvanbergen/kazoo-go"
	sarama "gopkg.in/Shopify/sarama.v1"
)

type GroupConsumerService struct {
	Config struct {
		saramaConfig    *sarama.Config
		zookeeperConfig *kazoo.Config
		Offsets         struct {
			Initial           int64         // The initial offset method to use if the consumer has no previously stored offset. Must be either sarama.OffsetOldest (default) or sarama.OffsetNewest.
			ProcessingTimeout time.Duration // Time to wait for all the offsets for a partition to be processed after stopping to consume from it. Defaults to 1 minute.
			CommitInterval    time.Duration // The interval between which the processed offsets are commited.
			ResetOffsets      bool          // Resets the offsets for the consumergroup so that it won't resume from where it left off previously.
		}
	}

	consumer  sarama.Consumer
	GroupName string
	kazoo     *kazoo.Kazoo
	group     *kazoo.Consumergroup
	instance  *kazoo.ConsumergroupInstance

	Wg             *sync.WaitGroup
	singleShutdown sync.Once

	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	stopper  chan struct{}

	consumers kazoo.ConsumergroupInstanceList

	offsetManager OffsetManager

	messagehandlerList map[string]*consumers.MessageHandler
	topics             []string
}

func (c *GroupConsumerService) NewConsumer(zookeepers []string) error {
	if c.Config.saramaConfig == nil {
		c.Config.saramaConfig = sarama.NewConfig()
		c.Config.saramaConfig.Version = sarama.V1_0_0_0
		c.Config.saramaConfig.Consumer.Return.Errors = true
	}

	if c.Config.zookeeperConfig == nil {
		c.Config.zookeeperConfig = kazoo.NewConfig()
	}

	c.Config.Offsets.Initial = sarama.OffsetOldest        //Offsets.Initial should be sarama.OffsetOldest or sarama.OffsetNewest.
	c.Config.Offsets.ProcessingTimeout = 60 * time.Second //ZookeeperTimeout should have a duration > 0
	c.Config.Offsets.CommitInterval = 10 * time.Second    //CommitInterval should have a duration >= 0

	if c.Config.saramaConfig.ClientID == "" {
		return consumers.ErrClientIDNotSet
	}

	if len(zookeepers) == 0 {
		return consumers.ErrZookeeperNotSet
	}

	log.Printf("Starting up Group Kafka Consumer with zookeeper: %v\n", zookeepers)

	var err error

	if c.GroupName != "" {
		c.Config.saramaConfig.ClientID = c.GroupName
	} else {
		if c.Config.saramaConfig.ClientID, err = generateConsumerID(); err != nil {
			log.Printf("Error generating Consumer ID: %v\n", err)
			return err
		}
	}
	log.Printf("Consumer ID: %s\n", c.Config.saramaConfig.ClientID)

	log.Printf("Connecting to zookeeper=%v: config=%v\n", zookeepers, c.Config.zookeeperConfig)
	if c.kazoo, err = kazoo.NewKazoo(zookeepers, c.Config.zookeeperConfig); err != nil {
		log.Printf("Error connecting to zookeeper: %v\n", err)
		return err
	}

	brokers, err := c.kazoo.BrokerList()
	if err != nil {
		log.Printf("Error retrieving brokers list: %v\n", err)
		_ = c.kazoo.Close()
		return err
	}
	log.Printf("Retrieved brokers list=%v\n", brokers)

	c.group = c.kazoo.Consumergroup(c.Config.saramaConfig.ClientID)

	if c.Config.Offsets.ResetOffsets {
		err = c.group.ResetOffsets()
		if err != nil {
			log.Printf("Error reseting offsets: %v\n", err)
			_ = c.kazoo.Close()
			return err
		}
		log.Printf("All offsets reset\n")
	}

	c.instance = c.group.NewInstance()

	if c.consumer, err = sarama.NewConsumer(brokers, c.Config.saramaConfig); err != nil {
		log.Printf("Error instantiating new consumer: %v\n", err)
		_ = c.kazoo.Close()
		return err
	}
	log.Printf("Instantiated new consumer with brokers=%v: config=%v\n", brokers, c.Config.saramaConfig)

	//c.messages = make(chan *sarama.ConsumerMessage, c.Config.saramaConfig.ChannelBufferSize)
	c.errors = make(chan *sarama.ConsumerError, c.Config.saramaConfig.ChannelBufferSize)
	c.stopper = make(chan struct{})
	c.messagehandlerList = make(map[string]*consumers.MessageHandler)

	// Register consumer group
	log.Printf("Registering consumer group\n")
	if exists, err := c.group.Exists(); err != nil {
		log.Printf("Error cehcking existing consumer group: %v\n", err)
		_ = c.consumer.Close()
		_ = c.kazoo.Close()
		return err
	} else if !exists {
		log.Printf("Consumer group doesn't exist, creating new..: %s\n", c.group.Name)
		if err = c.group.Create(); err != nil {
			log.Printf("Error creating consumer group in Zookeeper: %v\n", err)
			_ = c.consumer.Close()
			_ = c.kazoo.Close()
			return err
		}
	}

	log.Printf("Group Kafka Consumer Service started: %v\n", brokers)
	return nil
}

func (c *GroupConsumerService) Close() error {
	log.Printf("Closing Group Kafka Consumer Service...\n")

	shutdownError := consumers.ErrAlreadyClosing
	c.singleShutdown.Do(func() {
		defer c.kazoo.Close()

		shutdownError = nil

		close(c.stopper)
		c.Wg.Wait()

		if err := c.offsetManager.Close(); err != nil {
			log.Printf("FAILED closing the offset manager: %s!\n", err)
		}

		if shutdownError = c.instance.Deregister(); shutdownError != nil {
			log.Printf("FAILED deregistering consumer instance: %s!\n", shutdownError)
		} else {
			log.Printf("Deregistered consumer instance %s.\n", c.instance.ID)
		}

		if shutdownError = c.consumer.Close(); shutdownError != nil {
			log.Printf("FAILED closing the Sarama client: %s\n", shutdownError)
		}

		//close(c.messages)
		close(c.errors)
		c.instance = nil
	})

	return shutdownError
}

func (c *GroupConsumerService) RegisterMessageHandler(topic string, callback func(data []byte)) error {
	log.Printf("Registering topics handlers for topic=%s...\n", topic)

	var msgHandler *consumers.MessageHandler
	var ok bool

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error registering topics handlers for topic=%s: %s\n", topic, r)
		}
	}()

	if callback == nil {
		log.Printf("%s:%s...\n", consumers.ErrInvalidCallback.Error(), topic)
		return consumers.ErrInvalidCallback
	}

	if msgHandler, ok = c.messagehandlerList[topic]; !ok {
		msgHandler = consumers.NewMessageHandler()
	}

	msgHandler.Handlers = append(msgHandler.Handlers, callback)
	c.messagehandlerList[topic] = msgHandler

	log.Printf("Topics handlers registered: %s\n", topic)
	return nil
}

func (c *GroupConsumerService) Subscribe() error {
	log.Printf("Subscribing to topics for consumer instance %s...\n", c.instance.ID)

	for t, _ := range c.messagehandlerList {
		c.topics = append(c.topics, t)
	}

	if len(c.topics) == 0 {
		_ = c.consumer.Close()
		_ = c.kazoo.Close()
		return consumers.ErrTopicsNotSet
	}

	// Register itself with zookeeper
	if err := c.instance.Register(c.topics); err != nil {
		log.Printf("Failed to register consumer instance: %v\n", err)
		return err
	} else {
		log.Printf("Consumer instance registered: %s\n", c.instance.ID)
	}

	offsetConfig := OffsetManagerConfig{CommitInterval: c.Config.Offsets.CommitInterval}
	c.offsetManager = NewZookeeperOffsetManager(c, &offsetConfig)

	c.Wg.Add(1)
	go topicListConsumer(c, c.topics)

	return nil
}

func topicListConsumer(c *GroupConsumerService, topics []string) {
	defer c.Wg.Done()

	for {
		select {
		case <-c.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := c.group.WatchInstances()
		if err != nil {
			log.Printf("FAILED to get list of registered consumer instances: %s\n", err)
			return
		}

		c.consumers = consumers
		log.Printf("Currently registered consumers: %d\n", len(c.consumers))

		stopper := make(chan struct{})

		for _, topic := range topics {
			c.Wg.Add(1)
			go topicConsumer(c, topic, c.errors, stopper)
		}

		select {
		case <-c.stopper:
			close(stopper)
			return

		case <-consumerChanges:
			registered, err := c.instance.Registered()
			if err != nil {
				log.Printf("FAILED to get register status: %s\n", err)
			} else if !registered {
				err = c.instance.Register(topics)
				if err != nil {
					log.Printf("FAILED to register consumer instance: %s!\n", err)
				} else {
					log.Printf("Consumer instance registered (%s).", c.instance.ID)
				}
			}

			log.Printf("Triggering rebalance due to consumer list change\n")
			close(stopper)
			c.Wg.Wait()
		}
	}
}

func CommitUpto(c *GroupConsumerService, message *sarama.ConsumerMessage) error {
	c.offsetManager.MarkAsProcessed(message.Topic, message.Partition, message.Offset)
	return nil
}

func topicConsumer(c *GroupConsumerService, topic string, errors chan<- *sarama.ConsumerError, stopper <-chan struct{}) {
	defer c.Wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	log.Printf("%s :: Started topic consumer\n", topic)

	// Fetch a list of partition IDs
	partitions, err := c.kazoo.Topic(topic).Partitions()
	if err != nil {
		log.Printf("%s :: FAILED to get list of partitions: %s\n", topic, err)
		c.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	partitionLeaders, err := retrievePartitionLeaders(partitions)
	if err != nil {
		log.Printf("%s :: FAILED to get leaders of partitions: %s\n", topic, err)
		c.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	dividedPartitions := dividePartitionsBetweenConsumers(c.consumers, partitionLeaders)
	myPartitions := dividedPartitions[c.instance.ID]
	log.Printf("%s :: Claiming %d of %d partitions", topic, len(myPartitions), len(partitionLeaders))

	// Consume all the assigned partitions
	var wg sync.WaitGroup
	for _, pid := range myPartitions {

		wg.Add(1)
		go partitionConsumer(c, topic, pid.ID, errors, &wg, stopper)
	}

	wg.Wait()
	log.Printf("%s :: Stopped topic consumer\n", topic)
}

// Consumes a partition
func partitionConsumer(c *GroupConsumerService, topic string, partition int32, errors chan<- *sarama.ConsumerError, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	maxRetries := int(c.Config.Offsets.ProcessingTimeout/time.Second) + 3
	for tries := 0; tries < maxRetries; tries++ {
		if err := c.instance.ClaimPartition(topic, partition); err == nil {
			break
		} else if err == kazoo.ErrPartitionClaimedByOther && tries+1 < maxRetries {
			time.Sleep(1 * time.Second)
		} else {
			log.Printf("%s/%d :: FAILED to claim the partition: %s\n", topic, partition, err)
			return
		}
	}
	defer c.instance.ReleasePartition(topic, partition)

	nextOffset, err := c.offsetManager.InitializePartition(topic, partition)
	if err != nil {
		log.Printf("%s/%d :: FAILED to determine initial offset: %s\n", topic, partition, err)
		return
	}

	if nextOffset >= 0 {
		log.Printf("%s/%d :: Partition consumer starting at offset %d.\n", topic, partition, nextOffset)
	} else {
		nextOffset = c.Config.Offsets.Initial
		if nextOffset == sarama.OffsetOldest {
			log.Printf("%s/%d :: Partition consumer starting at the oldest available offset.\n", topic, partition)
		} else if nextOffset == sarama.OffsetNewest {
			log.Printf("%s/%d :: Partition consumer listening for new messages only.\n", topic, partition)
		}
	}

	consumer, err := c.consumer.ConsumePartition(topic, partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		log.Printf("%s/%d :: Partition consumer offset out of Range.\n", topic, partition)
		// if the offset is out of range, simplistically decide whether to use OffsetNewest or OffsetOldest
		// if the configuration specified offsetOldest, then switch to the oldest available offset, else
		// switch to the newest available offset.
		if c.Config.Offsets.Initial == sarama.OffsetOldest {
			nextOffset = sarama.OffsetOldest
			log.Printf("%s/%d :: Partition consumer offset reset to oldest available offset.\n", topic, partition)
		} else {
			nextOffset = sarama.OffsetNewest
			log.Printf("%s/%d :: Partition consumer offset reset to newest available offset.\n", topic, partition)
		}
		// retry the consumePartition with the adjusted offset
		consumer, err = c.consumer.ConsumePartition(topic, partition, nextOffset)
	}
	if err != nil {
		log.Printf("%s/%d :: FAILED to start partition consumer: %s\n", topic, partition, err)
		return
	}
	defer consumer.Close()

	var lastOffset int64 = -1 // aka unknown
	consumeMessages(c, consumer, &lastOffset, stopper)

	/*
			err = nil

		partitionConsumerLoop:
			for {
				log.Printf("Consuming partition...\n")
				select {
				case <-stopper:
					log.Printf("Error consuming partition: Stopper triggered\n")
					break partitionConsumerLoop

				case err := <-consumer.Errors():
					log.Printf("Error consuming message: %v\n", err)

					for {
						select {
						case errors <- err:
							continue partitionConsumerLoop

						case <-stopper:
							break partitionConsumerLoop
						}
					}

				case msg := <-consumer.Messages():
					lastOffset = msg.Offset

					messageReceived(msg)


						mh := c.messagehandlerList[topic]
						if mh != nil && len(mh.Handlers) > 0 {
							for _, handler := range mh.Handlers {
								handler(msg.Value)
							}
						}

					if err := CommitUpto(c, msg); err != nil {
						log.Printf("Error commiting offet:%s: %s\n", topic, err)
						break partitionConsumerLoop
					}


					select {
					case <-stopper:
						break partitionConsumerLoop
					}

				}

			}
	*/
	log.Printf("%s/%d :: Stopping partition consumer at offset %d\n", topic, partition, lastOffset)
	if err := c.offsetManager.FinalizePartition(topic, partition, lastOffset, c.Config.Offsets.ProcessingTimeout); err != nil {
		log.Printf("%s/%d :: %s\n", topic, partition, err)
	}

}

func consumeMessages(c *GroupConsumerService, pc sarama.PartitionConsumer, lastOffset *int64, stopper <-chan struct{}) {
	var err error
	//var lastOffset int64 = -1 // aka unknown

	for {
		select {
		case <-stopper:
			log.Printf("Error consuming partition: Stopper triggered\n")
			break
		case err = <-pc.Errors():
			log.Printf("Error consuming message: %v\n", err)
			break
		case msg := <-pc.Messages():

			lastOffset = &msg.Offset

			messageReceived(msg)
			/*
				mh := c.messagehandlerList[topic]
				if mh != nil && len(mh.Handlers) > 0 {
					for _, handler := range mh.Handlers {
						handler(msg.Value)
					}
				}
			*/
			if err := CommitUpto(c, msg); err != nil {
				log.Printf("Error commiting offet:%s: %s\n", msg.Topic, err)
				break
			}
		}
	}
}

func messageReceived(message *sarama.ConsumerMessage) {
	log.Printf("Message Topic=%s Value=%s Offset=%d\n", message.Topic, message.Value, message.Offset)
	//log.Printf("Message Key: %s - Value: %s\n", message.Key, message.Value)
}
