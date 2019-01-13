package main

import (
	"flag"
	"log"
	"sync"

	consumers "github.com/howlun/sarama-go-test/consumers"
	ng_processor "github.com/howlun/sarama-go-test/consumers/consumer"
	g_processor "github.com/howlun/sarama-go-test/consumers/consumergroup"
	"github.com/howlun/sarama-go-test/events"
	jobEvent "github.com/howlun/sarama-go-test/events/job"
	locationEvent "github.com/howlun/sarama-go-test/events/location"
	fleetDataSvc "github.com/howlun/sarama-go-test/services/fleet/data"
	locationSvc "github.com/howlun/sarama-go-test/services/location/business"
	locationDataSvc "github.com/howlun/sarama-go-test/services/location/data"
)

var (
	mode      = flag.String("m", "g", "mode: g (Group) or ng (Non-Group")
	groupName = flag.String("group", "", "group name: name of the group")
)

func main() {
	log.Printf("Entering Main\n")
	flag.Parse()

	var wg sync.WaitGroup
	var err error
	var consumer consumers.ConsumerService
	//brokers := []string{"35.240.167.230:9092"}
	brokers := []string{"35.237.203.189:9092", "35.237.203.189:9093", "35.237.203.189:9094"}
	zookeepers := []string{"35.237.203.189:2181"}
	//topics := []string{"ping", "DRIVERASSIGN"}
	_, _ = locationEvent.NewLocationHandlerService()
	_, _ = fleetDataSvc.NewClient()
	_, _ = locationDataSvc.NewClient()
	_, _ = locationSvc.NewLocationService(nil, nil)
	_, _ = jobEvent.NewJobHandlerService(nil)

	switch *mode {
	case "g":
		consumer = &g_processor.GroupConsumerService{Wg: &wg, GroupName: *groupName}
		err = consumer.NewConsumer(zookeepers)
		if err != nil {
			log.Fatalln("Could not create Group Consumer: ", err)
		}
	case "ng":
		consumer = &ng_processor.NonGroupConsumerService{Wg: &wg}
		err = consumer.NewConsumer(brokers)
		if err != nil {
			log.Fatalln("Could not create Non-Group Consumer: ", err)
		}
	}
	defer consumer.Close()

	// register Ping events
	consumer.RegisterMessageHandler(string(events.PingEvent), PingMessageHandler)
	//consumer.RegisterMessageHandler(string(events.PingEvent), PingMessageHandlerAgain)

	// register Location events
	locationEventService, err := locationEvent.NewLocationHandlerService()
	if err != nil {
		log.Fatalln("Failed to instantiate Location Event Handler Service: ", err)
	}
	consumer.RegisterMessageHandler(string(events.DriverArrivingEvent), locationEventService.ArrivingHandler)
	consumer.RegisterMessageHandler(string(events.DriverArrivedEvent), locationEventService.ArrivedHandler)

	// register Job events
	fleetClient, err := fleetDataSvc.NewClient()
	if err != nil {
		log.Fatalln("Failed to instantiate Fleet Client: ", err)
	}
	fleetDataService, err := fleetDataSvc.NewService(fleetClient)
	if err != nil {
		log.Fatalln("Failed to instantiate Fleet Data Service: ", err)
	}
	redisClient, err := locationDataSvc.NewClient()
	if err != nil {
		log.Fatalln("Failed to instantiate Redis Client: ", err)
	}
	locationDataService, err := locationDataSvc.NewRedisService(redisClient)
	if err != nil {
		log.Fatalln("Failed to instantiate Location Data Service: ", err)
	}
	locationService, err := locationSvc.NewLocationService(locationDataService, fleetDataService)
	if err != nil {
		log.Fatalln("Failed to instantiate Location Service: ", err)
	}
	jobEventService, err := jobEvent.NewJobHandlerService(locationService)
	if err != nil {
		log.Fatalln("Failed to instantiate Job Event Handler Service: ", err)
	}
	consumer.RegisterMessageHandler(string(events.DriverAssignedEvent), jobEventService.DriverAssignedHandler)
	consumer.RegisterMessageHandler(string(events.DriverChangedEvent), jobEventService.DriverChangedHandler)
	consumer.RegisterMessageHandler(string(events.DriverAvailabilityChangedEvent), jobEventService.DriverAvailChangedHandler)
	consumer.RegisterMessageHandler(string(events.JobCompletedEvent), jobEventService.JobCompletedHandler)
	consumer.RegisterMessageHandler(string(events.JobCancelledEvent), jobEventService.JobCancelledHandler)

	err = consumer.Subscribe()
	if err != nil {
		log.Fatalln("Could not subscribe to topics: ", err)
	}

	wg.Wait()
	log.Println("Consumer Service is closing...\n")
}

func PingMessageHandler(data []byte) {
	log.Printf("Handling Ping message=%v\n", data)

	var ok bool
	pingEventData := &events.PingEventCodec{}
	v, err := pingEventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Ping message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Ping message=%v\n", v)
		if pingEventData, ok = v.(*events.PingEventCodec); ok {
			log.Printf("Process Ping message=%s\n", pingEventData.OK)
		}
	}
}

func PingMessageHandlerAgain(data []byte) {
	log.Printf("Handling Ping message2=%v\n", data)

	var ok bool
	pingEventData := &events.PingEventCodec{}
	v, err := pingEventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Ping message2: %v\n", err)
	} else {
		log.Printf("Successfully decoded Ping message2=%v\n", v)
		if pingEventData, ok = v.(*events.PingEventCodec); ok {
			log.Printf("Process Ping message2=%s\n", pingEventData.OK)
		}
	}
}
