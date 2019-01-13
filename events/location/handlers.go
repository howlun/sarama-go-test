package location

import (
	"log"
)

type LocationHandlerService interface {
	ArrivingHandler(data []byte)
	ArrivedHandler(data []byte)
}

type locationHandlerService struct {
}

func NewLocationHandlerService() (LocationHandlerService, error) {
	return &locationHandlerService{}, nil
}

func (s *locationHandlerService) ArrivingHandler(data []byte) {
	log.Printf("Handling Arriving message=%v\n", data)

	var ok bool
	eventData := &LocationEventCodec{}
	v, err := eventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Location message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Location message=%v\n", v)
		if eventData, ok = v.(*LocationEventCodec); ok {
			log.Printf("Process Location message=%v\n", eventData)
		}
	}

}

func (s *locationHandlerService) ArrivedHandler(data []byte) {
	log.Printf("Handling Arrived message=%v\n", data)

	var ok bool
	eventData := &LocationEventCodec{}
	v, err := eventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Location message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Location message=%v\n", v)
		if eventData, ok = v.(*LocationEventCodec); ok {
			log.Printf("Process Location message=%v\n", eventData)
		}
	}

}
