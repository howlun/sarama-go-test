package events

import (
	"encoding/json"
)

type EventName string

const (
	PingEvent                      EventName = "PING"
	DriverArrivingEvent            EventName = "HKDRIVERARRIVING"
	DriverArrivedEvent             EventName = "HKDRIVERARRIVED"
	DriverAssignedEvent            EventName = "DRIVERASSIGN"
	DriverChangedEvent             EventName = "DRIVERCHANGED"
	JobCompletedEvent              EventName = "JOBCOMPLETED"
	JobCancelledEvent              EventName = "JOBCANCELLED"
	DriverAvailabilityChangedEvent EventName = "DRIVERAVAILABILITYCHANGED"
)

type PingEventCodec struct {
	OK string `json:"ok"`
}

func (c *PingEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *PingEventCodec) Decode(data []byte) (interface{}, error) {
	var m PingEventCodec
	return &m, json.Unmarshal(data, &m)
}
