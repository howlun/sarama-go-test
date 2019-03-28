package job

import "encoding/json"

type DriverUpdatedEventCodec struct {
	DriverID            int32 `json:"driverId"`
	ProviderID          int32 `json:"providerId"`
	RegisteredStatusID  int32 `json:"status"`
	Priority            int32 `json:"priority"`
	ActiveServiceID     int32 `json:"activeServiceId"`
	ActiveServiceTypeID int32 `json:"activeServiceTypeId"`
}

func (c *DriverUpdatedEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *DriverUpdatedEventCodec) Decode(data []byte) (interface{}, error) {
	var m DriverUpdatedEventCodec
	return &m, json.Unmarshal(data, &m)
}

type DriverAssignedEventCodec struct {
	DriverID int32 `json:"driverId"`
	JobID    int32 `json:"jobId"`
}

func (c *DriverAssignedEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *DriverAssignedEventCodec) Decode(data []byte) (interface{}, error) {
	var m DriverAssignedEventCodec
	return &m, json.Unmarshal(data, &m)
}

type DriverChangedEventCodec struct {
	OldDriverID int32 `json:"oldDriverId"`
	DriverID    int32 `json:"newDriverId"`
	JobID       int32 `json:"jobId"`
}

func (c *DriverChangedEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *DriverChangedEventCodec) Decode(data []byte) (interface{}, error) {
	var m DriverChangedEventCodec
	return &m, json.Unmarshal(data, &m)
}

type JobCompletedEventCodec struct {
	DriverID int32 `json:"driverId"`
	JobID    int32 `json:"jobId"`
}

func (c *JobCompletedEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *JobCompletedEventCodec) Decode(data []byte) (interface{}, error) {
	var m JobCompletedEventCodec
	return &m, json.Unmarshal(data, &m)
}

type JobCancelledEventCodec struct {
	DriverID int32 `json:"driverId"`
	JobID    int32 `json:"jobId"`
}

func (c *JobCancelledEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *JobCancelledEventCodec) Decode(data []byte) (interface{}, error) {
	var m JobCancelledEventCodec
	return &m, json.Unmarshal(data, &m)
}

type DriverAvailabilityChangedEventCodec struct {
	DriverID int32   `json:"driverId"`
	Lat      float32 `json:"lat"`
	Lng      float32 `json:"lng"`
	Avail    int32   `json:"avail"`
}

func (c *DriverAvailabilityChangedEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *DriverAvailabilityChangedEventCodec) Decode(data []byte) (interface{}, error) {
	var m DriverAvailabilityChangedEventCodec
	return &m, json.Unmarshal(data, &m)
}
