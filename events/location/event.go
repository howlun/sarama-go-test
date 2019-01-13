package location

import "encoding/json"

type LocationEventCodec struct {
	Command string                    `json:"command"`
	Detect  string                    `json:"detect"`
	Hook    string                    `json:"hook"`
	Time    string                    `json:"time"`
	Key     string                    `json:"key"`
	ID      string                    `json:"id"`
	Object  LocationResponseObject    `json:"object,omitempty"`
	Fields  LocationObject_Properties `json:"fields,omitempty"`
}

func (c *LocationEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *LocationEventCodec) Decode(data []byte) (interface{}, error) {
	var m LocationEventCodec
	return &m, json.Unmarshal(data, &m)
}
