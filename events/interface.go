package events

type MessageEventService interface {
	Register(e MessageEventCodec, cb func()) error
}

type MessageEventCodec interface {
	Encode(value interface{}) ([]byte, error)
	Decode(data []byte) (interface{}, error)
}
