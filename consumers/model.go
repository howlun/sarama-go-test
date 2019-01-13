package consumers

type MessageHandler struct {
	Handlers []func(data []byte)
}

func NewMessageHandler() *MessageHandler {
	return &MessageHandler{}
}
