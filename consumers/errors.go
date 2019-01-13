package consumers

import "errors"

var ErrInvalidCodec = errors.New("Consumers: Invalid message codec")
var ErrInvalidCallback = errors.New("Consumers: Invalid message callback function")

var ErrAlreadyClosing = errors.New("The consumer group is already shutting down.")
var ErrClientIDNotSet = errors.New("Consumer Client ID not set")
var ErrZookeeperNotSet = errors.New("Zookeeer address is not set")
var ErrTopicsNotSet = errors.New("No topics are set")
var ErrRegisterTopicHandlers = errors.New("Error registering topic handlers")
