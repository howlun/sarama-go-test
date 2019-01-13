package business

import "errors"

var (
	ErrRedisClientNotSet                                   error = errors.New("Redis Client not set")
	ErrFleetHTTPClientNotSet                               error = errors.New("Fleet HTTP Client not set")
	ErrRetriveDriverObject                                 error = errors.New("Error retrieving driver object")
	ErrRetriveDriverObjectNotFound                         error = errors.New("Error retrieving driver object, not found")
	ErrInvalidUpdateDriverBusy                             error = errors.New("Set Driver busy not allowed, driver currently on job or currently not available")
	ErrInvalidUpdateDriverCompleteOrCancelDriverNotOnJob   error = errors.New("Set Driver job complete or cancel not allowed, driver currently not on job")
	ErrInvalidUpdateDriverCompleteOrCancelDriverAnotherJob error = errors.New("Set Driver job complete or cancel not allowed, driver is on another job")
)
