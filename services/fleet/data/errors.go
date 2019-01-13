package data

import "errors"

var (
	ErrHTTPClientNotSet                error = errors.New("HTTP Client not set")
	ErrDriverIDNotSet                  error = errors.New("Driver ID is not set")
	ErrHTTPResponseNotSuccessful       error = errors.New("HTTP Response unsuccessful")
	ErrDriverFleetInfoInvalid          error = errors.New("Driver Fleet info invalid or not found or invalid")
	ErrDriverFleetServiceNotConfigured error = errors.New("Driver has no service info configured")
)
