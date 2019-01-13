package data

import "errors"

var (
	ErrObjectKeyEmpty           error = errors.New("Object Key is empty")
	ErrObjectIDNotSet           error = errors.New("Object Id is not set")
	ErrFieldMapPropertieNotSet  error = errors.New("Fields Map object is not set")
	ErrNearbySearchRadiusNotSet error = errors.New("Nearby Search radius is not set")
	ErrNearbySearchLimitNotSet error = errors.New("Nearby Search limit is not set")
)
