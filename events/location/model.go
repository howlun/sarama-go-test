package location

type LocationObject_Type string

const (
	LocationObject_Type_Point LocationObject_Type = "point"
)

type LocationResponseObject struct {
	Type        LocationObject_Type `json:"type"`
	Coordinates [2]float32          `json:"coordinates"`
}

// DriverStatus should match the proto message of driverstatuspoll
type DriverStatus int

const (
	DriverStatus_AVAILABLE    DriverStatus = 1
	DriverStatus_NOTAVAILABLE DriverStatus = 0 // default status
	DriverStatus_BUSY         DriverStatus = 2
)

func (status DriverStatus) String() string {
	// declare a map of int, string

	names := make(map[int]string)
	names[int(DriverStatus_AVAILABLE)] = "Available"
	names[int(DriverStatus_NOTAVAILABLE)] = "Not Available"
	names[int(DriverStatus_BUSY)] = "Busy"

	// prevent panicking in case of
	// `day` is out of range of Weekday
	if status < DriverStatus_NOTAVAILABLE || status > DriverStatus_BUSY {
		return "Unknown"
	}

	// return the name of a Drver Status
	// constant from the names array
	// above.
	return names[int(status)]
}

func (status DriverStatus) CanAcceptJob() bool {
	switch status {
	// status is Available:
	case DriverStatus_AVAILABLE:
		return true
	// else
	default:
		return false
	}
}

type LocationObject_Properties struct {
	DriverID             int32        `json:"driverid"`
	ProviderID           int32        `json:"providerid"`
	Status               DriverStatus `json:"driverstatus"`
	JobID                int32        `json:"jobid"`
	ActiveServiceID      int32        `json:"activeserviceid"`
	ActiveServiceTypeID  int32        `json:"activeservicetypeid"`
	Priority             int32        `json:"priority"`
	LastUpdatedTimestamp int64        `json:"lastupdatedtime"`
}
