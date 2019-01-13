package location

// LocationObjectType
type LocationObject_Type string

const (
	LocationObject_Type_Point LocationObject_Type = "point"
)

// ObjectCollection
type Object_Collection string

const (
	Object_Collection_Fleet Object_Collection = "fleet"
	Object_Collection_POI   Object_Collection = "poi"
)

// SearchAvailability
type NearbySearch_Availability string

const (
	NearbySearch_Availability_1   NearbySearch_Availability = "1"   // Available
	NearbySearch_Availability_0   NearbySearch_Availability = "0"   // Not Available
	NearbySearch_Availability_All NearbySearch_Availability = "all" // All
)

type ObjectProperties struct {
	Type        string     `json:"type"`
	Coordinates [2]float32 `json:"coordinates"`
}

type ObjectsProperties struct {
	ID       string           `json:"id,omitempty"`
	Object   ObjectProperties `json:"object,omitempty"`
	Fields   []interface{}    `json:"fields,omitempty"`
	Distance float64          `json:"distance,omitempty"` // in meters
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

type FieldsProperties struct {
	DriverID             int32        `json:"driverid"`
	ProviderID           int32        `json:"providerid"`
	Status               DriverStatus `json:"driverstatus"`
	JobID                int32        `json:"jobid"`
	ActiveServiceID      int32        `json:"activeserviceid"`
	ActiveServiceTypeID  int32        `json:"activeservicetypeid"`
	Priority             int32        `json:"priority"`
	LastUpdatedTimestamp int64        `json:"lastupdatedtime"`
}

type FieldsMapProperties map[string]interface{}

type GetObjectResponseObject struct {
	Ok               bool             `json:"ok"`
	ObjectCollection string           `json:"collection,omitempty"`
	Object           ObjectProperties `json:"object,omitempty"`
	Fields           FieldsProperties `json:"fields,omitempty"`
	Error            string           `json:"err,omitempty"`
	Elapsed          string           `json:"elapsed"`
}

type SetFieldResponseObject struct {
	Ok      bool   `json:"ok"`
	Error   string `json:"err,omitempty"`
	Elapsed string `json:"elapsed"`
}

type SetObjectResponseObject struct {
	Ok      bool   `json:"ok"`
	Error   string `json:"err,omitempty"`
	Elapsed string `json:"elapsed"`
}

type WhereConditionFieldProperties struct {
	FieldName string
	Min       interface{}
	Max       interface{}
}

type WhereInConditionFieldProperties struct {
	FieldName string
	Values    []interface{}
}

type NearbyObjectResponseObject struct {
	Ok               bool                `json:"ok"`
	ObjectCollection string              `json:"collection,omitempty"`
	Fields           []string            `json:"fields,omitempty"`
	Objects          []ObjectsProperties `json:"objects,omitempty"`
	Error            string              `json:"err,omitempty"`
	Count            int32               `json:"count"`
	Cursor           int32               `json:"cursor,omitempty"`
	Elapsed          string              `json:"elapsed"`
}

func (o *NearbyObjectResponseObject) RemoveObject(objID string) {
	if objID != "" && len(o.Objects) > 0 {
		foundIndex := -1
		for i, o := range o.Objects {
			if o.ID == objID {
				foundIndex = i
			}
		}

		if foundIndex != -1 {
			o.Objects[foundIndex] = o.Objects[0] // copy first element to the i index
			o.Objects = o.Objects[1:]            // return a copy of array starting from 2 item in the array
		}
	}
}
