package business

import (
	"errors"
	"log"
	"time"

	fleetSvc "github.com/iknowhtml/sarama-test/services/fleet/data"
	"github.com/iknowhtml/sarama-test/services/location"
	"github.com/iknowhtml/sarama-test/services/location/data"
)

type LocationService interface {
	SetAvailabilityBusy(driverID int32, jobID int32) (*location.SetFieldResponseObject, error)
	SetDriverJobCompleteOrCancel(driverID int32, jobID int32) (*location.SetFieldResponseObject, error)
	SetAvailability(driverID int32, curlocLat float32, curlocLng float32, available location.NearbySearch_Availability) (*location.SetObjectResponseObject, error)
}

type locationService struct {
	dataService  data.RedisService
	fleetService fleetSvc.HTTPService
}

func NewLocationService(dataSvc data.RedisService, fleetSvc fleetSvc.HTTPService) (LocationService, error) {
	if dataSvc == nil {
		return nil, ErrRedisClientNotSet
	}
	if fleetSvc == nil {
		return nil, ErrFleetHTTPClientNotSet
	}
	return &locationService{dataService: dataSvc, fleetService: fleetSvc}, nil
}

// Set Driver Busy and Job ID
// if Driver object not found, return error "faield to retrieve object"
// if Driver status is Busy, return error "Set Driver busy not allowed, driver currently on job"
// if Driver status is Not-Available, return error "Set Driver busy not allowed, driver currently not available"
func (s *locationService) SetAvailabilityBusy(driverID int32, jobID int32) (*location.SetFieldResponseObject, error) {

	// get current driver status
	driverExistObj, err := s.dataService.GetObject(string(location.Object_Collection_Fleet), driverID)
	if err != nil {
		return nil, err
	}

	// check if response object is empty
	if driverExistObj == nil {
		return nil, ErrRetriveDriverObject
	}

	// check if driver object found
	if driverExistObj.Ok != true {
		return nil, ErrRetriveDriverObjectNotFound
	}

	// if Driver status is Busy, throw error "Set Driver busy not allowed, driver currently on job"
	// if Driver status is Not-Available, throw error "Set Driver busy not allowed, driver currently not available"
	if driverExistObj.Ok && driverExistObj.Fields.Status != location.DriverStatus_AVAILABLE {
		return nil, ErrInvalidUpdateDriverBusy
	}

	timeNow := time.Now().Unix()
	fields := location.FieldsMapProperties{}

	// setup param object
	fields = location.FieldsMapProperties{
		"driverstatus":    int32(location.DriverStatus_BUSY),
		"jobid":           jobID,
		"lastupdatedtime": timeNow,
	}

	// Update object
	res, err := s.dataService.SetField(string(location.Object_Collection_Fleet), driverID, fields)
	if err != nil {
		return nil, err
	}

	// check if ok is false
	if res.Ok == false {
		return nil, errors.New(res.Error)
	}

	log.Printf("Driver availability updated: %v\n", res.Ok)
	return res, nil
}

// Set Driver Job Cancel
// if Driver object not found, return error "faield to retrieve object"
// if Driver status not Busy (not on job), return error "Set Driver job cancel not allowed, driver currently not on job"
// if Driver Job ID not match, return error "Set Driver job cancel not allowed, driver is on another job"

// Set Driver Job Complete
// if Driver object not found, return error "faield to retrieve object"
// if Driver status not Busy (not on job), return error "Set Driver job complete or cancel not allowed, driver currently not on job"
// if Driver Job ID not match, return error "Set Driver job complete or cancel not allowed, driver is on another job"
func (s *locationService) SetDriverJobCompleteOrCancel(driverID int32, jobID int32) (*location.SetFieldResponseObject, error) {

	// get current driver status
	driverExistObj, err := s.dataService.GetObject(string(location.Object_Collection_Fleet), driverID)
	if err != nil {
		return nil, err
	}

	// check if response object is empty
	if driverExistObj == nil {
		return nil, ErrRetriveDriverObject
	}

	// check if driver object found
	if driverExistObj.Ok != true {
		return nil, ErrRetriveDriverObjectNotFound
	}

	// if Driver status not Busy (not on job), return error "Set Driver job complete not allowed, driver currently not on job"
	if driverExistObj.Ok && driverExistObj.Fields.Status != location.DriverStatus_BUSY {
		return nil, ErrInvalidUpdateDriverCompleteOrCancelDriverNotOnJob
	}

	// if Driver status not Busy (not on job), return error "Set Driver job complete not allowed, driver currently not on job"
	if driverExistObj.Ok && driverExistObj.Fields.Status == location.DriverStatus_BUSY && driverExistObj.Fields.JobID != jobID {
		return nil, ErrInvalidUpdateDriverCompleteOrCancelDriverAnotherJob
	}

	timeNow := time.Now().Unix()
	fields := location.FieldsMapProperties{}

	// setup param object
	fields = location.FieldsMapProperties{
		"driverstatus":    int32(location.DriverStatus_AVAILABLE),
		"jobid":           0,
		"lastupdatedtime": timeNow,
	}

	// Update object
	res, err := s.dataService.SetField(string(location.Object_Collection_Fleet), driverID, fields)
	if err != nil {
		return nil, err
	}

	// check if ok is false
	if res.Ok == false {
		return nil, errors.New(res.Error)
	}

	log.Printf("Driver Job Compelte or Cancel updated: %v\n", res.Ok)
	return res, nil
}

// Set Driver availability with current location
// if Driver object not found, get Fleet Info to create Driver object
// else update availability and location
func (s *locationService) SetAvailability(driverID int32, curlocLat float32, curlocLng float32, available location.NearbySearch_Availability) (*location.SetObjectResponseObject, error) {

	// get current driver status
	driverExistObj, err := s.dataService.GetObject(string(location.Object_Collection_Fleet), driverID)
	if err != nil {
		return nil, err
	}

	// check if response object is empty
	if driverExistObj == nil {
		return nil, ErrRetriveDriverObject
	}

	var driverStatus location.DriverStatus
	switch available {
	case location.NearbySearch_Availability_1:
		driverStatus = location.DriverStatus_AVAILABLE
	default:
		driverStatus = location.DriverStatus_NOTAVAILABLE
	}

	// get fleet info
	driverFleetInfo, err := s.fleetService.GetDriverFleetInfo(driverID)
	if err != nil {
		return nil, err
	}

	// Construct LocationObject in GeoJSON format
	locationObj := new(location.ObjectProperties)
	locationObj.Type = string(location.LocationObject_Type_Point)
	locationObj.Coordinates = [2]float32{curlocLat, curlocLng}
	timeNow := time.Now().Unix()
	fields := location.FieldsMapProperties{}
	// if driver object exist
	if driverExistObj.Ok && driverExistObj.Fields.DriverID != 0 {
		// if driver is currently busy, throw error: cannot set availability when busy on job
		if driverExistObj.Fields.Status == location.DriverStatus_BUSY {
			return nil, errors.New("cannot change driver availability because driver is busy on job")
		}

		// setup param object
		fields = location.FieldsMapProperties{
			"providerid":          driverFleetInfo.Data.ProviderID,
			"driverstatus":        int32(driverStatus),
			"jobid":               0,
			"activeserviceid":     driverFleetInfo.Data.ActiveServiceID,
			"activeservicetypeid": driverFleetInfo.Data.ActiveServiceTypeID,
			"priority":            driverFleetInfo.Data.Priority,
			"lastupdatedtime":     timeNow,
		}
	} else {

		fields = location.FieldsMapProperties{
			"driverid":            driverID,
			"providerid":          driverFleetInfo.Data.ProviderID,
			"driverstatus":        int32(driverStatus),
			"jobid":               0,
			"activeserviceid":     driverFleetInfo.Data.ActiveServiceID,
			"activeservicetypeid": driverFleetInfo.Data.ActiveServiceTypeID,
			"priority":            driverFleetInfo.Data.Priority,
			"lastupdatedtime":     timeNow,
		}
	}

	// Update object
	res, err := s.dataService.SetObject(string(location.Object_Collection_Fleet), driverID, locationObj, fields)

	if err != nil {
		return nil, err
	}

	// check if ok is false
	if res.Ok == false {
		return nil, errors.New(res.Error)
	}

	log.Printf("Driver availability updated: %v\n", res.Ok)
	return res, nil
}
