package job

import (
	"log"

	"github.com/howlun/sarama-go-test/services/location"
	"github.com/howlun/sarama-go-test/services/location/business"
)

type DriverRegistrationStatus int32

const (
	DriverRegistrationStatus_Registered    DriverRegistrationStatus = 1
	DriverRegistrationStatus_Terminated    DriverRegistrationStatus = 2
	DriverRegistrationStatus_NotRegistered DriverRegistrationStatus = 0
)

type JobHandlerService interface {
	DriverUpdatedHandler(data []byte)
	DriverAssignedHandler(data []byte)
	DriverChangedHandler(data []byte)
	DriverAvailChangedHandler(data []byte)
	JobCompletedHandler(data []byte)
	JobCancelledHandler(data []byte)
}

type jobHandlerService struct {
	locationService business.LocationService
}

func NewJobHandlerService(locationService business.LocationService) (JobHandlerService, error) {
	if locationService == nil {
		return nil, ErrLocationServiceNotSet
	}
	return &jobHandlerService{locationService: locationService}, nil
}

func (s *jobHandlerService) DriverAssignedHandler(data []byte) {
	log.Printf("Handling Driver Assigned message=%v\n", data)

	//var ok bool
	eventData := &DriverAssignedEventCodec{}
	v, err := eventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Driver Assigned message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Driver Assigned message=%v\n", v)
		eventData = v.(*DriverAssignedEventCodec)
		//if eventData, ok = v.(DriverAssignedEventCodec); ok {
		log.Printf("Process Driver Assigned message=%s\n", eventData)

		// TODO push data to channel

		// Update Driver Status to Busy and set JobID
		res, err := s.locationService.SetAvailabilityBusy(eventData.DriverID, eventData.JobID)
		if err != nil {
			log.Printf("Error: DriverAssigned: %v\n", err.Error())

			// TODO send notification to frontend
			return
		}

		log.Printf("Job/DriverAssigned: Success: %v\n", res)

		// TODO send notification to frontend
		//}
	}
}

func (s *jobHandlerService) DriverUpdatedHandler(data []byte) {
	log.Printf("Handling Driver Updated message=%v\n", data)

	//var ok bool
	eventData := &DriverUpdatedEventCodec{}
	v, err := eventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Driver Updated message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Driver Updated message=%v\n", v)
		eventData = v.(*DriverUpdatedEventCodec)
		//if eventData, ok = v.(DriverUpdatedEventCodec); ok {
		log.Printf("Process Driver Updated message=%s\n", eventData)

		// TODO push data to channel

		// check Driver registration status
		if eventData.RegisteredStatusID != int32(DriverRegistrationStatus_Registered) {
			// if not equal to registered, quit processing
			log.Printf("Not processing driver with status=%d\n", eventData.RegisteredStatusID)
			return
		}

		// Update Driver active service and active service type
		res, err := s.locationService.SetServiceAndServiceType(eventData.DriverID, eventData.ActiveServiceID, eventData.ActiveServiceTypeID)
		if err != nil {
			log.Printf("Error: driverUpdated: %v\n", err.Error())

			// TODO send notification to frontend
			return
		}

		if res.Ok != true {
			log.Printf("Error: driverUpdated: update driver failed: %s\n", res.Error)

			// TODO send notification to frontend
			return
		}

		log.Printf("Job/Processor/driverUpdated: Success: %v\n", res)

		// TODO send notification to frontend
		//}
	}
}

func (s *jobHandlerService) DriverChangedHandler(data []byte) {
	log.Printf("Handling Driver Changed message=%v\n", data)

	//var ok bool
	eventData := &DriverChangedEventCodec{}
	v, err := eventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Driver Changed message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Driver Changed message=%v\n", v)
		eventData = v.(*DriverChangedEventCodec)
		//if eventData, ok = v.(DriverChangedEventCodec); ok {
		log.Printf("Process Driver Changed message=%s\n", eventData)

		// TODO push data to channel

		// Update Driver Status to Busy and set JobID
		// Cancel job for old driver
		resOldDriver, err := s.locationService.SetDriverJobCompleteOrCancel(eventData.OldDriverID, eventData.JobID)
		if err != nil {
			log.Printf("Error: driverChanged: %v\n", err.Error())

			// TODO send notification to frontend
			return
		}

		if resOldDriver.Ok != true {
			log.Printf("Error: driverChanged: update old driver failed: %s\n", resOldDriver.Error)

			// TODO send notification to frontend
			return
		}

		// Set Availability Busy for new driver
		resNewDriver, err := s.locationService.SetAvailabilityBusy(eventData.DriverID, eventData.JobID)
		if err != nil {
			log.Printf("Error: driverChanged: %v\n", err.Error())

			// TODO send notification to frontend
			return
		}

		if resNewDriver.Ok != true {
			log.Printf("Error: driverChanged: update new driver failed: %s\n", resNewDriver.Error)

			// TODO send notification to frontend
			return
		}

		log.Printf("Job/Processor/driverChanged: Success: %v\n", resNewDriver)

		// TODO send notification to frontend
		//}
	}
}

func (s *jobHandlerService) DriverAvailChangedHandler(data []byte) {
	log.Printf("Handling Driver Avail Changed message=%v\n", data)

	//var ok bool
	eventData := &DriverAvailabilityChangedEventCodec{}
	v, err := eventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Driver Avail Changed message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Driver Avail Changed message=%v\n", v)
		eventData = v.(*DriverAvailabilityChangedEventCodec)
		//if eventData, ok = v.(DriverAvailabilityChangedEventCodec); ok {
		log.Printf("Process Driver Avail Changed message=%s\n", eventData)

		// TODO push data to channel

		// Update Driver Status to Available or Unavailable
		var avail location.NearbySearch_Availability
		if eventData.Avail == 1 {
			avail = location.NearbySearch_Availability_1
		} else {
			avail = location.NearbySearch_Availability_0
		}

		// Set Availability fo driver
		res, err := s.locationService.SetAvailability(eventData.DriverID, eventData.Lat, eventData.Lng, avail)
		if err != nil {
			log.Printf("Error: driverAvailChanged: %v\n", err.Error())

			// TODO send notification to frontend
			return
		}

		if res.Ok != true {
			log.Printf("Error: driverAvailChanged: update driver availability failed: %s\n", res.Error)

			// TODO send notification to frontend
			return
		}

		log.Printf("Job/Processor/driverAvailChanged: Success: %v\n", res)

		// TODO send notification to frontend
		//}
	}
}

func (s *jobHandlerService) JobCompletedHandler(data []byte) {
	log.Printf("Handling Job Complete message=%v\n", data)

	//var ok bool
	eventData := &JobCompletedEventCodec{}
	v, err := eventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Job Complete message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Job Complete message=%v\n", v)
		eventData = v.(*JobCompletedEventCodec)
		//if eventData, ok = v.(JobCompletedEventCodec); ok {
		log.Printf("Process Job Complete message=%s\n", eventData)

		// TODO push data to channel

		// Update Driver Status to Available and set JobID = 0
		res, err := s.locationService.SetDriverJobCompleteOrCancel(eventData.DriverID, eventData.JobID)
		if err != nil {
			log.Printf("Error: jobCompleted: %v\n", err.Error())

			// TODO send notification to frontend
			return
		}

		log.Printf("Job/Processor/jobCompleted: Success: %v\n", res)

		// TODO send notification to frontend
		//}
	}
}

func (s *jobHandlerService) JobCancelledHandler(data []byte) {
	log.Printf("Handling Job Cancel message=%v\n", data)

	//var ok bool
	eventData := &JobCancelledEventCodec{}
	v, err := eventData.Decode(data)
	if err != nil {
		log.Printf("Error decoding Job Cancel message: %v\n", err)
	} else {
		log.Printf("Successfully decoded Job Cancel message=%v\n", v)
		eventData = v.(*JobCancelledEventCodec)
		//if eventData, ok = v.(JobCancelledEventCodec); ok {
		log.Printf("Process Job Cancel message=%s\n", eventData)

		// TODO push data to channel

		// Update Driver Status to Available and set JobID = 0
		res, err := s.locationService.SetDriverJobCompleteOrCancel(eventData.DriverID, eventData.JobID)
		if err != nil {
			log.Printf("Error: jobCancelled: %v\n", err.Error())

			// TODO send notification to frontend
			return
		}

		log.Printf("Job/Processor/jobCancelled: Success: %v\n", res)

		// TODO send notification to frontend
		//}
	}
}
