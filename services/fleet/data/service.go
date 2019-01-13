package data

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/howlun/sarama-go-test/helpers"
	"github.com/howlun/sarama-go-test/services/fleet"
)

type HTTPService interface {
	GetDriverFleetInfo(driverID int32) (*DriverFleetResponseObj, error)
}

type httpService struct {
	client HTTPClient
}

func NewService(client HTTPClient) (HTTPService, error) {
	if client == nil {
		return nil, ErrHTTPClientNotSet
	}
	return &httpService{client: client}, nil
}

func (s *httpService) GetDriverFleetInfo(driverID int32) (*DriverFleetResponseObj, error) {
	respObj := DriverFleetResponseObj{}

	// check if driverID is zero
	if driverID == 0 {
		return nil, ErrDriverIDNotSet
	}

	requestURI := helpers.Concate(fleet.REMOTEADDR, fleet.API_STRING_DRIVERFLEET, "/", helpers.String(driverID))
	log.Printf("Request URI: %s \n", requestURI)
	res, body, _ := s.client.Get(requestURI)
	if res.StatusCode != http.StatusOK {
		return nil, ErrHTTPResponseNotSuccessful
	}

	err := json.Unmarshal([]byte(body), &respObj)
	if err != nil {
		return nil, err
	}
	log.Printf("Request Driver Fleet Info: %d, result: %v\n", driverID, respObj)

	if respObj.Data.DriverID == 0 || respObj.Data.ProviderID == 0 {
		return nil, ErrDriverFleetInfoInvalid
	}

	if respObj.Data.ActiveServiceTypeID == 0 || respObj.Data.ActiveServiceID == 0 {
		return nil, ErrDriverFleetServiceNotConfigured
	}

	return &respObj, nil
}
