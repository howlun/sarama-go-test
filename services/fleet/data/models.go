package data

type DriverFleetResponseObj struct {
	Code    int32           `json:"code"`
	Message string          `json:"Message"`
	Data    DriverFleetInfo `json:"data"`
}

type DriverFleetInfo struct {
	DriverID            int32 `json:"driverId"`
	ProviderID          int32 `json:"providerId"`
	ActiveServiceID     int32 `json:"activeServiceId"`
	ActiveServiceTypeID int32 `json:"activeServiceTypeId"`
	Priority            int32 `json:"priority"`
}
