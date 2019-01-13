package data

import (
	"log"

	"github.com/howlun/sarama-go-test/services/fleet"
	"github.com/parnurzeal/gorequest"
)

type HTTPClient interface {
	Get(requestURI string) (gorequest.Response, string, []error)
}

type httpClient struct {
	remoteAddr string
	request    *gorequest.SuperAgent
}

func NewClient() (*httpClient, error) {
	log.Printf("Initializing Fleet Client...\n")

	c := &httpClient{remoteAddr: fleet.REMOTEADDR}
	c.request = gorequest.New()

	/*
		// load system configuration based on environment, singleton pattern
		configuration, err := config.GetInstance("")
		if configuration == nil || err != nil {
			//log.Panicf("Failed to load configuration: %s\n", err.Error())
			return err
		}

		l.remoteAddr = configuration.Locationremoteserver.RemoteAddr
		l.pool = caching.NewPool(l.remoteAddr, "", true)
	*/
	return c, nil
}

func (c *httpClient) Get(requestURI string) (gorequest.Response, string, []error) {
	return c.request.Get(requestURI).End()
}
