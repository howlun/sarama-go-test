package data

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/gomodule/redigo/redis"
	"github.com/iknowhtml/sarama-test/helpers"
	"github.com/iknowhtml/sarama-test/services/location"
)

type RedisService interface {
	GetObject(key string, id int32) (*location.GetObjectResponseObject, error)
	SetField(key string, id int32, fields location.FieldsMapProperties) (*location.SetFieldResponseObject, error)
	SetObject(key string, id int32, obj *location.ObjectProperties, fields location.FieldsMapProperties) (*location.SetObjectResponseObject, error)
	NearbyObject(key string, pointLat float32, pointLng float32, radius int32, limit int32, whereList []location.WhereConditionFieldProperties, whereInList []location.WhereInConditionFieldProperties) (*location.NearbyObjectResponseObject, error)
}

type redisService struct {
	client RedisClient
}

func NewRedisService(client RedisClient) (RedisService, error) {
	var err error
	if client != nil {
		log.Printf("On connection: %v\n", client)
	} else {
		client, err = NewClient()
		if err != nil {
			return nil, err
		}
	}
	return &redisService{client: client}, nil
}

func (s *redisService) GetObject(
	key string,
	id int32) (*location.GetObjectResponseObject, error) {

	var respObj location.GetObjectResponseObject
	conn := s.client.GetConn()
	defer conn.Close()

	if key == "" {
		return nil, ErrObjectKeyEmpty
	}
	if id == 0 {
		return nil, ErrObjectIDNotSet
	}

	// generate command args
	objID := GenerateObjectId("", id)
	commandType := "GET"
	commandArgs := []interface{}{key, objID, "WITHFIELDS"}

	// send command to redis to update cache
	log.Printf("Cmd: %s %v\n", commandType, commandArgs)
	res, err := redis.Bytes(conn.Do(commandType, commandArgs...))
	if err != nil {
		return nil, err
	}
	log.Printf("Get Object successful - key: %s, id: %s\n", key, objID)
	//log.Println(res)
	// decode response to json object of ley value pair (string, generic)
	err = json.Unmarshal(res, &respObj)
	if err != nil {
		return nil, err
	}

	log.Println(string(res))
	log.Println(respObj)

	// Return the struct object of result
	respObj.ObjectCollection = key
	return &respObj, nil
}

func (s *redisService) SetField(key string, id int32, fields location.FieldsMapProperties) (*location.SetFieldResponseObject, error) {

	var respObj location.SetFieldResponseObject
	conn := s.client.GetConn()
	defer conn.Close()

	if key == "" {
		return nil, ErrObjectKeyEmpty
	}
	if id == 0 {
		return nil, ErrObjectIDNotSet
	}
	if fields == nil {
		return nil, ErrFieldMapPropertieNotSet
	}

	// generate command args
	objID := GenerateObjectId("", id)
	commandType := "FSET"
	commandArgs := []interface{}{key, objID}
	for k, v := range fields {
		commandArgs = append(commandArgs, k)
		commandArgs = append(commandArgs, v)
	}
	// send command to redis to update cache
	log.Printf("Cmd: %s %v\n", commandType, commandArgs)
	res, err := redis.Bytes(conn.Do(commandType, commandArgs...))
	if err != nil {
		return nil, err
	}
	log.Printf("Set Field successful - key: %s, id: %s\n", key, objID)
	//log.Println(res)
	// decode response to json object of ley value pair (string, generic)
	err = json.Unmarshal(res, &respObj)
	if err != nil {
		return nil, err
	}

	log.Println(string(res))
	log.Println(respObj)
	// Return value is the integer count of how many fields actually changed their values
	return &respObj, nil
}

func (s *redisService) SetObject(key string, id int32, obj *location.ObjectProperties, fields location.FieldsMapProperties) (*location.SetObjectResponseObject, error) {

	var respObj location.SetObjectResponseObject
	conn := s.client.GetConn()
	defer conn.Close()

	if key == "" {
		return nil, errors.New("Key is empty")
	}
	if id == 0 {
		return nil, errors.New("Id is not set")
	}
	if obj == nil {
		return nil, errors.New("Location object is nil")
	}

	// convert LocationObject to JSON
	jsonObj, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	log.Printf("JSON object created: %s\n", string(jsonObj))

	// send command to redis to update cache
	objID := GenerateObjectId("", id)
	commandType := "SET"
	commandArgs := []interface{}{key, objID}
	if fields != nil && len(fields) > 0 {
		for k, v := range fields {
			commandArgs = append(commandArgs, "FIELD")
			commandArgs = append(commandArgs, k)
			commandArgs = append(commandArgs, v)
		}
	}
	// Pass by POINT
	commandArgs = append(commandArgs, "POINT")
	commandArgs = append(commandArgs, obj.Coordinates[0]) // lat
	commandArgs = append(commandArgs, obj.Coordinates[1]) // lng

	log.Printf("Cmd: %s %v\n", commandType, commandArgs)
	res, err := redis.Bytes(conn.Do(commandType, commandArgs...))
	if err != nil {
		return nil, err
	}
	log.Println(string(res))
	log.Printf("Set Object successful - key: %s, id: %s\n", key, objID)

	// decode response to json object of ley value pair (string, generic)
	err = json.Unmarshal(res, &respObj)
	if err != nil {
		return nil, err
	}

	log.Println(respObj)

	// return string 'OK'
	return &respObj, nil
}

func (s *redisService) NearbyObject(key string, pointLat float32, pointLng float32, radius int32, limit int32, whereList []location.WhereConditionFieldProperties, whereInList []location.WhereInConditionFieldProperties) (*location.NearbyObjectResponseObject, error) {

	if key == "" {
		return nil, ErrObjectKeyEmpty
	}
	if radius <= 0 {
		return nil, ErrNearbySearchRadiusNotSet
	}
	if limit <= 0 {
		return nil, ErrNearbySearchLimitNotSet
	}

	var respObj location.NearbyObjectResponseObject
	conn := s.client.GetConn()
	defer conn.Close()

	// send command to redis to update cache
	commandType := "NEARBY"
	commandArgs := []interface{}{key}
	if limit > 0 {
		commandArgs = append(commandArgs, "LIMIT")
		commandArgs = append(commandArgs, limit)
	}

	for _, c := range whereList {
		commandArgs = append(commandArgs, "WHERE")
		commandArgs = append(commandArgs, c.FieldName)
		commandArgs = append(commandArgs, c.Min)
		commandArgs = append(commandArgs, c.Max)
	}

	for _, wi := range whereInList {
		commandArgs = append(commandArgs, "WHEREIN")
		if count := len(wi.Values); count > 0 {
			commandArgs = append(commandArgs, wi.FieldName)
			commandArgs = append(commandArgs, count)
			for _, wiv := range wi.Values {
				commandArgs = append(commandArgs, wiv)
			}
		}
	}

	commandArgs = append(commandArgs, "POINT")
	commandArgs = append(commandArgs, pointLat)
	commandArgs = append(commandArgs, pointLng)
	commandArgs = append(commandArgs, radius)

	log.Printf("Cmd: %s %v\n", commandType, commandArgs)
	res, err := redis.Bytes(conn.Do(commandType, commandArgs...))
	if err != nil {
		return nil, err
	}

	log.Println(string(res))
	log.Printf("Search Nearby successful - key: %s, lat: %f, lng: %f, radius: %d\n", key, pointLat, pointLng, radius)

	// decode response to json object of ley value pair (string, generic)
	err = json.Unmarshal(res, &respObj)
	if err != nil {
		return nil, err
	}

	log.Println(respObj)

	respObj.ObjectCollection = key
	return &respObj, nil
}

func GenerateObjectId(objType string, id int32) string {
	if id == 0 {
		return "*"
	} else {
		if objType != "" {
			return helpers.String(id) + ":" + objType
		} else {
			return helpers.String(id)
		}

	}
}

func GenerateHookName(topic string, key string, objId string) string {
	return topic + "_" + key + "_" + objId
}
