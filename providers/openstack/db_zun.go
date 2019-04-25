package openstack

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"os"
)

//ues redis DB

func NewRedisClient() (redisClient redis.Conn, err error) {
	// init redis client
	if os.Getenv("REDIS_ADDR") == "" {
		if client, err := redis.Dial("tcp", "localhost:6379"); err == nil {
			redisClient = client
		} else {
			return nil, fmt.Errorf("redis client create error: %s ", err)
		}
	} else {
		redisIP := fmt.Sprintf("%s:6379", os.Getenv("REDIS_ADDR"))
		if client, err := redis.Dial("tcp", redisIP); err == nil {
			redisClient = client
		} else {
			return nil, fmt.Errorf("redis client create error: %s ", err)
		}
	}
	return
}

func addData(key, value string) (err error) {
	client, err := NewRedisClient()
	if err != nil {
		return fmt.Errorf("get redis client error:%s", err)
	}
	defer client.Close()
	_, err = client.Do("SET", key, value)
	if err != nil {
		return fmt.Errorf("add data error:%s", err)
	}
	return nil
}

func deleteByKey(key string) (err error) {
	client, err := NewRedisClient()
	if err != nil {
		return fmt.Errorf("get redis client error:%s", err)
	}
	defer client.Close()
	_, err = client.Do("DEL", key)
	if err != nil {
		return fmt.Errorf("delete data error:%s", err)
	}
	return nil
}

func getByKey(key string) (value string, err error) {
	client, err := NewRedisClient()
	if err != nil {
		return "", fmt.Errorf("get redis client error:%s", err)
	}
	defer client.Close()
	value, err = redis.String(client.Do("GET", key))
	if err != nil {
		return "", fmt.Errorf("get data error:%s", err)
	}
	return value, nil
}

func isExitKey(key string) (flag bool, err error) {
	client, err := NewRedisClient()
	if err != nil {
		return false, fmt.Errorf("get redis client error:%s", err)
	}
	defer client.Close()
	isKeyExit, err := redis.Bool(client.Do("EXISTS", key))
	if err != nil {
		return false, fmt.Errorf("exit data error:%s", err)
	}
	return isKeyExit, nil
}
