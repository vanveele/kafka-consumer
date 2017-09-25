package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var Brokers = []string{"127.0.0.1:9092"}

func TestConsumer(t *testing.T) {

	_ = KafkaConsumer{}
	assert.Nil(t, nil)
}

func TestNewConsumer(t *testing.T) {
	c, err := CreateConsumer(map[string]string{
		"consumer-type": "kafka",
		"brokers":       strings.Join(Brokers, ","),
		"topics":        "^test$",
		"groupid":       "test"})

	assert.Nil(t, err)
	fmt.Printf("consumer: %+v", c)

	//c.Start()

}

func TestFetchKafkaMetadata(t *testing.T) {

	kafkaStatus, _, err := FetchKafkaMetadata(Brokers, []string{""})
	assert.Nil(t, err)

	fmt.Printf("topics: %+v\n", kafkaStatus.Topics)
	fmt.Printf("brokers: %+v\n", kafkaStatus.Brokers)

	//c.Start()
}

func TestStart(t *testing.T) {
	s, err := CreateConsumer(map[string]string{
		"consumer-type": "kafka",
		"brokers":       strings.Join(Brokers, ","),
		"topics":        "^test$",
		"groupid":       "test"})
	assert.Nil(t, err)

	err = s.StartConsuming()
	assert.Nil(t, err)

	time.Sleep(5 * time.Second)

	//c.Start()
}
