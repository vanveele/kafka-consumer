package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

// Consumer TODO: docs
type Consumer interface {
	StartConsuming() error
	Wait()
	GetMsgChan() <-chan *Message
}

// ConsumerFactory TODO: docs
type ConsumerFactory func(conf map[string]string) (Consumer, error)

var consumerFactories = make(map[string]ConsumerFactory)

// Register TODO: docs
func Register(name string, factory ConsumerFactory) {
	if factory == nil {
		log.Panicf("Consumer factory %s does not exist.", name)
	}
	_, registered := consumerFactories[name]
	if registered {
		fmt.Printf("Consumer factory %s already registered. Ignoring.", name)
	}
	consumerFactories[name] = factory
}

func init() {
	Register("kafka", NewKafkaConsumer)
	Register("stdin", NewRsyslogConsumer)
}

// CreateConsumer TODO: docs
func CreateConsumer(conf map[string]string) (Consumer, error) {
	consumerType, ok := conf["consumer-type"]
	if !ok {
		consumerType = "kafka"
	}

	consumerFactory, ok := consumerFactories[consumerType]
	if !ok {
		// Factory has not been registered.
		// Make a list of all available consumer factories for logging.
		availableConsumers := make([]string, len(consumerFactories))
		for k := range consumerFactories {
			availableConsumers = append(availableConsumers, k)
		}
		return nil, fmt.Errorf("Invalid Consumer type. Must be one of: %s", strings.Join(availableConsumers, ", "))
	}

	return consumerFactory(conf)
}

// KafkaConsumer TODO: docs
type KafkaConsumer struct {
	groupID     string
	seedBrokers []string
	topics      []string
	exitChannel chan bool

	client *cluster.Client

	KafkaStatus KafkaStatus
	config      *cluster.Config

	msgChannel     chan *Message
	consumers      map[string]*cluster.Consumer
	consumersMutex *sync.Mutex
}

// RsyslogConsumer TODO: docs
type RsyslogConsumer struct {
	exitChannel chan bool
	consumer    *io.Reader

	msgChannel chan *Message
}

// KafkaStatus TODO: docs
type KafkaStatus struct {
	Brokers []string

	Topics []string
	// Map: Topic name -> []int32 slice of partitions
	TopicPartitions map[string][]int32
}

// NewKafkaStatus TODO: docs
func NewKafkaStatus() KafkaStatus {
	c := KafkaStatus{}
	c.TopicPartitions = make(map[string][]int32)

	return c
}

// NewRsyslogConsumer TODO: docs
func NewRsyslogConsumer(conf map[string]string) (Consumer, error) {
	var input io.Reader = os.Stdin
	return &RsyslogConsumer{
		consumer:   &input,
		msgChannel: make(chan *Message, 256),
	}, nil
}

func read(r *io.Reader) <-chan *string {
	lines := make(chan *string)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(*r)
		for scan.Scan() {
			s := scan.Text()
			lines <- &s
		}
	}()
	return lines
}

// StartConsuming TODO: docs
func (s *RsyslogConsumer) StartConsuming() error {
	go func(in <-chan *string, out chan<- *Message) {
		for message := range in {
			m := &Message{}
			m.Data = []byte(*message)
			out <- m
		}
	}(read(s.consumer), s.msgChannel)

	log.Printf("Started consuming rsyslog\n")
	return nil
}

// GetMsgChan TODO: docs
func (s *RsyslogConsumer) GetMsgChan() <-chan *Message {
	return s.msgChannel
}

// GetMsgChan TODO: docs
func (s *KafkaConsumer) GetMsgChan() <-chan *Message {
	return s.msgChannel
}

// Wait TODO: docs
func (s *RsyslogConsumer) Wait() {
	<-s.exitChannel
	return
}

// NewKafkaConsumer TODO: docs
func NewKafkaConsumer(conf map[string]string) (Consumer, error) {
	brokerstring, ok := conf["brokers"]
	if !ok {
		return nil, fmt.Errorf("%s is required for the Kafka consumer", "brokers")
	}
	topicString, ok := conf["topics"]
	if !ok {
		return nil, fmt.Errorf("%s is required for the Kafka consumer", "topics")
	}
	brokers := strings.Split(brokerstring, ",")
	topics := strings.Split(topicString, ",")
	groupid, _ := conf["groupid"]
	consumers := make(map[string]*cluster.Consumer)

	kafkaStatus, topicsMetadata, err := FetchKafkaMetadata(brokers, topics)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize Kafka metadata")
	}

	kafkaConfig := cluster.NewConfig()
	kafkaConfig.Group.Return.Notifications = true
	kafkaConfig.Consumer.Return.Errors = true
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := cluster.NewClient(kafkaStatus.Brokers, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating new kafka client: %v", err)
	}

	if kafkaConfig.Validate() != nil {
		return nil, fmt.Errorf("error validating kafka config")
	}

	return &KafkaConsumer{
		groupID:     groupid,
		seedBrokers: brokers,
		topics:      topicsMetadata,
		msgChannel:  make(chan *Message, 256),
		config:      kafkaConfig,
		KafkaStatus: kafkaStatus,
		client:      client,
		consumers:   consumers,
	}, nil
}

// StartConsuming TODO: docs
func (s *KafkaConsumer) StartConsuming() error {
	consumer, err := cluster.NewConsumerFromClient(s.client, s.groupID, s.topics)

	if err != nil {
		fmt.Printf("Error on StartConsumingTopic for topic %s: %+v\n", s.topics, err)
		return err
	}

	go func(notifications <-chan *cluster.Notification) {
		for notification := range notifications {
			fmt.Printf("Notifications: %+v\n", notification)
		}
	}(consumer.Notifications())

	go func(in <-chan *sarama.ConsumerMessage, out chan<- *Message) {
		for message := range in {
			m := &Message{}
			m.Key = string(message.Key)
			m.Topic = message.Topic
			m.Partition = message.Partition
			m.Data = message.Value

			out <- m

			consumer.MarkOffset(message, "")
		}
	}(consumer.Messages(), s.msgChannel)

	// Add return channel here to consume successful message submit
	/*
		  go func(return <-chan *Message) {
			  for message := range return {
				  m.Offset =

					consumer.MarkOffset(, "")
			  }
		  }
	*/

	go func(in <-chan error) {
		for error := range in {
			fmt.Printf("Errors: %s\n", error.Error())
		}
	}(consumer.Errors())

	fmt.Printf("Started consuming topic %s\n", s.topics)

	return nil
}

// Wait TODO: docs
func (s *KafkaConsumer) Wait() {
	<-s.exitChannel
}

// connects to on of a list of brokers
func connectToBroker(config *sarama.Config, seed_brokers []string) (*sarama.Broker, error) {
	var err error
	for _, host := range seed_brokers {
		broker := sarama.NewBroker(host)
		err = broker.Open(config)
		if err != nil {
			log.Errorf("error connecting to broker: %s %v", host, err)
		} else {
			return broker, nil
		}
	}
	return nil, err
}

// FetchKafkaMetadata connects to the broker and fetches current brokers' address and partition ids
func FetchKafkaMetadata(seedBrokers, topics []string) (KafkaStatus, []string, error) {
	kf := NewKafkaStatus()
	found := []string{}

	config := sarama.NewConfig()
	broker, err := connectToBroker(config, seedBrokers)
	if err != nil {
		return kf, nil, err
	}
	request := sarama.MetadataRequest{}
	response, err := broker.GetMetadata(&request)
	if err != nil {
		_ = broker.Close()
		return kf, nil, err
	}

	if len(response.Brokers) == 0 {
		return kf, nil, fmt.Errorf("Unable to find any brokers")
	}

	for _, broker := range response.Brokers {
		kf.Brokers = append(kf.Brokers, broker.Addr())
	}
	fmt.Printf("Brokers: %+v\n", kf.Brokers)

	regexpFilter := regexp.MustCompile(strings.Join(topics, "|"))
	for _, topic := range response.Topics {
		if regexpFilter.MatchString(topic.Name) {
			found = append(found, topic.Name)
			kf.Topics = append(kf.Topics, topic.Name)
			for _, partition := range topic.Partitions {
				kf.TopicPartitions[topic.Name] = append(kf.TopicPartitions[topic.Name], partition.ID)
			}
		}
	}
	return kf, found, broker.Close()
}
