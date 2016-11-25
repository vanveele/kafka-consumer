package main

import (
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/buger/jsonparser"
	"github.com/urfave/cli"
)

var (
	stats, _ = statsd.New("127.0.0.1:8125")
)

func init() {
	app.Commands = append(app.Commands,
		cli.Command{
			Name:  "datadog",
			Usage: "forward messages to datadog",
			Action: func(c *cli.Context) {
				consumer, err := CreateConsumer(map[string]string{
					"consumer-type": "kafka",
					"brokers":       globalFlags.Brokers,
					"topics":        globalFlags.Topic,
					"groupid":       globalFlags.Groupid,
				})
				if err != nil {
					log.Printf("Failed to create consumer: %v", err)
				}

				log.SetOutput(os.Stdout)
				if c.Bool("verbose") {
					log.SetLevel(log.DebugLevel)
				} else {
					log.SetLevel(log.WarnLevel)
				}

				log.Debug(c.String("datadog"))

				stats.Namespace = "techarch.dd_consumer."
				stats.Tags = append(stats.Tags, "version:0.1.0")

				go listenforchecks()
				series := make(chan *Message, 2048)
				intake := make(chan *Message, 2048)
				defer close(series)
				defer close(intake)

				go func(messages <-chan *Message, s chan<- *Message, i chan<- *Message) {
					count := 0

					for msg := range messages {
						count++
						if count%100 == 0 {
							if len(messages) > 1000 {
								log.Warn("Datadog goroutine is not fast enough: ", len(messages))
							}
							stats.Gauge("queue_length", float64(len(messages)), []string{"stage:consumer"}, 1)
						}
						msg.ParseJSON()

						if msg.IsSeries() {
							// Post to Series API
							log.Debug("Send to Series API")
							jsonparser.ArrayEach(msg.Data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
								if v, err := jsonparser.GetUnsafeString(value, "metric"); nil == err {
									if !strings.HasPrefix(v, ".") {
										namespace := strings.SplitN(v, ".", 2)[0]
										log.Debug("namespace:" + namespace)
										stats.Incr("message.count", []string{"namespace:" + namespace}, 1)
									}
								}
							}, "series")
							if !c.Bool("dryrun") {
								s <- msg
							}
						} else {
							// Post to Intake API
							log.Debug("Send to Intake API")
							stats.Incr("message.count", nil, 1)
							if !c.Bool("dryrun") {
								i <- msg
							}
						}
					}
				}(consumer.GetMsgChan(), series, intake)

				for w := 1; w <= 8; w++ {
					log.Info("Starting Intake worker ", w)
					go DatadogIntakeSender(
						intake,
						c.String("apikey"),
						c.String("appkey"),
					)
				}

				for w := 1; w <= 12; w++ {
					log.Info("Starting Series worker ", w)
					go DatadogSeriesSender(
						series,
						c.String("apikey"),
						c.String("appkey"),
					)
				}

				consumer.StartConsuming()

				consumer.Wait()
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "datadog",
					Usage:  "The API address where the messages should be forwarded",
					Value:  "https://app.datadoghq.com",
					EnvVar: "DATADOG_HOST",
				},
				cli.StringFlag{
					Name:   "apikey",
					Usage:  "The API key for account access",
					EnvVar: "DATADOG_APIKEY",
				},
				cli.StringFlag{
					Name:   "appkey",
					Usage:  "The APP key for account access",
					EnvVar: "DATADOG_APPKEY",
				},
				cli.BoolFlag{
					Name:   "dryrun",
					Usage:  "Consume but do not submit to Datadog",
					EnvVar: "DATADOG_DRYRUN",
				},
			},
		})
}

func DatadogSeriesSender(c <-chan *Message, apikey, appkey string) {
	w := NewClient(apikey, appkey)
	count := 0

	for msg := range c {
		count++
		if count%100 == 0 {
			if len(c) > 100 {
				log.Warn("Datadog Series goroutine is not fast enough: ", len(c))
			}
			stats.Gauge("queue_length", float64(len(c)), []string{"stage:series"}, 1)
		}

		err := w.doJsonRequest("POST", "/v1/series/", msg.Data, nil)
		if err != nil {
			log.Error(err)
		}
		log.Debug("Sent")
	}
}

func DatadogIntakeSender(c <-chan *Message, apikey, appkey string) {
	w := NewClient(apikey, appkey)
	count := 0

	for msg := range c {
		count++
		if count%100 == 0 {
			if len(c) > 100 {
				log.Warn("Datadog Intake goroutine is not fast enough: ", len(c))
			}
			stats.Gauge("queue_length", float64(len(c)), []string{"stage:intake"}, 1)
		}
		err := w.doJsonRequest("POST", "/intake/", msg.Data, nil)
		if err != nil {
			log.Error(err)
			stats.Incr("intake.errors", nil, 1)
		} else {
			stats.Incr("intake.count", nil, 1)
		}
		log.Debug("Sent")
	}
}
