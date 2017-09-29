package main

import (
	"crypto/tls"
	"io"
	"net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/fuyufjh/splunk-hec-go"
	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands,
		cli.Command{
			Name:  "splunk",
			Usage: "forward messages to splunk",
			Action: func(c *cli.Context) {
				var (
					consumer Consumer
					err      error
				)

				if globalFlags.StdIn {
					consumer, err = CreateConsumer(map[string]string{
						"consumer-type": "stdin",
					})
					if err != nil {
						log.Printf("Failed to create consumer: %v", err)
					}
				} else {
					consumer, err = CreateConsumer(map[string]string{
						"consumer-type": "kafka",
						"brokers":       globalFlags.Brokers,
						"topics":        globalFlags.Topic,
						"groupid":       globalFlags.Groupid,
					})
					if err != nil {
						log.Printf("Failed to create consumer: %v", err)
					}

					go listenforchecks()
				}

				stats.Namespace = "techarch.splunk_consumer."
				stats.Tags = append(stats.Tags, "version:0.1.0")

				spk := make(chan *hec.Event, 256)
				defer close(spk)

				go func(messages <-chan *Message, spk chan<- *hec.Event) {
					count := 0
					for msg := range messages {
						var (
							timeRep time.Time
						)
						count++
						var index, srcType, appID string
						var timeStr, hostname string

						if count%1000 == 0 {
							if len(messages) > 200 {
								log.Warn("Splunk consume goroutine is not fast enough: ", len(messages))
							}
							stats.Gauge("queue_length", float64(len(messages)), []string{"stage:consumer"}, 1)
						}
						msg.ParseJSON()

						sdID := "splunk::v1@" + c.String("eid")
						_, err = msg.GetString("$!", "rfc5424-sd")
						if err != nil {
							// Rsyslog has not passed mmparsestruc
							// Likely due to rsyslog7
							sd, _ := msg.GetString("structured-data")

							i, err := parseStructuredData(sd)
							if err != nil {
								log.Warn("No matching STRUCTURED-DATA")
								continue
							}

							index, _ = i[sdID].Params["index"]
							srcType, _ = i[sdID].Params["sourcetype"]
							appID, _ = i[sdID].Params["appID"]
						} else {
							index, _ = msg.GetString("$!", "rfc5424-sd", sdID, "index")
							srcType, _ = msg.GetString("$!", "rfc5424-sd", sdID, "sourcetype")
							appID, _ = msg.GetString("$!", "rfc5424-sd", sdID, "appid")
						}
						timeStr, _ = msg.GetString("timereported")
						hostname, _ = msg.GetString("hostname")

						event := hec.NewEvent(string(msg.Data))
						addTags := []string{}

						//timeEpoch := time.Parse("2006-01-02T15:04:05.999Z")
						timeRep, err = time.Parse(time.RFC3339Nano, timeStr)

						if err != nil {
							log.Warn("unknown format for inline timestamp: " + timeStr)
						} else {
							event.SetTime(timeRep)
						}

						if len(index) != 0 {
							event.SetIndex(index)
							addTags = append(addTags, "index:"+string(index))
						}
						if len(srcType) != 0 {
							event.SetSourceType(srcType)
							addTags = append(addTags, "sourcetype:"+string(srcType))
						}
						if len(hostname) != 0 {
							event.SetHost(hostname)
							addTags = append(addTags, "srchost:"+string(hostname))
						}
						if len(appID) != 0 {
							addTags = append(addTags, "appid:"+string(appID))
						}

						stats.Incr("message.count", addTags, 1)
						stats.Gauge("delay", time.Since(timeRep).Seconds(), addTags, 1)
						if !c.Bool("dryrun") {
							spk <- event
						}
					}
				}(consumer.GetMsgChan(), spk)

				for w := 1; w <= c.Int("workers"); w++ {

					go SplunkSender(
						spk,
						strings.Split(c.String("splunk"), ","),
						c.String("token"),
						c.Int("max-retries"),
					)
				}
				err = consumer.StartConsuming()
				if err != nil {
					log.Printf("Error starting consumer: %v", err)
				}

				consumer.Wait()
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "splunk",
					Value:  "splunk",
					Usage:  "Comma delimited HEC addresses where the messages should be forwarded",
					EnvVar: "SPLUNK_ADDRESS",
				},
				cli.StringFlag{
					Name:   "token",
					Usage:  "The HEC token for input",
					EnvVar: "SPLUNK_TOKEN",
				},
				cli.IntFlag{
					Name:   "max-retries",
					Value:  5,
					Usage:  "The max number of retries to send to Splunk HEC",
					EnvVar: "SPLUNK_MAX_RETRIES",
				},
				cli.IntFlag{
					Name:   "workers",
					Value:  8,
					Usage:  "The number of parallel workers sending to Splunk HEC",
					EnvVar: "SPLUNK_WORKERS",
				},
				cli.StringFlag{
					Name:   "eid",
					Value:  "7594",
					Usage:  "Syslog SD custom Eid number",
					EnvVar: "SPLUNK_EID",
				},
				cli.BoolFlag{
					Name:   "dryrun",
					Usage:  "Consume but do not submit to Splunk",
					EnvVar: "SPLUNK_DRYRUN",
				},
			},
		})
}

// SplunkSender TODO: docs
func SplunkSender(c <-chan *hec.Event, address []string, token string, retry int) {
	w := hec.NewCluster(
		address,
		token)
	w.SetHTTPClient(&http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}})
	w.SetKeepAlive(true)
	w.SetMaxRetry(retry)

	count := 0

	for msg := range c {
		count++
		if count%100 == 0 {
			if len(c) > 100 {
				log.Warn("Splunk Sender goroutine is not fast enough: ", len(c))
			}
			stats.Gauge("queue_length", float64(len(c)), []string{"stage:sender"}, 1)
		}

		err := w.WriteEvent(msg)
		if err != nil {
			log.Printf("Error from SplunkSender: %s", err)
			stats.Incr("sender.errors", nil, 1)
		} else {
			stats.Incr("sender.count", nil, 1)
		}
		log.Debug("Sent")
	}
}

func SplunkSenderRaw(reader io.ReadSeeker, metadata *hec.EventMetadata, address []string, token string, retry int) {
	w := hec.NewCluster(address, token)
	w.SetHTTPClient(&http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}})
	w.SetKeepAlive(true)
	w.SetMaxRetry(retry)

	err := w.WriteRaw(reader, metadata)
	if err != nil {
		log.Printf("Error from SplunkSenderRaw: %s", err)
		stats.Incr("senderraw.errors", nil, 1)
	} else {
		stats.Incr("senderraw.count", nil, 1)
	}
	log.Debug("Sent")
}
