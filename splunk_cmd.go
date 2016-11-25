package main

import (
	"crypto/tls"
	"log"
	"net/http"
	"strings"

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
				}

				log.Println(c.String("splunk"))

				go listenforchecks()
				spk := make(chan *hec.Event, 256)
				defer close(spk)
				go func(messages <-chan *Message, spk chan<- *hec.Event) {
					count := 0
					for msg := range messages {
						count++

						msg.ParseJSON()
						index, _ := msg.GetString("$!", "rfc5424-sd", "splunk::v1@"+c.String("eid"), "index")
						srcType, _ := msg.GetString("$!", "rfc5424-sd", "splunk::v1@"+c.String("eid"), "sourcetype")
						hostname, _ := msg.GetString("hostname")

						event := hec.NewEvent(string(msg.Data))

						if len(index) != 0 {
							event.SetIndex(index)
						}
						if len(srcType) != 0 {
							event.SetSourceType(srcType)
						}
						if len(hostname) != 0 {
							event.SetHost(hostname)
						}

						spk <- event
					}
				}(consumer.GetMsgChan(), spk)

				for w := 1; w <= 4; w++ {

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
				cli.StringFlag{
					Name:   "eid",
					Value:  "7594",
					Usage:  "Syslog SD custom Eid number",
					EnvVar: "SPLUNK_EID",
				},
			},
		})
}

func SplunkSender(c <-chan *hec.Event, address []string, token string, retry int) {
	w := hec.NewCluster(
		address,
		token)
	w.SetHTTPClient(&http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}})
	w.SetKeepAlive(true)
	w.SetMaxRetry(retry)

	for msg := range c {
		err := w.WriteEvent(msg)
		if err != nil {
			log.Printf("Error from SplunkSender: %s", err)
		}
	}
}
