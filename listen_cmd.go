package main

import (
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli"
)

func init() {
	app.Commands = append(app.Commands,
		cli.Command{
			Name:  "listen",
			Usage: "tails messages from kafka",
			Action: func(c *cli.Context) {

				if len(c.Args()) > 0 && c.Args()[0] != "" {
					globalFlags.Topic = c.Args()[0]
				}

				consumer, err := CreateConsumer(map[string]string{
					"consumer-type": "kafka",
					"brokers":       globalFlags.Brokers,
					"groupid":       globalFlags.Groupid,
					"topics":        globalFlags.Topic,
				})
				if err != nil {
					log.Printf("Failed to create consumer: %v", err)
				}

				go func(messages <-chan *Message) {
					count := 0
					for msg := range messages {
						count++
						if count%100 == 0 {
							if len(messages) > 100 {
								fmt.Fprintf(os.Stderr, "Display goroutine is not fast enough: %d\n", len(messages))
							}
							fmt.Fprintf(os.Stdout, "%d\r", count)
						}

						if c.Bool("all") {
							fmt.Printf("%s\n", msg.Data)
						} else {
							msg.ParseJSON()
							series, _ := msg.GetString("series")
							fmt.Printf("%s", series)
						}
					}
				}(consumer.GetMsgChan())

				consumer.StartConsuming()

				consumer.Wait()
			},
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "all",
					Usage: "Print entire JSON, not just series metrics",
				},
			},
		})
}
