package main

import (
	"log"
	"os"

	"github.com/satori/go.uuid"
	"github.com/urfave/cli"
)

var (
	logger    = log.New(os.Stderr, "", log.LstdFlags)
	app       = cli.NewApp()
	builddate string
)

var globalFlags = struct {
	Brokers string
	Topic   string
	Offset  string
	Groupid string
	Verbose bool
	StdIn   bool
	Port    string
}{}

func main() {
	app.Name = "kafka-consumer"
	app.Usage = "kafka-consumer is a tool to tail a kafka stream for json based datadog messages"
	app.Version = builddate

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "port",
			Usage:  "Port where the service listens for health check requests",
			EnvVar: "PORT",
			Value:  "8080",
		},
		cli.BoolFlag{
			Name:   "stdin",
			Usage:  "Read messages from stdin rather than Kafka",
			EnvVar: "STDIN",
		},
		cli.StringFlag{
			Name:   "srcformat",
			Usage:  "Messages expected in JSON or RAW format",
			EnvVar: "SRCFORMAT",
		},
		cli.StringFlag{
			Name:   "brokers",
			Usage:  "The comma seperated list of brokers in the Kafka cluster",
			EnvVar: "KAFKA_BROKERS",
		},
		cli.StringFlag{
			Name:   "topic",
			Usage:  "Kafka topic to subscribe",
			EnvVar: "KAFKA_TOPIC",
		},
		cli.StringFlag{
			Name:   "groupid",
			Usage:  "Consumer group identifier",
			EnvVar: "KAFKA_GROUPID",
			Value:  uuid.NewV4().String(),
		},
		cli.BoolFlag{
			Name:   "verbose",
			Usage:  "Use verbose logging",
			EnvVar: "VERBOSE",
		},
	}

	app.Before = func(c *cli.Context) error {
		globalFlags.Brokers = c.String("brokers")
		globalFlags.Topic = c.String("topic")
		globalFlags.Offset = c.String("offset")
		globalFlags.Groupid = c.String("groupid")
		globalFlags.Port = c.String("port")
		globalFlags.Verbose = c.Bool("verbose")
		globalFlags.StdIn = c.Bool("stdin")

		logger.Printf("Kafka-consumer starting with options %+v\n", globalFlags)

		return nil
	}

	app.Run(os.Args)
}
