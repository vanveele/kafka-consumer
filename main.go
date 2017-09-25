package main

import (
	"os"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/satori/go.uuid"
	"github.com/urfave/cli"
)

var (
	app       = cli.NewApp()
	stats     *statsd.Client
	builddate string
)

var globalFlags = struct {
	Brokers  string
	Topic    string
	Offset   string
	Groupid  string
	Verbose  bool
	StdIn    bool
	Port     string
	Statsd   string
	Replay   bool
	TimeFrom time.Time
	TimeTo   time.Time
}{}

func main() {
	app.Name = "kafka-consumer"
	app.Usage = "kafka-consumer is a tool to tail a kafka stream for json based datadog or splunk messages"
	app.Version = builddate

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "port",
			Usage:  "Port where the service listens for health check requests",
			EnvVar: "PORT",
			Value:  "8080",
		},
		cli.StringFlag{
			Name:   "statsd",
			Usage:  "target address (addr:port) for metrics",
			EnvVar: "STATSD",
			Value:  "localhost:8125",
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
		cli.BoolFlag{
			Name:   "replay",
			Usage:  "Replay old messages",
			EnvVar: "REPLAY",
		},
		cli.StringFlag{
			Name:   "replay-oldest",
			Usage:  "A timestamp of format time.RFC3339Nano (2017-01-02T01:02:03.123Z) to replay from",
			EnvVar: "REPLAY_OLDEST",
		},
		cli.StringFlag{
			Name:   "replay-newest",
			Usage:  "A time.RFC3339Nano formatted string (2017-01-02T01:02:03.123Z) to replay to",
			EnvVar: "REPLAY_NEWEST",
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
		globalFlags.Statsd = c.String("statsd")
		globalFlags.Replay = c.Bool("replay")
		if globalFlags.Replay {
			globalFlags.TimeFrom, _ = time.Parse(time.RFC3339Nano, c.String("replay-oldest"))
			globalFlags.TimeTo, _ = time.Parse(time.RFC3339Nano, c.String("replay-newest"))
			stats, _ = statsd.New("127.0.0.1:28125")
		} else {
			stats, _ = statsd.New(globalFlags.Statsd)
		}
		log.SetOutput(os.Stdout)
		if c.Bool("verbose") {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.WarnLevel)
		}

		log.Debugf("Consumer starting with options %+v\n", globalFlags)
		return nil
	}

	app.Run(os.Args)
}
