package main

import (
	"flag"
	"strings"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/kafka"
)

var (
	brokers = flag.String("brokers", "127.0.0.1:9092", "a list of comma separated broker:port")
	topic   = flag.String("topic", "", "the topic name")
	group   = flag.String("group", "group", "the consumer group name")
	timeout = flag.Int("timeout", 30, "the number of seconds for timeout connections")
)

func main() {

	gio.Init()
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")

	k := kafka.New(brokerList, *group, *topic)
	k.TimeoutSeconds = *timeout

	f := flow.New().Read(k).Printlnf("%x")

	f.Run()

}
