package main

import (
	"flag"
	"octopi/impl/producer"
	"octopi/util/log"
)

func main() {
	var broker = flag.String("broker", "localhost:12345", "host and port number of broker")
	flag.Parse()

	tp, err := producer.NewTwitProducer("octopx", "octopioctopus", *broker, nil)

	if nil != err {
		log.Warn("Did not receive a correct twitproducer")
	}

	_, err = tp.RelayMessages(10)

	if nil != err {
		log.Warn("Did not send all 10 messages")
	}
}
