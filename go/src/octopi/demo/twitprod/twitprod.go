package main

import (
	"flag"
	"octopi/demo/twitproducer"
	"octopi/util/log"
)

func main() {

	var broker = flag.String("register", "localhost:12345", "host and port number of broker")
	var user = flag.String("user", "octopx", "username")
	flag.Parse()

	tp, err := twitproducer.NewTwitProducer(*user, "octopioctopus", *broker, nil)

	if nil != err {
		log.Warn("Did not receive a correct twitproducer")
	}

	err = tp.RelayMessages()

	if nil != err {
		log.Warn("%v", err)
	}

}
