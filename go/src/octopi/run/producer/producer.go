// producer: command line tool for testing the producer library.
// Usage:
//    ./producer --topic TOPIC --broker BROKER
// topic:    topic to send messages under
// broker:   host and port number of broker

package main

import (
	"bufio"
	"flag"
	"octopi/impl/producer"
	"octopi/util/log"
	"os"
)

// main launches a producer instance
func main() {

	log.SetVerbose(log.DEBUG)
	log.SetPrefix("producer: ")

	var broker = flag.String("broker", "localhost:12345", "host and port number of broker")
	var topic = flag.String("topic", "hello", "topic to send message under")
	flag.Parse()

	p, err := producer.New(*broker, nil)
	if nil != err {
		log.Fatal(err.Error())
	}

	defer p.Close()
	pipe(p, *topic)

}

// pipe relays messages from the command line to the producer library.
func pipe(p *producer.Producer, topic string) {

	reader := bufio.NewReader(os.Stdin)
	for {
		line, _ := reader.ReadBytes('\n')
		if err := p.Send(topic, line); nil != err {
			log.Error("Gave up: %s", err.Error())
			break
		}
	}

}
