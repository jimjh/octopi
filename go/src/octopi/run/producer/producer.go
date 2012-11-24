// producer: command line tool for testing the producer library.
// Usage:
//    ./producer --topic TOPIC BROKER
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

func main() {

	log.SetVerbose(log.DEBUG)
	log.SetPrefix("producer: ")

	var broker = flag.String("broker", "localhost:12345", "host and port number of broker")
	var topic = flag.String("topic", "hello", "topic to send message under")
	flag.Parse()

	p, err := producer.New(*broker)
	if nil != err {
		log.Fatal(err.Error())
	}

	pipe(p, *topic)
	p.Close()

}

func pipe(p *producer.Producer, topic string) {

	reader := bufio.NewReader(os.Stdin)
	for {
		line, _ := reader.ReadBytes('\n')
		if err := p.Send(topic, line); nil != err {
			log.Error(err.Error())
			break
		}
	}

}
