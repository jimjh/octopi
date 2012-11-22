package main

import (
	"code.google.com/p/go.net/websocket"
	"net/http"
	"octopi/api/protocol"
	"octopi/impl/brokerimpl"
	"octopi/util/log"
)

// Global broker object - one broker per process
var broker *brokerimpl.Broker

// registerBroker
func registerBroker(regUrl string, regOrigin string, myHostPort string) *brokerimpl.Broker {

	/* connect to register server */
	regconn, err := websocket.Dial(regUrl, "", regOrigin)
	/* fatal error if connection or messages failed */
	checkError(err)

	bri := protocol.BrokerRegInit{protocol.BROKER, myHostPort}
	/* send relevant broker information to register */
	err = websocket.JSON.Send(regconn, bri)
	checkError(err)

	/* receive register assignments */
	var rbi protocol.RegBrokerInit
	err = websocket.JSON.Receive(regconn, &rbi)
	checkError(err)

	/* create the broker based on JSON from register server */
	b, err := brokerimpl.NewBroker(rbi, regconn)
	checkError(err)

	/* close the connection if broker not assigned as leader */
	if b.Role() != protocol.LEADER {
		regconn.Close()
	}

	return b
}

// producerHandler handles incoming produce requests.
func producerHandler(ws *websocket.Conn) {

	var request protocol.PublishRequest
	defer ws.Close()

	err := websocket.JSON.Receive(ws, &request)
	if nil != err || request.Source != protocol.PRODUCER {
		log.Warn("Ignoring invalid message from %v.", ws.LocalAddr())
		return
	}

	log.Info("Received publish request from %v.", ws.LocalAddr())
	if err = broker.RegisterProducer(ws, &request); nil != err {
		log.Error(err.Error())
	}

}

// consumerHandler handles incoming consume requests.
func consumerHandler(ws *websocket.Conn) {

	var request protocol.SubscribeRequest
	defer ws.Close()

	err := websocket.JSON.Receive(ws, &request)
	if nil != err || request.Source != protocol.CONSUMER {
		log.Warn("Ignoring invalid message from %v.", ws.LocalAddr())
		return
	}

	// TODO: catchup
	log.Info("Received subscribe request from %v.", ws.LocalAddr())
	if err = broker.RegisterConsumer(ws, &request); nil != err {
		log.Error(err.Error())
	}

}

// brokerHandler handles incoming broker requests.
func brokerHandler(ws *websocket.Conn) {

	var fli protocol.FollowLeadInit

	err := websocket.JSON.Receive(ws, &fli)

	// close if message is corrupted or invalid
	if nil != err || fli.Source != protocol.BROKER {
		log.Warn("Ignoring invalid message from %v.", ws.LocalAddr())
		ws.Close()
		return
	}

	block := make(chan interface{})
	conn := &protocol.FollowWSConn{ws, block}
	broker.CacheFollower(fli.HostPort, conn)
	
	/* blocks until disconnection detected */
	<-block
}

// main starts a broker instance.
func main() {

	log.SetPrefix("broker: ")
	log.SetVerbose(log.INFO)

	if false {
		// TODO: add command line arguments for register
		broker = registerBroker("", "", "")
	} else { // standalone mode
		var err error
		config := protocol.RegBrokerInit{Role: protocol.LEADER}
		broker, err = brokerimpl.NewBroker(config, nil)
		checkError(err)
	}
	// TODO: single-partition mode

	if broker.Role() == protocol.LEADER {
		http.Handle("/publish", websocket.Handler(producerHandler))
		http.Handle("/broker", websocket.Handler(brokerHandler))
	}

	http.Handle("/subscribe", websocket.Handler(consumerHandler))
	log.Info("Listening on 12345 ...")

	http.ListenAndServe(":12345", nil)
	// XXX: port number should be configurable

}

// checkError logs a fatal error message and exits if `err` is not nil.
func checkError(err error) {
	if nil != err {
		log.Fatal(err.Error())
	}
}
