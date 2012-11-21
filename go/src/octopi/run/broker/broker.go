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

	var pli protocol.ProdLeadInit

	err := websocket.JSON.Receive(ws, &pli)
	if nil != err || pli.MessageSrc != protocol.PRODUCER {
		log.Warn("Ignoring invalid message from %v.", ws.RemoteAddr())
		ws.Close()
		return
	}

	broker.RegProd(ws, pli)
	//TODO: send catchup if not
	for {
		var pubMsg protocol.PubMsg
		err := websocket.JSON.Receive(ws, &pubMsg)
		if nil != err {
			broker.RemoveProd(pli)
			ws.Close()
			return
		}
		broker.FollowBroadcast(pubMsg)
		//TODO: send message to consumers
	}

}

// consumerHandler handles incoming consume requests.
func consumerHandler(ws *websocket.Conn) {

	var request protocol.SubscribeRequest
	defer ws.Close()

	err := websocket.JSON.Receive(ws, &request)
	if nil != err || request.MessageSrc != protocol.CONSUMER {
		log.Warn("Ignoring invalid message from %v.", ws.RemoteAddr())
		return
	}

	// TODO: catchup
	if err != broker.RegisterConsumer(ws, &request) { // this should block
		log.Error(err.Error())
	}

}

// brokerHandler handles incoming broker requests.
func brokerHandler(ws *websocket.Conn) {

	var fli protocol.FollowLeadInit

	err := websocket.JSON.Receive(ws, &fli)

	// close if message is corrupted or invalid
	if nil != err || fli.MessageSrc != protocol.BROKER {
		log.Warn("Ignoring invalid message from %v.", ws.RemoteAddr())
		ws.Close()
		return
	}

	// TODO: do something on success

}

// main starts a broker instance.
func main() {

	// TODO: add command line arguments for register
	log.SetPrefix("broker: ")
	broker := registerBroker("", "", "")

	if broker.Role() == protocol.LEADER {
		http.Handle("/producer", websocket.Handler(producerHandler))
		http.Handle("/broker", websocket.Handler(brokerHandler))
	}

	http.Handle("/consumer", websocket.Handler(consumerHandler))
	http.ListenAndServe(":12345", nil)
	// XXX: port number should be configurable

	log.Info("Started on 12345.")

}

// checkError logs a fatal error message and exits if `err` is not nil.
func checkError(err error) {
	if nil != err {
		log.Fatal(err.Error())
	}
}
