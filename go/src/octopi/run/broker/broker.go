package main

import (
	"code.google.com/p/go.net/websocket"
	"io"
	"net/http"
	"octopi/api/protocol"
	"octopi/impl/brokerimpl"
	"octopi/util/log"
)

// Global broker object - one broker per process.
var broker *brokerimpl.Broker

// producerHandler handles incoming produce requests. Producers may send
// multiple produce requests on the same persistent connection.
func producerHandler(ws *websocket.Conn) {

	defer ws.Close()

	for {

		var request protocol.ProduceRequest

		err := websocket.JSON.Receive(ws, &request)
		if err == io.EOF { // graceful shutdown
			break
		}

		if nil != err {
			log.Warn("Ignoring invalid message from %v.", ws.RemoteAddr())
			continue
		}

		log.Info("Received produce request from %v.", ws.RemoteAddr())
		if err := broker.Publish(request.Topic, &request.Message); nil != err {
			log.Error(err.Error())
			continue
		}

	}

	log.Info("Closed producer connection from %v.", ws.RemoteAddr())

}

// consumerHandler handles incoming consume requests.
func consumerHandler(ws *websocket.Conn) {

	defer ws.Close()
	subscriptions := make(map[*brokerimpl.Subscription]string)

	for {

		var request protocol.SubscribeRequest

		err := websocket.JSON.Receive(ws, &request)
		if err == io.EOF { // graceful shutdown
			break
		}

		if nil != err {
			log.Warn("Ignoring invalid message from %v.", ws.RemoteAddr())
			continue
		}

		// TODO: catchup/rewind
		log.Info("Received subscribe request from %v.", ws.RemoteAddr())
		subscription := broker.Subscribe(ws, request.Topic)
		subscriptions[subscription] = request.Topic

	}

	log.Info("Closed consumer connection from %v.", ws.RemoteAddr())

	// delete all subscriptions
	for subscription, topic := range subscriptions {
		broker.Unsubscribe(topic, subscription)
	}

}

// brokerHandler handles incoming broker requests.
func brokerHandler(ws *websocket.Conn) {

	defer ws.Close()

	for {

		var request protocol.FollowRequest
		err := websocket.JSON.Receive(ws, &request)
		if err == io.EOF { // graceful shutdown
			break
		}

		conn := &protocol.Follower{ws, make(chan interface{})}
		broker.RegisterFollower(conn, request.HostPort, request.Offsets)

		// TODO: deal with acks

	}

	// TODO: cleanup after follower connection is lost

}

// main starts a broker instance.
func main() {

	log.SetPrefix("broker: ")
	log.SetVerbose(log.DEBUG)

	// TODO: port number should be configurable
	// TODO: add command line arguments for register

	broker = brokerimpl.New(12345, "localhost:12345")

	http.Handle("/"+protocol.PUBLISH, websocket.Handler(producerHandler))
	http.Handle("/"+protocol.FOLLOW, websocket.Handler(brokerHandler))
	http.Handle("/"+protocol.SUBSCRIBE, websocket.Handler(consumerHandler))
	http.ListenAndServe(":12345", nil)

	log.Info("Listening on 12345 ...")

}

// checkError logs a fatal error message and exits if `err` is not nil.
func checkError(err error) {
	if nil != err {
		log.Fatal(err.Error())
	}
}
