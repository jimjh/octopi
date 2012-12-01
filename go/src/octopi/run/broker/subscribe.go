package main

import (
	"code.google.com/p/go.net/websocket"
	"io"
	"octopi/api/protocol"
	"octopi/impl/brokerimpl"
	"octopi/util/log"
)

// consumer handles incoming subscribe requests. Consumers may send
// multiple subscribe requests on the same persistent connection. However,
// consumers may only subscribe to the same topic once. The function exits when
// an `io.EOF` is received on the connection.
func consumer(conn *websocket.Conn) {

	defer conn.Close()
	subscriptions := make(map[string]*brokerimpl.Subscription)

	for {

		var request protocol.SubscribeRequest

		err := websocket.JSON.Receive(conn, &request)
		if err == io.EOF { // graceful shutdown
			break
		}

		if nil != err {
			log.Warn("Ignoring invalid message from %v.",
				conn.RemoteAddr())
			continue
		}

		if _, exists := subscriptions[request.Topic]; exists {
			log.Warn("Ignoring duplicate subscribe request from %v.",
				conn.RemoteAddr())
			continue
		}

		log.Info("Received subscribe request from %v with offset %d.",
			conn.RemoteAddr(), request.Offset)

		subscription, err := broker.Subscribe(conn, request.Topic, request.Offset)
		if nil != err {
			log.Error(err.Error())
			continue
		}

		subscriptions[request.Topic] = subscription
		ack := protocol.Ack{Status: protocol.StatusSuccess}
		websocket.JSON.Send(conn, &ack)

		go subscription.Serve()

	}

	log.Info("Closed consumer connection from %v.", conn.RemoteAddr())

	// delete all subscriptions
	for topic, subscription := range subscriptions {
		broker.Unsubscribe(topic, subscription)
	}

}
