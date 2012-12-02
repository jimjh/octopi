package main

import (
	"code.google.com/p/go.net/websocket"
	"io"
	"octopi/api/protocol"
	"octopi/util/log"
)

// producer handles incoming produce requests. Producers may send multiple
// produce requests on the same persistent connection. The function exits when
// an `io.EOF` is received on the connection.
func producer(conn *websocket.Conn) {

	defer conn.Close()

	for {

		var request protocol.ProduceRequest

		err := websocket.JSON.Receive(conn, &request)
		if err == io.EOF { // graceful shutdown
			break
		}

		if nil != err {
			log.Warn("Ignoring invalid message from %v.", conn.RemoteAddr())
			continue
		}

		log.Info("Received produce request from %v.", conn.RemoteAddr())

		ack := new(protocol.Ack)
		if err := broker.Publish(request.Topic, request.ID, &request.Message); nil != err {
			log.Error(err.Error())
			ack.Status = protocol.StatusFailure
		} else {
			ack.Status = protocol.StatusSuccess
		}
		// TODO: should redirect if this node is not the leader

		websocket.JSON.Send(conn, &ack)

	}

	log.Info("Closed producer connection from %v.", conn.RemoteAddr())

}
