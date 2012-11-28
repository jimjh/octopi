// Package broker is an executable that launches a single broker instance.
// Every process may have at most one broker.
//
// Usage:
//    $> bin/broker --conf=conf.json
//
// Configuration Options:
//    port:     port number of broker; it will listen for connections on this port
//    register: host:port of register/leader for this broker to register
package main

import (
	"code.google.com/p/go.net/websocket"
	"flag"
	"io"
	"net/http"
	"octopi/api/protocol"
	"octopi/impl/brokerimpl"
	"octopi/util/config"
	"octopi/util/log"
	"strconv"
)

// Global broker object - at most one broker per process.
var broker *brokerimpl.Broker

// producerHandler handles incoming produce requests. Producers may send
// multiple produce requests on the same persistent connection. The function
// exits when an `io.EOF` is received on the connection.
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
		if err := broker.Publish(request.Topic, request.ID, &request.Message); nil != err {
			log.Error(err.Error())
			continue
		}

	}

	log.Info("Closed producer connection from %v.", ws.RemoteAddr())

}

// consumerHandler handles incoming subscribe requests. Consumers may send
// multiple subscribe requests on the same persistent connection. However,
// consumers may only subscribe to the same topic once. The function exits when
// an `io.EOF` is received on the connection.
func consumerHandler(conn *websocket.Conn) {

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
		ack := protocol.Ack{Status: protocol.SUCCESS}
		websocket.JSON.Send(conn, &ack)

		go subscription.Serve()

	}

	log.Info("Closed consumer connection from %v.", conn.RemoteAddr())

	// delete all subscriptions
	for topic, subscription := range subscriptions {
		broker.Unsubscribe(topic, subscription)
	}

}

// followerHandler handles incoming follow requests.
func followerHandler(ws *websocket.Conn) {

	defer ws.Close()

	var request protocol.FollowRequest
	err := websocket.JSON.Receive(ws, &request)

	// shut down if broken connection or wrong message format
	if err != nil {
		return
	}

	log.Info("Received follow request from %v.", ws.RemoteAddr())

	//send followack
	err = websocket.JSON.Send(ws, protocol.FollowACK{})

	//shut down if broken connection
	if err == io.EOF {
		return
	}
	// conn := &protocol.Follower{ws}
	// TODO: broker.RegisterFollower(conn, request.Offsets)

	// deal with sync
	/*for {
		var ack protocol.SyncACK
		err := websocket.JSON.Receive(ws, &ack)
		if err == io.EOF {
			break
		}
		broker.SyncFollower(conn, ack)
	}

	broker.DeleteFollower(conn)*/

}

// main starts a broker instance.
// Configuration Options:
//  - port number
//  - host:port of register/leader
func main() {

	log.SetPrefix("broker: ")
	log.SetVerbose(log.DEBUG)

	defer func() {
		if r := recover(); nil != r {
			log.Error("%v", r)
		}
	}()

	// parse command line args
	var configFile = flag.String("conf", "conf.json", "configuration file")
	flag.Parse()

	// init configuration
	config, err := config.Init(*configFile)
	checkError(err)

	log.Info("Initializing broker with options from %s.", *configFile)
	log.Info("Options read were: %v", config.Options)

	port, err := strconv.Atoi(config.Get("port", "5050"))
	checkError(err)

	broker = brokerimpl.New(config)
	// TODO: go broker.HandleMessages()

	listenHttp(port)

}

// listenHttp starts a http server at the given port and listens for incoming
// websocket message.
func listenHttp(port int) {
	http.Handle("/"+protocol.PUBLISH, websocket.Handler(producerHandler))
	http.Handle("/"+protocol.FOLLOW, websocket.Handler(followerHandler))
	http.Handle("/"+protocol.SUBSCRIBE, websocket.Handler(consumerHandler))
	http.ListenAndServe(":"+strconv.Itoa(port), nil)
	log.Info("Listening on %d ...", port)
}

// checkError logs a fatal error message and exits if `err` is not nil.
func checkError(err error) {
	if nil != err {
		log.Fatal(err.Error())
	}
}
