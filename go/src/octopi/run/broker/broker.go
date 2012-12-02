// Package main is an executable that launches a single broker instance.
// Each process may have at most one broker.
//
// Usage:
//    $> bin/broker --conf=conf.json
//
// Configuration Options:
//    port:     port number of broker; it will listen for connections on this port
//    register: host:port of register/leader for this broker to register
//    log_dir:  path to log directory
//    role:     launch as leader/follower
package main

import (
	"code.google.com/p/go.net/websocket"
	"flag"
	"net/http"
	"octopi/api/protocol"
	"octopi/impl/brokerimpl"
	"octopi/util/config"
	"octopi/util/log"
	"strconv"
)

// Global broker object - at most one broker per process.
var broker *brokerimpl.Broker

// Default port to use.
const PORT = "5050"

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

	log.Info("Initializing broker with options from %s.", *configFile)

	// init configuration
	config, err := config.Init(*configFile)
	checkError(err)

	log.Info("Options read were: %v", config)

	port, err := strconv.Atoi(config.Get("port", PORT))
	checkError(err)

	broker, err = brokerimpl.New(config)
	checkError(err)

	listenHttp(port)

}

// listenHttp starts a http server at the given port and listens for incoming
// websocket messages.
func listenHttp(port int) {
	http.Handle("/"+protocol.PUBLISH, websocket.Handler(producer))
	http.Handle("/"+protocol.FOLLOW, websocket.Handler(follower))
	http.Handle("/"+protocol.SUBSCRIBE, websocket.Handler(consumer))
	http.Handle("/"+protocol.SWAP, websocket.Handler(register))
	log.Info("HTTP server started on %d.", port)
	http.ListenAndServe(":"+strconv.Itoa(port), nil)
}

// checkError logs a fatal error message and exits if `err` is not nil.
func checkError(err error) {
	if nil != err {
		log.Fatal(err.Error())
	}
}
