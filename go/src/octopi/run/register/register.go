package main

import (
	"code.google.com/p/go.net/websocket"
	"flag"
	"io"
	"net/http"
	"octopi/api/protocol"
	"octopi/impl/regimpl"
	"octopi/util/config"
	"octopi/util/log"
	"strconv"
	"sync"
)

const (
	LEADER   = "leader"
	REDIRECT = "redirect"
	CONSUMER = "consumer"
)

var register *regimpl.Register
var handlerLock sync.Mutex

// leaderHandler handles brokers that are trying to initiate
// leader connections with the register. Blocks connection
// if there is already a leader
func leaderHandler(ws *websocket.Conn) {

	// need to ensure that only one connection access at a time
	handlerLock.Lock()
	defer handlerLock.Unlock()

	defer ws.Close()

	// close the connection if there is already a leader
	if !register.NoLeader() {
		return
	}

	var leaderhp protocol.Hostport
	err := websocket.JSON.Receive(ws, &leaderhp)

	if err == io.EOF {
		register.SetLeader(regimpl.EMPTY)
		register.LeaderDisconnect()
		// XXX: is there a more elegant way? Seems a bit hacky
		go register.CheckNewLeader()
		return
	}

	register.SetLeader(leaderhp)

	for {
		var change protocol.InsyncChange
		err := websocket.JSON.Receive(ws, &change)

		// leader has disconnected!
		if err == io.EOF {
			register.SetLeader(regimpl.EMPTY)
			register.LeaderDisconnect()
			// XXX: seems a bit hacky...
			go register.CheckNewLeader()
			return
		}

		if change.Type == protocol.ADD {
			// add a new follower
			register.AddFollower(change.Hostport)
		} else if change.Type == protocol.REMOVE {
			// remove a follower
			register.RemoveFollower(change.Hostport)
		} else {
			// ignore invalid message
			log.Warn("Ignoring invalid message from %v", ws.RemoteAddr())
			continue
		}
	}
}

// redirectHandler handles connections from new followers 
// joining the system or from producers wanting to publish
// a topic. ACK the new follower/producer with a redirect
// if a leader is determined. if not, disconnects.
func redirectHandler(ws *websocket.Conn) {

	defer ws.Close()

	var redirect protocol.Ack

	if register.NoLeader() {
		redirect.Status = protocol.NOTREADY
	} else {
		redirect.Status = protocol.REDIRECT
		redirect.HostPort = string(register.Leader())
	}

	// don't need to check if disconnect
	websocket.JSON.Send(ws, redirect)
}

// consumerHandler handles connections from new consumers
// joining the system. Sends the consumer a list of
// in-sync followers
func consumerHandler(ws *websocket.Conn) {

	defer ws.Close()
	// don't need to check if disconnect
	websocket.JSON.Send(ws, register.GetInsyncSet())
}

func main() {

	log.SetPrefix("register: ")

	// parse command line args
	var configFile = flag.String("conf", "conf.json", "configuration file")
	flag.Parse()

	// init configuration
	config, err := config.Init(*configFile)
	checkError(err)

	log.Info("Initializing register with options from %s.", *configFile)
	log.Info("Options read were: %v", config.Options)

	port, err := strconv.Atoi(config.Get("port", "12345"))
	checkError(err)

	register = regimpl.NewRegister()

	listenHttp(port)
}

func listenHttp(port int) {
	http.Handle("/"+LEADER, websocket.Handler(leaderHandler))
	http.Handle("/"+REDIRECT, websocket.Handler(redirectHandler))
	http.Handle("/"+CONSUMER, websocket.Handler(consumerHandler))

	http.ListenAndServe(":"+strconv.Itoa(port), nil)
}

// checkError logs a fatal error message and exits if `err` is not nil.
func checkError(err error) {
	if nil != err {
		log.Fatal(err.Error())
	}
}
