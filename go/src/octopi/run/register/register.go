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
)

var register *regimpl.Register
var singleton chan int

// leaderHandler handles brokers that are trying to initiate
// leader connections with the register. Blocks connection
// if there is already a leader
func leaderHandler(ws *websocket.Conn) {

	defer ws.Close()
	// need to ensure that only one connection access at a time
	select {
		case <-singleton:
			leaderChange(ws)
			singleton<-1
		default:
			//return directly if unavailable
	}
}

func leaderChange(ws *websocket.Conn) {

	var leaderhp protocol.HostPort
	err := websocket.JSON.Receive(ws, &leaderhp)

	log.Info("Received leader request from %v", leaderhp)

	// close the connection if there is already a leader
	if !register.NoLeader() {
		return
	}

	if err == io.EOF {
		register.SetLeader(regimpl.EMPTY)
		log.Info("Leader %v has disconnected", leaderhp)
		// XXX: is there a more elegant way? Seems a bit hacky
		go register.CheckNewLeader()
		return
	}

	log.Info("Made %v leader", leaderhp)

	register.SetLeader(string(leaderhp))

	for {
		var change protocol.InsyncChange
		err := websocket.JSON.Receive(ws, &change)

		// leader has disconnected!
		if err == io.EOF {
			log.Info("Leader %v has disconnected", leaderhp)
			register.SetLeader(regimpl.EMPTY)
			// XXX: seems a bit hacky...
			go register.CheckNewLeader()
			return
		}

		if change.Type == protocol.ADD {
			log.Info("Leader added an in-sync follower: %v", change.HostPort)
			// add a new follower
			register.AddFollower(string(change.HostPort))
		} else if change.Type == protocol.REMOVE {
			log.Info("Leader removed an in-sync follower: %v", change.HostPort)
			// remove a follower
			register.RemoveFollower(string(change.HostPort))
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
		redirect.Status = protocol.StatusNotReady
		log.Info("We have no established leader now!")
	} else {
		redirect.Status = protocol.StatusRedirect
		redirect.Payload = []byte(register.Leader())
	}

	log.Info("Redirect sending payload: %v", register.Leader())
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

	log.SetVerbose(log.DEBUG)
	log.SetPrefix("register: ")

	// parse command line args
	var configFile = flag.String("conf", "conf.json", "configuration file")
	flag.Parse()

	// init configuration
	config, err := config.Init(*configFile)
	checkError(err)

	log.Info("Initializing register with options from %s.", *configFile)
	log.Info("Options read were: %v", config)

	port, err := strconv.Atoi(config.Get("port", "12345"))
	checkError(err)

	register = regimpl.NewRegister()

	singleton = make(chan int, 1)
	singleton <- 1

	listenHttp(port)
}

func listenHttp(port int) {
	http.Handle("/"+protocol.LEADER, websocket.Handler(leaderHandler))
	http.Handle("/"+protocol.FOLLOW, websocket.Handler(redirectHandler))
	http.Handle("/"+protocol.PUBLISH, websocket.Handler(redirectHandler))
	http.Handle("/"+protocol.CONSUMER, websocket.Handler(consumerHandler))
	http.ListenAndServe(":"+strconv.Itoa(port), nil)
}

// checkError logs a fatal error message and exits if `err` is not nil.
func checkError(err error) {
	if nil != err {
		log.Fatal(err.Error())
	}
}
