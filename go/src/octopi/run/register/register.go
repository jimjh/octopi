package main

import (
	"code.google.com/p/go.net/websocket"
	"flag"
	"net/http"
	"io"
	"octopi/util/config"
	"octopi/util/log"
	"octopi/impl/regimpl"
	"octopi/api/protocol"
	"strconv"
)

const (
	LEADER   = "leader"
	FOLLOWER = "follower"
	PRODUCER = "producer"
	CONSUMER = "consumer"
)

var register *regimpl.Register

// leaderHandler handles brokers that are trying to initiate
// leader connections with the register. Blocks connection
// if there is already a leader
func leaderHandler(ws *websocket.Conn) {
	// close the connection if there is already a leader
	if !register.NoLeader() {
		ws.Close()
		return
	}

	for{
		var change protocol.InsyncChange
		err := websocket.JSON.Receive(ws, &change)

		// leader has disconnected!
		if err==io.EOF{
			ws.Close()
			register.LeaderDisconnect()
			return
		}

		if change.Type == protocol.ADD{
			register.AddFollower(change.Hostport)
		} else if change.Type == protocol.REMOVE{
			register.RemoveFollower(change.Hostport)
		} else{
			// ignore invalid message
			log.Warn("Ignoring invalid message from %v", ws.RemoteAddr())
			continue
		}
	}
}

func followerHandler(ws *websocket.Conn) {

}

func consumerHandler(ws *websocket.Conn) {

}

func producerHandler(ws *websocket.Conn) {

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
	http.Handle("/"+FOLLOWER, websocket.Handler(followerHandler))
	http.Handle("/"+CONSUMER, websocket.Handler(consumerHandler))
	http.Handle("/"+PRODUCER, websocket.Handler(producerHandler))

	http.ListenAndServe(":"+strconv.Itoa(port), nil)
}

// checkError logs a fatal error message and exits if `err` is not nil.
func checkError(err error) {
	if nil != err {
		log.Fatal(err.Error())
	}
}
