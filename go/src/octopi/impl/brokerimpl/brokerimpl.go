// Package brokerimpl contains implementation details of the broker.
package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"math/rand"
	"octopi/api/protocol"
	"octopi/util/config"
	"octopi/util/log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Brokers relay messages from producers to followers. One broker is a leader,
// and the others are followers.
type Broker struct {
	config        *Config
	myHostPort    protocol.Hostport
	followers     map[*Follower]bool         // set of followers
	subscriptions map[string]SubscriptionSet // map of topics to consumer connections
	logs          map[string]*Log            // map of topics to logs
	leader        *websocket.Conn            // connection to the leader
	regConn       *websocket.Conn            // connection to the register, used by leader
	port          int                        // port number of this broker
	lock          sync.Mutex                 // lock to manage broker access
	cond          *sync.Cond                 // conditional variable for message log
}

// SubscriptionSet implemented as a map from *Subscription to true.
type SubscriptionSet map[*Subscription]bool

// FollowerSet implemented as a map from *Follower to true.
type FollowerSet map[*Follower]bool

// Offsets implemented as a map from topics to file sizes.
type Offsets map[string]int64

// New takes the host:port of the registry/leader and creates a new broker.
// Options:
// - register: host:port of registry/leader
func New(options *config.Config) *Broker {

	host := options.Get("host", "localhost")
	port := options.Get("port", "5050")

	b := &Broker{
		config:        &Config{*options},
		myHostPort:    protocol.Hostport(host + ":" + port),
		followers:     make(FollowerSet),
		subscriptions: make(map[string]SubscriptionSet),
		logs:          make(map[string]*Log),
	}

	b.cond = sync.NewCond(&b.lock)
	b.initLogs()

	// THESE ARE NOT BRUTE FORCE SPIN LOOPS
	// THESE ARE SIMPLY RE-TRIES WITH WAITS INBETWEEN
	if FOLLOWER == b.config.Role() {
		for !b.register(b.config.Register()) {}
	} else{
		for b.becomeleader(b.config.Register())!=nil{}
	}

	return b

}

// LeaderClose closes the connection with leader
func (b *Broker) LeaderClose() {
	b.leader.Close()
}

// Leader change changes the leader connection
func (b *Broker) LeaderChange(ws *websocket.Conn) {
	b.leader = ws
}

// initLogs initializes the logs map.
func (b *Broker) initLogs() {

	pattern := filepath.Join(b.config.LogDir(), "*"+EXT)
	matches, err := filepath.Glob(pattern)
	if nil != err {
		log.Panic("Unable to read from log directory: %s", b.config.LogDir())
	}

	for _, name := range matches {

		topic := filepath.Base(name)
		topic = topic[0 : len(topic)-len(EXT)]

		file, err := OpenLog(b.config, topic, -1)

		if nil != err {
			log.Error("Ignoring bad log file: %s", name)
			continue
		}

		b.logs[topic] = file
		log.Info("Found log file for %s.", topic)

	}

}

func (b *Broker) becomeleader(hostport string) error {
	var err error
	regEndpoint := "ws://" + hostport + "/" + protocol.LEADER
	origin := b.Origin()

	for {
                // dial the register
                b.regConn, err = websocket.Dial(regEndpoint, "", origin)

                // failed to dial the register
                // backoff and try again
                if nil != err {
                        log.Warn("Error dialing %s: %s", regEndpoint, err.Error())
                        backoff()
                        continue
                }
                break
        }
	err = websocket.JSON.Send(b.regConn, b.myHostPort)
	return err
}

// register sends a follow request to the given leader.
// TODO: implement redirect.
func (b *Broker) register(hostport string) bool {

	var err error
	var leaderHostport protocol.Hostport
	origin := b.Origin()
	// establish register endpoint
	regEndpoint := "ws://" + hostport + "/" + protocol.REDIRECTOR

	for {
		// dial the register
		regConn, err := websocket.Dial(regEndpoint, "", origin)

		// failed to dial the register
		// backoff and try again
		if nil != err {
			log.Warn("Error dialing %s: %s", regEndpoint, err.Error())
			backoff()
			continue
		}

		var redirect protocol.Ack
		err = websocket.JSON.Receive(regConn, &redirect)

		if nil != err || redirect.Status != protocol.REDIRECT {
			log.Warn("Register is not ready yet")
			backoff()
			continue
		}

		leaderHostport = protocol.Hostport(redirect.HostPort)

		regConn.Close()
		break
	}

	endpoint := "ws://" + leaderHostport + "/" + protocol.FOLLOW
	b.leader, err = websocket.Dial(string(endpoint), "", origin)

	// failed to dial to leader! return false and wait re-try
	if nil != err {
		log.Warn("Error dialing leader %s: %s", endpoint, err.Error())
		return false
	}

	err = websocket.JSON.Send(b.leader, protocol.FollowRequest{b.tails(), b.myHostPort})

	// failed to send to leader! return false and wait re-try
	if nil != err {
		log.Warn("Error following %s: ", endpoint, err.Error())
		return false
	}

	// TODO: wait for ack
	log.Info("Registered with leader.")

	// success connection! start catching up!
	go b.catchUp()

	return true
}

// returns the hostport of the current broker
func (b *Broker) MyHostport() string {
	return string(b.myHostPort)
}

// tails returns the sizes of all log files, organized by their topics.
func (b *Broker) tails() Offsets {

	tails := make(Offsets, len(b.logs))

	for topic, file := range b.logs {
		stat, err := file.Stat()
		if nil != err {
			log.Warn("Unable to get stats from log file: %s.", file.Name())
			continue
		}
		tails[topic] = stat.Size()
	}

	return tails

}

// origin returns the host:port of this broker.
func (b *Broker) Origin() string {
	host, err := os.Hostname()
	if nil != err {
		log.Panic(err.Error())
	}
	return fmt.Sprintf("%s:%d", host, b.port)
}

// gets the log file from b.logs, or open it if it does not exist.
func (b *Broker) getOrOpenLog(topic string) (*Log, error) {

	file, exists := b.logs[topic]
	if exists {
		return file, nil
	}

	file, err := OpenLog(b.config, topic, -1)
	if nil != err {
		return nil, err
	}

	b.logs[topic] = file
	return file, nil

}

func checkError(err error) {
	if err != nil {
		os.Exit(1)
	}
}

func backoff(){
        duration := time.Duration(rand.Intn(protocol.MAX_RETRY_INTERVAL))
        log.Debug("Backing off %d milliseconds.", duration)
        time.Sleep(duration * time.Millisecond)
}
