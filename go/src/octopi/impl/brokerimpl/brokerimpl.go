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
	followers     map[*Follower]bool         // set of followers
	subscriptions map[string]SubscriptionSet // map of topics to consumer connections
	logs          map[string]*Log            // map of topics to logs
	leader        *websocket.Conn            // connection to the leader
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

	b := &Broker{
		config:        &Config{*options},
		followers:     make(FollowerSet),
		subscriptions: make(map[string]SubscriptionSet),
		logs:          make(map[string]*Log),
	}

	b.cond = sync.NewCond(&b.lock)
	b.initLogs()

	if FOLLOWER == b.config.Role() {
		b.register(b.config.Register())
	}

	return b

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

	}

}

// register sends a follow request to the given leader.
// TODO: implement redirect.
func (b *Broker) register(hostport string) {

	endpoint := "ws://" + hostport + "/" + protocol.FOLLOW
	origin := b.origin()

	backoff := func() {
		duration := time.Duration(rand.Intn(protocol.MAX_RETRY_INTERVAL))
		log.Debug("Backing off %d milliseconds.", duration)
		time.Sleep(duration * time.Millisecond)
	}

	for {

		var err error
		b.leader, err = websocket.Dial(endpoint, "", origin)
		if nil != err {
			log.Warn("Error dailing %s: %s", endpoint, err.Error())
			backoff()
			continue
		}

		err = websocket.JSON.Send(b.leader, protocol.FollowRequest{b.tails()})
		if nil != err {
			log.Warn("Error following %s: ", endpoint, err.Error())
			backoff()
			continue
		}

		// TODO: wait for ack
		log.Info("Registered with leader.")

		b.catchUp()
		break

	}

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
func (b *Broker) origin() string {
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
