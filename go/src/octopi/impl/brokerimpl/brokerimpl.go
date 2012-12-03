// Package brokerimpl contains implementation details of the broker.
package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"octopi/api/protocol"
	"octopi/util/config"
	"octopi/util/log"
	"path/filepath"
	"sync"
	"time"
)

// Brokers relay messages from producers to followers. One broker is a leader,
// and the others are followers.
type Broker struct {
	config        *Config
	role          int                        // leader or follower
	followers     map[*Follower]bool         // set of followers
	subscriptions map[string]SubscriptionSet // map of topics to consumer connections
	logs          map[string]*Log            // map of topics to logs
	leader        *protocol.Socket           // connection to the leader
	checkpoints   map[string]int64           // checkpoints for each topic log
	regConn       *websocket.Conn            // connection to the register, used by leader
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
func New(options *config.Config) (*Broker, error) {

	config := &Config{*options}
	b := &Broker{
		role:          config.Role(),
		config:        config,
		checkpoints:   make(map[string]int64),
		followers:     make(FollowerSet),
		subscriptions: make(map[string]SubscriptionSet),
		logs:          make(map[string]*Log),
	}

	b.cond = sync.NewCond(&b.lock)
	b.initLogs()
	b.initSocket()

	switch b.role {
	case FOLLOWER:
		return b, b.register()
	case LEADER:
		return b, b.BecomeLeader()
	}

	return nil, errors.New("Invalid configuration options.")

}

// initSocket initializes the socket to use to connect to the leader.
func (b *Broker) initSocket() {
	b.leader = &protocol.Socket{
		HostPort: b.config.Register(),
		Path:     protocol.FOLLOW,
		Origin:   b.Origin(),
	}
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

// ChangeLeader closes the current leader connection and re-registers.
func (b *Broker) ChangeLeader() error {

	b.lock.Lock()
	defer b.lock.Unlock()

	b.leader.Reset(b.config.Register())
	return b.register()

}

// BecomeLeader returns only after successfully declaring leadership with the
// register. It locks down the broker, declares leadership with the register,
// and checkpoints the tails of all open logs.
func (b *Broker) BecomeLeader() error {

	endpoint := "ws://" + b.config.Register() + "/" + protocol.LEADER
	origin := b.Origin()

	b.lock.Lock()
	defer b.lock.Unlock()

	log.Debug("Resetting...")
	b.leader.Reset(origin)

	for {

		var err error
		b.regConn, err = websocket.Dial(endpoint, "", origin)

		if nil != err {
			log.Warn("Error dialing %s: %s", endpoint, err.Error())
			backoff()
			continue
		}

		err = websocket.JSON.Send(b.regConn, origin)
		if nil != err {
			backoff()
			continue
		}

		break

	}

	b.checkpoints = b.tails()
	b.role = LEADER
	return nil

}

// register sends a follow request to the given leader.
func (b *Broker) register() error {

	follow := &protocol.FollowRequest{b.tails(), protocol.HostPort(b.Origin())}
	payload, err := b.leader.Send(follow, math.MaxInt32)
	if nil != err {
		return err
	}

	log.Info("Registered with leader.")

	var ack protocol.FollowACK
	if err := json.Unmarshal(payload, &ack); nil != err {
		return err
	}

	for topic, checkpoint := range ack.Truncate {
		if err := truncateLog(b.config, topic, checkpoint); nil != err {
			return err
		}
	}

	// successful connection
	go b.catchUp()

	log.Info("Catching up with leader.")
	return nil

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
	return fmt.Sprintf("%s:%d", b.config.Host(), b.config.Port())
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

func backoff() {
	duration := time.Duration(rand.Intn(protocol.MAX_RETRY_INTERVAL))
	log.Debug("Backing off %d milliseconds.", duration)
	time.Sleep(duration * time.Millisecond)
}
