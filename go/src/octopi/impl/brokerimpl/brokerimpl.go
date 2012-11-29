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
	followers     map[*protocol.Follower]bool // set of followers
	subscriptions map[string]SubscriptionSet  // map of topics to consumer connections
	logs          map[string]*Log             // map of topics to logs
	leader        *websocket.Conn             // connection to the leader
	port          int                         // port number of this broker
	lock          sync.Mutex                  // lock to manage broker access
	cond          *sync.Cond                  // conditional variable for message log
}

// SubscriptionSet implemented as a map from *Subscription to true.
type SubscriptionSet map[*Subscription]bool

// FollowerSet implemented as a map from *Follower to true.
type FollowerSet map[*protocol.Follower]bool

// New takes the host:port of the registry/leader and creates a new broker.
// Options:
// - register: host:port of registry/leader
func New(options *config.Config) *Broker {

	config := &Config{*options}
	b := &Broker{
		config:        config,
		followers:     make(FollowerSet),
		subscriptions: make(map[string]SubscriptionSet),
		logs:          make(map[string]*Log),
	}
	b.cond = sync.NewCond(&b.lock)

	if FOLLOWER == config.Role() {
		b.register(config.Register())
	}

	return b

}

// register sends a follow request to the given leader.
// TODO: implement redirect.
func (b *Broker) register(hostport string) {

	var leader *websocket.Conn

	endpoint := "ws://" + hostport + "/" + protocol.FOLLOW
	origin := b.origin()

	backoff := func() {
		duration := time.Duration(rand.Intn(protocol.MAX_RETRY_INTERVAL))
		log.Debug("Backing off %d milliseconds.", duration)
		time.Sleep(duration * time.Millisecond)
	}

	for {

		leader, err := websocket.Dial(endpoint, "", origin)
		if nil != err {
			log.Warn("Error dailling %s: %s", endpoint, err.Error())
			backoff()
			continue
		}

		err = websocket.JSON.Send(leader, protocol.FollowRequest{b.tails()})
		if nil != err {
			// TODO: wait for ack
			log.Warn("Error dailling %s: ", endpoint, err.Error())
			backoff()
			continue
		}

		log.Info("Registered with leader.")

	}

	b.lock.Lock()
	defer b.lock.Unlock()
	b.leader = leader

}

// tails returns the sizes of all log files, organized by their topics.
func (b *Broker) tails() map[string]int64 {

	pattern := filepath.Join(b.config.LogDir(), "*"+EXT)
	matches, err := filepath.Glob(pattern)
	if nil != err {
		return nil
	}

	ext := len(EXT)
	tails := make(map[string]int64, len(matches))

	for _, name := range matches {
		stat, err := os.Stat(name)
		if nil != err {
			continue
		}
		topic := filepath.Base(name)
		topic = topic[0 : len(topic)-ext]
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

// start handling messages once leader is ready
// func (b *Broker) HandleMessages() {
// 	// blocks until leader is ready
// 	for {
// 		// start synchronization process
// 		var sreq protocol.SyncRequest
// 		err := websocket.JSON.Receive(b.leader, &sreq)
// 		if err == io.EOF {
// 			// TODO: Leader failure
// 		}
//
// 		currtopic := sreq.Topic
//
// 		b.lock.Lock()
// 		tLog, exists := b.logs[currtopic]
//
// 		// create a new log if topic not seen before
// 		if !exists {
// 			// TODO: set correct path and handle failure
// 			b.logs[currtopic], _ = brokerlog.OpenLog(currtopic)
// 			tLog = b.logs[currtopic]
// 		}
// 		// XXX: what to do if log failure?
// 		switch sreq.Type {
// 		case protocol.UPDATE:
// 			tLog.WriteBytes(sreq.Message)
// 			// advance hwmark if not up-to-date yet
// 			if !b.upToDateTopics[currtopic] {
// 				tLog.Commit(tLog.Tail())
// 			}
// 			// construct update/write acknowledgement
// 			synAck := protocol.SyncACK{
// 				Topic:  currtopic,
// 				Offset: tLog.HighWaterMark(),
// 			}
// 			b.lock.Unlock()
// 			// send the update/write acknowledgement
// 			websocket.JSON.Send(b.leader, synAck)
// 		case protocol.COMMIT:
// 			b.upToDateTopics[currtopic] = true
// 			var msgid int64
// 			buf := bytes.NewBuffer(sreq.Message)
// 			binary.Read(buf, binary.LittleEndian, &msgid)
// 			tLog.Commit(msgid)
// 			b.lock.Unlock()
// 		}
// 	}
// }

/*func (b *Broker) RegisterFollower(conn *protocol.Follower, offsets map[string]int64) {

	b.lock.Lock()
	defer b.lock.Unlock()

	// add to followers set
	b.followers[conn] = true

	// loop through topics of offsets
	for topic, offset := range offsets {
		bLog, exists := b.logs[topic]
		if !exists {
			continue
		}
		err := b.sendSyncRequest(conn, bLog.HighWaterMark(), offset, topic, bLog)
		if err == io.EOF {
			// return if connection breaks
			return
		}
	}
}*/

//updates to check if follower has caught up or not
/*func (b *Broker) SyncFollower(conn *protocol.Follower, ack protocol.SyncACK) {
	b.lock.Lock()
	defer b.lock.Unlock()
	bLog, exists := b.logs[ack.Topic]
	if !exists {
		return
	}
	_, exists = b.insyncFollowers[ack.Topic][conn]
	if !exists {
		// ACK from out of sync follower
		b.sendSyncRequest(conn, bLog.HighWaterMark(), ack.Offset, ack.Topic, bLog)
	} else {
		// ACK of write from in-sync follower
		// TODO: record ACK and determine if send commit
	}
}*/

/*func (b *Broker) sendSyncRequest(conn *protocol.Follower, myOffset int64, followerOffset int64, topic string, blog *brokerlog.BLog) error {
	var sreq protocol.SyncRequest
	sreq.Topic = topic
	if myOffset <= followerOffset {
		// caught up!
		b.insyncFollowers[topic][conn] = true
		sreq.Type = protocol.COMMIT
		buf := new(bytes.Buffer)
		// XXX: what if write error?
		binary.Write(buf, binary.LittleEndian, myOffset)
		sreq.Message = buf.Bytes()
	} else {
		// not yet caught up!
		// XXX: what if read error?
		msg, _, _ := blog.Read(followerOffset)
		sreq.Type = protocol.UPDATE
		sreq.Message = msg
	}
	return websocket.JSON.Send(conn.Conn, sreq)
}

func (b *Broker) DeleteFollower(follower *protocol.Follower) {
	b.lock.Lock()
	defer b.lock.Unlock()
	delete(b.followers, follower)
	for _, followers := range b.insyncFollowers {
		delete(followers, follower)
	}
}*/
