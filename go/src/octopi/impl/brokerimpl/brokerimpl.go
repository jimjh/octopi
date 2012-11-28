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
	leadChan      chan int                    // channel to make sure leader not nil
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
		leadChan:      make(chan int),
	}
	b.cond = sync.NewCond(&b.lock)

	if FOLLOWER == config.Role() {
		// go b.register(config.Register())
	}

	return b

}

// register sends a follow request to the given leader.
// TODO: implement redirect.
func (b *Broker) register(hostport string) {

	var err error
	var leader *websocket.Conn

	endpoint := "ws://" + hostport + "/" + protocol.FOLLOW
	origin := b.origin()

	for { // keep trying to connect

		leader, err = websocket.Dial(endpoint, "", origin)
		if nil == err {
			websocket.JSON.Send(leader, protocol.FollowRequest{b.tails()})
			// TODO: wait for ack
			break
		}

		log.Warn("Error: %s. Retrying ...", err.Error())
		backoff := time.Duration(rand.Intn(protocol.MAX_RETRY_INTERVAL))
		time.Sleep(backoff * time.Millisecond)

	}

	b.lock.Lock()
	defer b.lock.Unlock()
	b.leader = leader

}

// tails returns the sizes of all log files, organized by their topics.
func (b *Broker) tails() map[string]int64 {
	// TODO
	return nil
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
// 	<-b.leadChan
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

// Subscribe creates a new subscription for the given consumer connection.
// Consumers are allowed to register for non-existent topics, but will not
// receive any messages until a producer publishes a message under that topic.
func (b *Broker) Subscribe(
	conn *websocket.Conn,
	topic string,
	offset int64) (*Subscription, error) {

	// create new subscription
	subscription, err := NewSubscription(b, conn, topic, offset)
	if nil != err {
		return nil, err
	}

	var subscriptions SubscriptionSet

	b.lock.Lock()
	defer b.lock.Unlock()

	// save subscription
	subscriptions, exists := b.subscriptions[topic]
	if !exists {
		subscriptions = make(map[*Subscription]bool)
		b.subscriptions[topic] = subscriptions
	}
	subscriptions[subscription] = true

	return subscription, nil

}

// Unsubscribe removes the given subscription from the broker.
func (b *Broker) Unsubscribe(topic string, subscription *Subscription) {

	b.lock.Lock()
	defer b.lock.Unlock()

	subscriptions, exists := b.subscriptions[topic]
	if !exists {
		return
	}

	subscription.quit <- nil
	delete(subscriptions, subscription)

}

// wait checks if the subscription is really at the end of the log. It returns
// iff there is more to be read.
func (b *Broker) wait(s *Subscription) {

	b.lock.Lock()
	defer b.lock.Unlock()

	for s.log.IsEOF() {
		b.cond.Wait()
		return
	}

}

// Publish publishes the given message to all subscribers.
func (b *Broker) Publish(topic, producer string, msg *protocol.Message) error {

	// TODO: topic-specific locks
	b.lock.Lock()
	defer b.lock.Unlock()

	var err error
	file, exists := b.logs[topic]
	if !exists {
		file, err = OpenLog(b.config, topic, -1)
		if nil != err {
			return err
		} else {
			b.logs[topic] = file
		}
	}

	err = file.Append(producer, msg)
	if nil != err {
		return err
	}

	b.cond.Broadcast()
	return nil

}
