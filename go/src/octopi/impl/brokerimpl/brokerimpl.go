package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"bytes"
	"encoding/binary"
	"math/rand"
	"octopi/api/protocol"
	"octopi/impl/brokerlog"
	"octopi/util/log"
	"os"
	"io"
	"sync"
	"time"
)

// Brokers relay messages from producers to followers. Every broker is a
// follower, and one of the follower is a broker (so it registers with itself.)
type Broker struct {
	followers       map[*protocol.Follower]bool // set of followers
	insyncFollowers map[string]FollowerSet      //list of followers that are in-sync with leader on a certain topic
	subscriptions   map[string]SubscriptionSet  // map of topics to consumer connections
	logs            map[string]*brokerlog.BLog  // map of topics to logs
	leader          *websocket.Conn             // connection to the leader
	port            int                         // port number of this broker
	lock            sync.Mutex                  // lock to manage broker access
}

// SubscriptionSet implemented as a map from *Subscription to true.
type SubscriptionSet map[*Subscription]bool
type FollowerSet map[*protocol.Follower]bool

// New takes the host:port of the registry/leader and creates a new broker.
func New(port int, master string) *Broker {

	b := &Broker{
		subscriptions:   make(map[string]SubscriptionSet),
		followers:       make(map[*protocol.Follower]bool),
		insyncFollowers: make(map[string]FollowerSet),
	}

	go b.register(master)
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
			// TODO: send follow request, wait for ACK
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

// origin returns the host:port of this broker.
func (b *Broker) origin() string {

	host, err := os.Hostname()
	if nil != err {
		log.Panic(err.Error())
	}

	return fmt.Sprintf("%s:%d", host, b.port)

}

// Publish publishes the given message to all subscribers.
func (b *Broker) Publish(topic string, msg *protocol.Message) error {

	b.lock.Lock()
	defer b.lock.Unlock()

	bLog, exists := b.logs[topic]
	if !exists {
		// TODO: determine path of logs
		tmpLog, err := brokerlog.OpenLog(topic)
		if nil != err {
			//TODO: cannot open log!
			return err
		} else {
			b.logs[topic] = tmpLog
			bLog = tmpLog
		}
		followerset := make(map[*protocol.Follower]bool)
		b.insyncFollowers[topic] = followerset
	}

	bLog = bLog // FIXME: Temporary placeholder

	// b.FollowBroadcast(msg)

	// TODO: message duplication check and follower synchronization
	// XXX: how to check for duplication in logs? need to loop through?
	// XXX: just need to check the last entry
	// TODO: this is not a good idea (should be async.)
	// XXX: before writing to file, make sure to replace ID with offset and check
	// for duplicates.

	subscriptions, exists := b.subscriptions[topic]
	if !exists { // no subscribers
		return nil
	}

	for subscription := range subscriptions {
		subscription.send <- msg
	}

	return nil

}

/*func (b *Broker) sendToFollow(hostport string, ws *protocol.FollowWSConn, msg *protocol.Message) {
	err := websocket.JSON.Send(ws.FollowWS, msg)
	if nil != err {
		b.lock.Lock()
		defer b.lock.Unlock()
		delete(b.brokerConns, hostport)
		ws.FollowWS.Close()
		ws.Block <- nil
	}
}*/

/*func (b *Broker) CacheFollower(hostport string, fconn *protocol.FollowWSConn) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.brokerConns[hostport] = fconn
}*/

func (b *Broker) RegisterFollower(conn *protocol.Follower, offsets map[string]int64) {

	b.lock.Lock()
	defer b.lock.Unlock()

	// add to followers set
	b.followers[conn] = true

	// loop through topics of offsets
	for topic, offset := range offsets {
		bLog, exists := b.logs[topic]
		if !exists {continue}
		err := b.sendSyncRequest(conn, bLog.HighWaterMark(), offset, topic, bLog)
		if err == io.EOF{
			// return if connection breaks
			return
		}
	}
}

//updates to check if follower has caught up or not
func (b* Broker) SyncFollower(conn *protocol.Follower, ack protocol.SyncACK){
	b.lock.Lock()
	defer b.lock.Unlock()
	bLog, exists := b.logs[ack.Topic]
	if !exists{ return }
	b.sendSyncRequest(conn, bLog.HighWaterMark(), ack.Offset, ack.Topic, bLog)
}

func (b *Broker) sendSyncRequest(conn *protocol.Follower, myOffset int64, followerOffset int64, topic string, blog *brokerlog.BLog) error{
	var sreq protocol.SyncRequest
	sreq.Topic = topic
	if myOffset <= followerOffset{
		// caught up!
		b.insyncFollowers[topic][conn] = true
		sreq.Type = protocol.COMMIT
		buf := new(bytes.Buffer)
		// XXX: what if write error?
		binary.Write(buf, binary.LittleEndian, myOffset)
		sreq.Message = buf.Bytes()
	} else{
		// not yet caught up!
		// XXX: what if read error?
		msg, _, _ := blog.Read(followerOffset)
		sreq.Type = protocol.UPDATE
		sreq.Message = msg
	}
	return websocket.JSON.Send(conn.Conn, sreq)
}

func (b *Broker) DeleteFollower(follower *protocol.Follower){
	b.lock.Lock()
	defer b.lock.Unlock()
	delete(b.followers, follower)
	for _, followers := range b.insyncFollowers{
		delete(followers, follower)
	}
}

// Subscribe creates a new subscription for the given consumer connection.
// Consumers are allowed to register for non-existent topics, but will not
// receive any messages until a producer publishes a message under that topic.
func (b *Broker) Subscribe(conn *websocket.Conn, topic string) *Subscription {

	// create new subscription
	subscription := NewSubscription(conn)
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

	// serve
	go subscription.Serve()
	return subscription

}

// Unsubscribe removes the given subscription from the broker.
func (b *Broker) Unsubscribe(topic string, subscription *Subscription) {

	b.lock.Lock()
	defer b.lock.Unlock()

	subscriptions, exists := b.subscriptions[topic]
	if !exists {
		return
	}

	close(subscription.send)
	delete(subscriptions, subscription)

}
