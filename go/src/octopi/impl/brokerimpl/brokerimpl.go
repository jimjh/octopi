package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"container/list"
	"octopi/api/protocol"
	"octopi/util/brokerlog"
	"sync"
)

type Broker struct {
	role          int                               // role of the broker
	brokerConns   map[string]*protocol.FollowWSConn // map of assoc broker hostport to connections, for leaders
	subscriptions map[string]*list.List             // map of topics to consumer connections
	logs          map[string]*brokerlog.BLog        // map of topics to logs
	leadUrl       string                            // url of the leader, for followers
	leadOrigin    string                            // origin of the leader, for followers
	leadConn      *websocket.Conn                   // connection to the leader, for followers
	lock          sync.Mutex                        //  lock to manage broker access
}

func NewBroker(rbi protocol.RegBrokerInit, regconn *websocket.Conn) (*Broker, error) {

	/* create the broker */
	b := &Broker{
		role:          rbi.Role,
		subscriptions: make(map[string]*list.List),
		leadUrl:       rbi.LeadUrl,
		leadOrigin:    rbi.LeadOrigin,
	}

	/* only initialize these variables if leader. otherwise leave as nil */
	if rbi.Role == protocol.LEADER {
		b.brokerConns = make(map[string]*protocol.FollowWSConn)
	} else {
		leadConn, err := websocket.Dial(rbi.LeadUrl, "", rbi.LeadOrigin)
		if nil != err {
			return nil, err
		}
		b.leadConn = leadConn
	}

	return b, nil
}

// Publish publishes the given message to all subscribers.
func (b *Broker) Publish(topic string, msg *protocol.Message) error {

	b.lock.Lock()
	defer b.lock.Unlock()

	// bLog, exists := b.logs[topic]
	_, exists := b.logs[topic] // FIXME: cannot compile
	var err error
	if !exists {
		// TODO: determine path of logs
		// tmpLog, err := brokerlog.OpenLog(topic)
		if nil != err {
			//TODO: cannot open log!
		} else {
			// b.logs[topic] = tmpLog
			// bLog = tmpLog
			// FIXME: cannot compile
		}
	}

	b.FollowBroadcast(msg)

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

	for ele := subscriptions.Front(); ele != nil; ele = ele.Next() {
		subscription := ele.Value.(*Subscription)
		subscription.send <- msg
	}

	return nil

}

func (b *Broker) FollowBroadcast(msg *protocol.Message) {
	/* loop through all following broker connections and send message to update */
	for hostport, conn := range b.brokerConns {
		go b.sendToFollow(hostport, conn, msg)
	}
}

func (b *Broker) sendToFollow(hostport string, ws *protocol.FollowWSConn, msg *protocol.Message) {
	err := websocket.JSON.Send(ws.FollowWS, msg)
	if nil != err {
		b.lock.Lock()
		defer b.lock.Unlock()
		delete(b.brokerConns, hostport)
		ws.FollowWS.Close()
		/* allows websocket handler */
		ws.Block <- nil
	}
}

func (b *Broker) CacheFollower(hostport string, fconn *protocol.FollowWSConn) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.brokerConns[hostport] = fconn
}

// Subscribe creates a new subscription for the given consumer connection.
// Consumers are allowed to register for non-existent topics, but will not
// receive any messages until a producer publishes a message under that topic.
func (b *Broker) Subscribe(conn *websocket.Conn, topic string) error {

	// create new subscription
	subscription := NewSubscription(conn)
	var subscriptions *list.List

	b.lock.Lock()

	// save subscription
	subscriptions, exists := b.subscriptions[topic]
	if !exists {
		b.subscriptions[topic] = list.New()
		subscriptions = b.subscriptions[topic]
	}
	subscriptions.PushBack(subscription)

	b.lock.Unlock()

	// serve (blocking call)
	go subscription.Serve()
	return nil

}

func (b *Broker) Role() int {
	return b.role
}
