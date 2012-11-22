package protocol

import (
	"code.google.com/p/go.net/websocket"
	"container/list"
)

// Constants for contacting register.
const (
	BROKER = iota
	PRODUCER
	CONSUMER
)

// Constants for register to tell which role a broker is taking.
const (
	LEADER = iota
	FOLLOWER
)

type FollowWSConn struct{
	FollowWS *websocket.Conn
	Block chan interface{}
}

// Used by brokers to contact register.
type BrokerRegInit struct {
	MessageSrc int
	HostPort   string
}

/* used by register to assign brokers */
type RegBrokerInit struct {
	Role       int
	LeadUrl    string     //url of the leader, for followers
	LeadOrigin string     //origin of the leader, for followers
	Brokers    *list.List //list of associated brokers used if new leader
}

/* used by producers to contact register */
type ProdRegInit struct {
	MessageSrc int
	Topic      string
}

/* used by register to tell producer the lead broker */
type RegProdInit struct {
	LeadUrl    string
	LeadOrigin string
}

/* used by consumers to contact register */
type ConsRegInit struct {
	MessageSrc int
	Topic      string
}

/* used by register to tell consumer its assigned broker */
type RegConsInit struct {
	BrokerUrl    string
	BrokerOrigin string
}

/* used by followers to contact leader */
type FollowLeadInit struct {
	MessageSrc int
	HostPort   string
	// TODO: other fields to identify position of log
}

// PublishRequests are sent from producers to brokers when they want to start
// sending messages under a particular topic.
// XXX: why do we need this? why can't producers send produce requests
// directly?
// XXX: perhaps keeping a persistent connection is a good idea.
type PublishRequest struct {
	MessageSrc int
	Topic      string
	// TODO: do we need acknowledgments?
}

// SubscribeRequests are sent from consumers to brokers when they want messages
// from a particular topic.
type SubscribeRequest struct {
	MessageSrc int // XXX: should use inheritance here.
	Topic      string
	// TODO: other fields to identify position of wanted
}

// ProduceRequests are messages sent from producers to brokers; the enclosed
// message is broadcast to all consumers subscribing to the topic.
type ProduceRequest struct {
	Topic   string
	Message string
}
