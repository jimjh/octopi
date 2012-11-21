package protocol

import (
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

type ProdLeadInit struct {
	MessageSrc int
	HostPort   string
	Topic      string
}

// SubscribeRequests are sent from consumers to brokers when they want messages
// from a particular topic.
type SubscribeRequest struct {
	MessageSrc int // XXX: should use inheritance here.
	Topic      string
	// TODO: other fields to identify position of wanted
}

/* used by producers to send publications to brokers*/
type PubMsg struct {
	Topic   string
	Message string
}
