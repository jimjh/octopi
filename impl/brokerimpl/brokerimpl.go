package brokerimpl

import(
	"code.google.com/p/go.net/websocket"
	"container/list"
	"sync"
)

const(
	LEADER = iota
	FOLLOWER
)

type Broker struct{
	role int //role of the broker
	brokers *list.List //list of assoc broker connections, for leaders
	producers *list.List //list of producer connections, for leaders
	consumers *list.List //list of consumer connections
	topics *list.List //list of topics handled
	leadUrl string //url of the leader, for followers
	leadOrigin string //origin of the leader, for followers
	leadConn *websocket.Conn //connection to the leader, for followers
	regConn *websocket.Conn //connection to the register, for leaders
	lock sync.Mutex //lock to manage broker access
}

type RegBrokerAssign struct{
	Role int
	LeadUrl string //url of the leader, for followers
	LeadOrigin string //origin of the leader, for followers
}

func NewBroker(rba RegBrokerAssign, regconn *websocket.Conn) *Broker{
	b := &Broker{
		role: rba.Role,
		consumers: list.New(),
		topics: list.New(),
		leadUrl: rba.LeadUrl,
		leadOrigin: rba.LeadOrigin,
	}
	if rba.Role==LEADER{
		b.brokers = list.New()
		b.producers = list.New()
		//TODO: CONTINUE HERE!
	} else{
		//TODO: DIAL LEADCONN
	}
	return b
}

func (b *Broker) Role() int{
	return b.role
}
