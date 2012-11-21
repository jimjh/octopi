package brokerimpl

import(
	"code.google.com/p/go.net/websocket"
	"octopi/api/messageapi"
	"container/list"
	"sync"
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

func NewBroker(rbi messageapi.RegBrokerInit, regconn *websocket.Conn) (*Broker, error){
	b := &Broker{
		role: rbi.Role,
		consumers: list.New(),
		topics: rbi.Topics,
		leadUrl: rbi.LeadUrl,
		leadOrigin: rbi.LeadOrigin,
	}
	if rbi.Role==messageapi.LEADER{
		b.brokers = list.New()
		b.producers = list.New()
		b.regConn = regconn
	} else{
		leadConn, err := websocket.Dial(rbi.LeadUrl, "", rbi.LeadOrigin)
		if nil != err { return nil, err }	
		b.leadConn = leadConn
	}
	return b, nil
}

func (b *Broker) Role() int{
	return b.role
}
