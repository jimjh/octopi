package brokerimpl

import(
	"code.google.com/p/go.net/websocket"
	"octopi/api/messageapi"
	"container/list"
	"sync"
	"errors"
)

type WebSocketConn struct{
	HostPort string
	Conn *websocket.Conn
}

type Broker struct{
	role int //role of the broker
	brokerConns map[string]*websocket.Conn //map of assoc broker hostport to connections, for leaders
	prodTopicsMap map[string]*list.List //map of topics to producer connections
	consTopicsMap map[string]*list.List //list of topics to consumer connections
	leadUrl string //url of the leader, for followers
	leadOrigin string //origin of the leader, for followers
	leadConn *websocket.Conn //connection to the leader, for followers
	regConn *websocket.Conn //connection to the register, for leaders
	lock sync.Mutex //lock to manage broker access
}

func NewBroker(rbi messageapi.RegBrokerInit, regconn *websocket.Conn) (*Broker, error){

	/* create the broker */
	b := &Broker{
		role: rbi.Role,
		consTopicsMap: make(map[string]*list.List),
		leadUrl: rbi.LeadUrl,
		leadOrigin: rbi.LeadOrigin,
	}

	/* initialize list of connections for each topic */
	l := rbi.Topics
	for e := l.Front(); e != nil; e = e.Next() {
		b.consTopicsMap[e.Value.(string)] = list.New()
	}

	/* only initialize these variables if leader. otherwise leave as nil */
	if rbi.Role==messageapi.LEADER{
		b.brokerConns = make(map[string]*websocket.Conn)
		b.prodTopicsMap = make(map[string]*list.List)
		b.regConn = regconn

		//TODO: make websocket connection to brokers. use rbi.Brokers
	} else{
		leadConn, err := websocket.Dial(rbi.LeadUrl, "", rbi.LeadOrigin)
		if nil != err { return nil, err }
		b.leadConn = leadConn
	}

	return b, nil
}

func (b *Broker) RegProd(ws *websocket.Conn, pli messageapi.ProdLeadInit){
	b.lock.Lock()
	defer b.lock.Unlock()
	wsc := WebSocketConn{pli.HostPort, ws}
	b.prodTopicsMap[pli.Topic].PushBack(wsc)
}

func (b *Broker) RemoveProd(pli messageapi.ProdLeadInit){
	b.lock.Lock()
	defer b.lock.Unlock()
	l := b.prodTopicsMap[pli.Topic]
	for e := l.Front(); e !=nil; e = e.Next(){
		if e.Value.(WebSocketConn).HostPort==pli.HostPort{
			l.Remove(e)
			return
		}
	}
}

func (b *Broker) FollowBroadcast(msg messageapi.PubMsg){
	b.lock.Lock()
	defer b.lock.Unlock()
	/* loop through all follwing broker connections and send message to update */
	for hostport, conn := range b.brokerConns{
		go b.sendToFollow(hostport, conn, msg)
	}
}

func (b *Broker) sendToFollow(hostport string, ws *websocket.Conn, msg messageapi.PubMsg){
	err := websocket.JSON.Send(ws, msg)
	if nil!=err{
		b.lock.Lock()
		delete(b.brokerConns, hostport)
		ws.Close()
		defer b.lock.Unlock()
	}
}

func (b *Broker) RegCons(ws *websocket.Conn, cbi messageapi.ConsBrokerInit) error{
	b.lock.Lock()
	defer b.lock.Unlock()
	/* topic does not exist! return error */
	if nil==b.consTopicsMap[cbi.Topic]{
		return errors.New("Topic does not exist")
	}
	/* save connection in list of consumer topics map */
	wsc := WebSocketConn{cbi.HostPort, ws}
	b.consTopicsMap[cbi.Topic].PushBack(wsc)
	return nil
}

func (b *Broker) RegBroker(ws *websocket.Conn, fli messageapi.FollowLeadInit){
	b.lock.Lock()
	defer b.lock.Unlock()
	
}

func (b *Broker) Role() int{
	return b.role
}
