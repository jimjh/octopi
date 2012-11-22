package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"octopi/api/protocol"
)

// Publications are used to store producer connections; each publication has a
// go channel that relays messages from the producer.
//
// XXX: for now, a publication is defined as a persistent connection for one
// more more produce requests. Should probably rename this.
type Publication struct {
	conn    *websocket.Conn  // producer websocket connection
	broker  *Broker          // message broker
	receive chan interface{} // go channel for relaying messages
	topic   string           // message topic
}

// NewPublication creates a new publication.
func NewPublication(conn *websocket.Conn, topic string, broker *Broker) *Publication {
	return &Publication{
		conn:    conn,
		broker:  broker,
		receive: make(chan interface{}, 1), // TODO: make this more robust
		topic:   topic,
	}
}

// Serve blocks until the websocket connection is broken. Closes the `receive`
// channel when it returns.
// TODO: allow broker to shut down publications?
func (p *Publication) Serve() error {

	var err error
	for {
		var message protocol.Message
		err = websocket.JSON.Receive(p.conn, &message)
		if nil != err {
			break
		}
		p.broker.Publish(p.topic, &message)
	}

	close(p.receive)
	return err

}
