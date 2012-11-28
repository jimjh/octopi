package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"io"
	"octopi/util/log"
)

// Subscriptions are used to store consumer connections; each subscription has
// a go channel that relays messages to the consumer.
type Subscription struct {
	broker *Broker
	topic  string           // topic
	conn   *websocket.Conn  // consumer websocket connection
	log    *Log             // broker log
	send   chan interface{} // <- a new msg is available
}

// NewSubscription creates a new subscription for the given topic. Messages are
// sent on the given connection.
func NewSubscription(
	broker *Broker,
	conn *websocket.Conn,
	topic string,
	offset int64) (*Subscription, error) {

	log, err := OpenLog(broker.config, topic, offset)
	if nil != err {
		return nil, err
	}

	return &Subscription{
		broker: broker,
		topic:  topic,
		conn:   conn,
		log:    log,
		send:   make(chan interface{}, 1),
	}, nil

}

// Serve blocks until either the websocket connection or channel is closed.
// This method may only be invoked once; after it returns, the subscription is
// closed.
func (s *Subscription) Serve() error {

	/*var err error
	for message := range s.send {
		err = websocket.JSON.Send(s.conn, message)
		if nil != err {
			break
		}
	}*/

	var err error
	for nil == err {
		entry, err := s.log.ReadNext()
		switch err {
		case nil: // send to consumer
			err = websocket.JSON.Send(s.conn, &entry.Message)
		case io.EOF: // wait for ping
			if s.broker.wait(s) {
				<-s.send
			}
		default: // abort
			log.Error("Error reading from log: ", err.Error())
			break
		}
	}

	log.Debug("Stopped serving subscription %p.", s)

	s.log.Close()
	return err

}
