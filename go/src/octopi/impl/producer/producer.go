package producer

import (
	"code.google.com/p/go.net/websocket"
	"hash/crc32"
	"octopi/api/protocol"
	"octopi/util/log"
)

type Producer struct {
	conn *websocket.Conn // persistent websocket connection
}

var ORIGIN = "http://localhost"

// New creates a new producer that sends messages to broker that lives
// at the given hostport.
func New(hostport string, topic string) (*Producer, error) {

	conn, err := websocket.Dial("ws://"+hostport+"/"+protocol.PUBLISH, "", ORIGIN)
	if nil != err {
		log.Error(err.Error())
		return nil, err
	}

	request := &protocol.PublishRequest{protocol.PRODUCER, topic}
	if err := websocket.JSON.Send(conn, request); nil != err {
		log.Error(err.Error())
		conn.Close()
		return nil, err
	}
	log.Info("Established connection to broker at %v.", conn.RemoteAddr())

	return &Producer{conn}, nil

}

// Send sends the message to the broker, and blocks until an acknowledgement is
// received.
func (p *Producer) Send(payload []byte) error {

	message := &protocol.Message{0, payload, crc32.ChecksumIEEE(payload)}

	err := websocket.JSON.Send(p.conn, message)
	if nil != err {
		log.Error(err.Error())
		return err
	}

	// TODO: retry? return error? reconnect?

	// TODO: wait for ack
	return nil

}

// Close closes the producer's websocket connection. Must not be invoked while
// there are still Sends pending.
func (p *Producer) Close() {
	p.conn.Close()
}
