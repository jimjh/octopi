package producer

import (
	"code.google.com/p/go.net/websocket"
	"hash/crc32"
	"math/rand"
	"octopi/api/protocol"
	"octopi/util/log"
	"sync"
	"sync/atomic"
	"time"
)

type Producer struct {
	conn   *websocket.Conn // persistent websocket connection
	seqnum uint32          // sequence number of messages
	lock   sync.Mutex      // lock for producer state
}

// Default origin to use.
var ORIGIN = "http://localhost"

// Max number of retries.
var MAX_RETRIES = 5

// Max number of milliseconds between retries.
var MAX_RETRY_INTERVAL = 2000

// New creates a new producer that sends messages to broker that lives
// at the given hostport.
func New(hostport string) (*Producer, error) {

	p := new(Producer)
	addr := "ws://" + hostport + "/" + protocol.PUBLISH

	if err := p.connect(addr); nil != err {
		return nil, err
	}

	return p, nil

}

// Establishes a websocket connection to the broker.
func (p *Producer) connect(addr string) error {

	conn, err := websocket.Dial(addr, "", ORIGIN)

	if nil != err {
		log.Error(err.Error())
	} else {
		p.conn = conn
		log.Info("Established new connection to broker at %v.", conn.RemoteAddr())
	}

	return err

}

// Locks down producer, closes current connection, and reconnects to broker.
func (p *Producer) reconnect() error {

	p.lock.Lock()
	defer p.lock.Unlock()

	p.conn.Close()

	// back off and reconnect
	backoff := time.Duration(rand.Intn(MAX_RETRY_INTERVAL))
	time.Sleep(backoff * time.Millisecond)
	return p.connect(p.conn.RemoteAddr().String())

}

// Send sends the message to the broker, and blocks until an acknowledgement is
// received.
func (p *Producer) Send(topic string, payload []byte) error {

	seqnum := atomic.AddUint32(&p.seqnum, 1)
	message := protocol.Message{seqnum, payload, crc32.ChecksumIEEE(payload)}
	request := &protocol.ProduceRequest{topic, message}

	log.Debug("Sending %v", request)

	var err error
	for tries := 1; tries <= MAX_RETRIES; tries++ {

		err = websocket.JSON.Send(p.conn, request)
		if nil == err {
			// TODO: wait for ack
			// TODO: timeout and retry
			break // success
		}

		log.Error(err.Error())
		p.reconnect()

	}

	return err

}

// Close closes the producer's websocket connection. Must not be invoked while
// there are still Sends pending.
func (p *Producer) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.conn.Close()
}
