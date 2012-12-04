package producer

import (
	crand "crypto/rand"
	"fmt"
	"hash/crc32"
	"math"
	"math/big"
	"math/rand"
	"octopi/api/protocol"
	"octopi/util/log"
	"os"
	"sync"
	"sync/atomic"
)

// Producers publish messages to brokers.
type Producer struct {
	socket *protocol.Socket // protocol socket
	seqnum int64            // sequence number of messages
	lock   sync.Mutex       // lock for producer state
	id     string           // producer ID
}

// Max number of retries.
const MAX_RETRIES = 5

// seed seeds the random number generator
func seed() {
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
}

// origin returns the hostname
func origin() string {
	name, err := os.Hostname()
	if nil != err {
		log.Panic(err.Error())
	}
	return "ws://" + name
}

// New creates a new producer that sends messages to broker that lives
// at the given hostport.
func New(hostport string, id *string) *Producer {

	if nil == id {
		seed()
		idStr := fmt.Sprintf("%d", rand.Uint32())
		id = &idStr
	}

	socket := &protocol.Socket{
		HostPort: hostport,
		Path:     protocol.PUBLISH,
		Origin:   origin(),
	}

	return &Producer{id: *id, socket: socket}

}

// Send sends the message to the broker, and blocks until an acknowledgement is
// received. If the max number of retries is exceeded, returns the last error.
func (p *Producer) Send(topic string, payload []byte) error {

	seqnum := atomic.AddInt64(&p.seqnum, 1)
	message := protocol.Message{seqnum, payload, crc32.ChecksumIEEE(payload)}
	request := &protocol.ProduceRequest{p.id, topic, message}

	log.Debug("Sending %v", request)

	p.lock.Lock()
	defer p.lock.Unlock()
	_, err := p.socket.Send(request, MAX_RETRIES, origin())

	if nil != err {
		return &ProduceError{seqnum}
	}

	log.Debug("Acknowledgement received.")
	return nil

}

// Close closes the producer's websocket connection. Must not be invoked while
// there are still Sends pending.
func (p *Producer) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.socket.Close()
}

// ProduceErrors indicate that a produce request failed.
type ProduceError struct {
	seqnum int64
}

func (e *ProduceError) Error() string {
	return fmt.Sprintf("Acknowledgement for message %d was not received.", e.seqnum)
}
