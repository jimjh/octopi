package producer

import (
	"code.google.com/p/go.net/websocket"
	"net"
	"net/http"
	"octopi/api/protocol"
	"octopi/util/test"
	"strconv"
	"sync"
	"testing"
)

type ptr struct {
	messages []*protocol.Message
}

func accept(ref *ptr) func(*websocket.Conn) {
	lock := new(sync.Mutex)
	return func(conn *websocket.Conn) {
		for {
			var msg protocol.ProduceRequest
			if err := websocket.JSON.Receive(conn, &msg); nil != err {
				return
			}
			lock.Lock()
			ref.messages = append(ref.messages, &msg.Message)
			lock.Unlock()
			ack := &protocol.Ack{protocol.StatusSuccess, nil}
			if err := websocket.JSON.Send(conn, ack); nil != err {
				return
			}
		}
	}
}

// TestBroker ensures that producer sends messages to the broker.
func TestBroker(tester *testing.T) {

	t := test.New(tester)

	listener, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	ref := &ptr{make([]*protocol.Message, 0)}
	server := &http.Server{
		Handler: websocket.Handler(accept(ref)),
	}

	go server.Serve(listener)

	producer := New("localhost:11111", nil)
	for i := 0; i < 10; i++ {
		err = producer.Send("x", []byte(strconv.Itoa(i)))
		t.AssertNil(err, "producer.Send")
	}

	listener.Close()
	t.AssertEqual(new(test.IntMatcher), 10, len(ref.messages))

	for i, msg := range ref.messages {
		value, err := strconv.Atoi(string(msg.Payload))
		t.AssertNil(err, "strconv.Atoi")
		t.AssertEqual(new(test.IntMatcher), i, value)
		i++
	}

}

func lost(count *int) func(*websocket.Conn) {
	return func(conn *websocket.Conn) {
		*count++
		conn.Close()
	}
}

// TestRetries ensures that producer retries is the connection is lost.
func TestRetries(tester *testing.T) {

	t := test.New(tester)
	count := 0

	listener, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	server := &http.Server{
		Handler: websocket.Handler(lost(&count)),
	}

	go server.Serve(listener)

	producer := New("localhost:11111", nil)
	err = producer.Send("x", []byte("abc"))
	t.AssertNotNil(err, "producer.Send")

	listener.Close()
	t.AssertEqual(new(test.IntMatcher), MAX_RETRIES, count)

}
