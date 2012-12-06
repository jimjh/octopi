package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"hash/crc32"
	"net"
	"net/http"
	"octopi/api/protocol"
	"octopi/util/test"
	"os"
	"testing"
	"time"
)

// TestNewSubscription tests that we can create a new subscription with a new
// topic.
func TestNewSubscription(tester *testing.T) {

	t := test.New(tester)
	config := newTestConfig()
	register := newTestRegister()
	defer register.Close()

	broker, err := New(&config.Config)
	t.AssertNil(err, "New")

	subscription, err := NewSubscription(broker, nil, "whatever", 0)

	t.AssertNotNil(subscription, "NewSubscription")
	t.AssertNil(err, "NewSubscription")

}

// TestQuit ensures that the subscription can be terminated using the quit
// channel.
func TestQuit(tester *testing.T) {

	t := test.New(tester)
	config := newTestConfig()
	register := newTestRegister()
	defer register.Close()

	broker, err := New(&config.Config)
	t.AssertNil(err, "New")

	subscription, err := NewSubscription(broker, nil, "whatever", 0)

	t.AssertNotNil(subscription, "NewSubscription")
	t.AssertNil(err, "NewSubscription")

	go func() {
		time.Sleep(200 * time.Millisecond)
		subscription.quit <- nil
		broker.cond.Broadcast()
	}()

	err = subscription.Serve() // should return after 200ms
	t.AssertNil(err, "subscription.Serve")

}

// TestServe ensures that the subscription can serve messages from the log
// file.
func TestServe(tester *testing.T) {

	config := newTestConfig()
	t := test.New(tester)
	var result byte = 0

	register := newTestRegister()
	client, listener := newTestClient(t, sum(t, &result))
	defer register.Close()

	log, err := OpenLog(config, "temp", 0)
	t.AssertNil(err, "OpenLog")

	defer log.Close()
	defer os.Remove(log.Name())

	var i byte
	for i = 1; i <= 10; i++ {
		payload := []byte{i}
		message := &protocol.Message{int64(i), payload, crc32.ChecksumIEEE(payload)}
		_, err := log.Append("x", message)
		t.AssertNil(err, "log.Append")
	}

	broker, err := New(&config.Config)
	t.AssertNil(err, "New")

	subscription, err := NewSubscription(broker, client, "temp", 0)
	t.AssertNil(err, "NewSubscription")

	go func() {
		time.Sleep(1 * time.Second)
		client.Close()
		listener.Close()
		subscription.quit <- nil
		broker.cond.Broadcast()
	}()

	err = subscription.Serve()
	t.AssertNil(err, "subscription.Serve()")
	t.AssertEqual(new(test.IntMatcher), 55, int(result))

}

func newTestClient(t *test.Test, f func(*websocket.Conn)) (*websocket.Conn, net.Listener) {

	listener, err := net.Listen("tcp", ":"+testSocketPort)
	if nil != err {
		panic(err)
	}

	server := &http.Server{Handler: websocket.Handler(f)}
	go server.Serve(listener)

	conn, err := websocket.Dial("ws://localhost:"+testSocketPort, "", "xyz:12312")
	if nil != err {
		panic(err)
	}

	return conn, listener

}

// sum just sums up all values received from the connection.
func sum(t *test.Test, x *byte) func(*websocket.Conn) {
	return func(conn *websocket.Conn) {
		defer conn.Close()
		for {
			message := new(protocol.Message)
			if err := websocket.JSON.Receive(conn, message); nil != err {
				return
			}
			*x += message.Payload[0]
		}
	}
}

// TestWait ensures that subscribers can wait on producers.
func TestWait(tester *testing.T) {

	config := newTestConfig()
	t := test.New(tester)
	var result byte = 0

	register := newTestRegister()
	client, listener := newTestClient(t, sum(t, &result))
	defer register.Close()
	defer client.Close()
	defer listener.Close()

	broker, err := New(&config.Config)
	t.AssertNil(err, "New")

	subscription, err := NewSubscription(broker, client, "temp", 0)
	t.AssertNil(err, "NewSubscription")

	go func() {
		time.Sleep(1 * time.Second)
		var i byte
		for i = 1; i <= 10; i++ {
			payload := []byte{i}
			message := &protocol.Message{int64(i), payload, crc32.ChecksumIEEE(payload)}
			err = broker.Publish("temp", "x", message)
			t.AssertNil(err, "broker.Publish")
		}
		time.Sleep(500 * time.Millisecond)
		subscription.quit <- nil
		broker.cond.Broadcast()
	}()

	err = subscription.Serve()
	t.AssertNil(err, "subscription.Serve()")
	t.AssertEqual(new(test.IntMatcher), 55, int(result))

	log, err := OpenLog(config, "temp", 0)
	os.Remove(log.Name())

}
