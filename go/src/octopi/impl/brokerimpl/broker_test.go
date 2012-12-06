package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"net"
	"net/http"
	"octopi/util/config"
	"os"
	"strconv"
	"testing"
)

const testRegisterPort = "11111"
const testSocketPort = "11112"

// newTestConfig creates a new test configuration.
func newTestConfig() *Config {
	options := &config.Config{Options: make(map[string]string), Base: "/"}
	options.Options["role"] = "leader"
	options.Options["log_dir"] = os.TempDir()
	options.Options["register"] = "localhost:" + testRegisterPort
	return &Config{*options}
}

// newTestRegister creates a new test register.
func newTestRegister() net.Listener {
	listener, err := net.Listen("tcp", ":"+testRegisterPort)
	if nil != err {
		panic(err)
	}
	server := &http.Server{Handler: websocket.Handler(testRegister)}
	go server.Serve(listener)
	return listener
}

// testRegister takes websocket connections.
func testRegister(conn *websocket.Conn) {
}

// TestTails checks that the tails function returns the sizes of all log files.
func TestTails(t *testing.T) {

	config := newTestConfig()
	register := newTestRegister()
	defer register.Close()

	expected := make(map[string]int64, 10)

	for i := 0; i < 10; i++ {
		name := strconv.Itoa(i)
		log, err := OpenLog(config, name, 0)
		if nil != err {
			t.Fatal("Unable to open log file.")
		}
		if err := log.WriteNext(&LogEntry{RequestId: []byte("x")}); nil != err {
			t.Fatal("Unable to write to log file.")
		}
		expected[name] = 40
		log.Close()
		defer os.Remove(log.Name())
	}

	broker, _ := New(&config.Config)
	tails := broker.tails()
	for topic, size := range tails {
		if expected[topic] != size {
			t.Fatalf("Expected %s to have size %d, was %d.", topic, expected[topic], size)
		}
		delete(expected, topic)
	}

	if 0 != len(expected) {
		t.Fatal("Tails did not report all log files.")
	}

}
