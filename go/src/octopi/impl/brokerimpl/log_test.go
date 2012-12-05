package brokerimpl

import (
	"hash/crc32"
	"octopi/api/protocol"
	"os"
	"testing"
)

// TestReadWrite tries reading ten entries from a log file.
func TestReadWrite(t *testing.T) {

	config := newTestConfig()
	log, err := OpenLog(config, "temp", 0)

	if nil != err {
		t.Fatal("Unable to open log file.")
	}

	defer os.Remove(log.Name())

	var i byte
	for i = 1; i <= 10; i++ {
		payload := []byte{i}
		message := &protocol.Message{int64(i), payload, crc32.ChecksumIEEE(payload)}
		if _, err := log.Append("x", message); nil != err {
			t.Errorf("Error: %s", err.Error())
		}
	}

	log.Close()

	log, err = OpenLog(config, "temp", 0)
	if nil != err {
		t.Fatal("Unable to open log file.")
	}

	for i = 1; i <= 10; i++ {
		entry, err := log.ReadNext()
		if nil != err {
			t.Errorf("Error: %s", err.Error())
			t.Fatal("Unable to read log file.")
		}
		if i != entry.Payload[0] {
			t.Errorf("Expected %d, was %d.", i, entry.Payload[0])
		}
	}

}
