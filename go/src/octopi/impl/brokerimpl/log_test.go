package brokerimpl

import (
	"hash/crc32"
	"octopi/api/protocol"
	"octopi/util/config"
	"os"
	"testing"
)

// TestReadWrite triess reading ten entries from a log file.
func TestReadWrite(t *testing.T) {

	options := &config.Config{make(map[string]string)}
	options.Options["log_dir"] = os.TempDir()
	config := &Config{*options}
	log, err := OpenLog(config, "temp", 0)

	if nil != err {
		t.Fatal("Unable to open log file.")
	}

	var i byte
	for i = 1; i <= 10; i++ {
		payload := []byte{i}
		message := protocol.Message{0, payload, crc32.ChecksumIEEE(payload)}
		entry := &LogEntry{message, make([]byte, 32)}
		if nil != log.WriteNext(entry) {
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