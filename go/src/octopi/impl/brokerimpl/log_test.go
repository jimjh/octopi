package brokerimpl

import (
	"hash/crc32"
	"io"
	"octopi/api/protocol"
	"octopi/util/test"
	"os"
	"testing"
)

// TestReadWrite tries reading ten entries from a log file.
func TestReadWrite(tester *testing.T) {

	config := newTestConfig()
	t := test.New(tester)

	log, err := OpenLog(config, "temp", 0)
	t.AssertNil(err, "OpenLog")

	defer os.Remove(log.Name())

	var i byte
	for i = 1; i <= 10; i++ {
		payload := []byte{i}
		message := &protocol.Message{int64(i), payload, crc32.ChecksumIEEE(payload)}
		_, err := log.Append("x", message)
		t.AssertNil(err, "log.Append")
	}

	log.Close()

	log, err = OpenLog(config, "temp", 0)
	t.AssertNil(err, "OpenLog")

	for i = 1; i <= 10; i++ {
		entry, err := log.ReadNext()
		t.AssertNil(err, "log.ReadNext()")
		t.AssertEqual(new(test.IntMatcher), int(i), int(entry.Payload[0]))
	}

}

// TestDuplicateDetection ensures that duplicate requests from the same
// producer are not written to the log.
func TestDuplicateDetection(tester *testing.T) {

	config := newTestConfig()
	t := test.New(tester)

	log, err := OpenLog(config, "temp", 0)
	t.AssertNil(err, "OpenLog")

	defer os.Remove(log.Name())

	var i byte
	for i = 1; i <= 10; i++ {
		payload := []byte{i}
		message := &protocol.Message{1, payload, crc32.ChecksumIEEE(payload)}
		_, err := log.Append("x", message)
		t.AssertNil(err, "log.Append")
	}

	log.Close()

	log, err = OpenLog(config, "temp", 0)
	t.AssertNil(err, "OpenLog")

	entry, err := log.ReadNext()
	t.AssertEqual(new(test.IntMatcher), 1, int(entry.Payload[0]))

	_, err = log.ReadNext()
	if err != io.EOF {
		t.Error("Error should be EOF.")
	}

}

// TODO: test open from end
// TODO: test open from offset
// TODO: test truncateLog
// TODO: test is eof
