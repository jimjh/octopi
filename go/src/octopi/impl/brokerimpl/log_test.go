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

	log.Close()

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

	log.Close()

}

// TestOpenFromEnd ensures that we can open a log file at its tail.
func TestOpenFromEnd(tester *testing.T) {

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

	log, err = OpenLog(config, "temp", -1)
	t.AssertNil(err, "OpenLog")

	_, err = log.ReadNext()
	if err != io.EOF {
		t.Error("Error should be EOF.")
	}

	t.AssertTrue(log.IsEOF(), "log.IsEOF")
	log.Close()

}

// TestOpenFromOffset ensures that we can open a log file from a given offset
func TestOpenFromOffset(tester *testing.T) {

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

	log, err = OpenLog(config, "temp", 41)
	t.AssertNil(err, "OpenLog")

	for i = 2; i <= 10; i++ {
		entry, err := log.ReadNext()
		t.AssertNil(err, "log.ReadNext()")
		t.AssertEqual(new(test.IntMatcher), int(i), int(entry.Payload[0]))
	}

	t.AssertTrue(log.IsEOF(), "log.IsEOF")
	log.Close()

}

// TestTruncateLog ensures that truncateLog cuts the log at the given point.
func TestTruncateLog(tester *testing.T) {

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
	truncateLog(config, "temp", 5*41)

	log, err = OpenLog(config, "temp", 0)
	t.AssertNil(err, "OpenLog")

	for i = 1; i <= 5; i++ {
		entry, err := log.ReadNext()
		t.AssertNil(err, "log.ReadNext()")
		t.AssertEqual(new(test.IntMatcher), int(i), int(entry.Payload[0]))
	}

	t.AssertTrue(log.IsEOF(), "log.IsEOF")
	log.Close()

}

// TestOpenError ensures that OpenLog returns an error if the file cannot be
// opened.
func TestOpenError(tester *testing.T) {

	config := newTestConfig()
	config.Base = "/some_non_existing_dir_on_your_machine"

	t := test.New(tester)

	_, err := OpenLog(config, "temp", 0)
	t.AssertNotNil(err, "OpenLog")

}
