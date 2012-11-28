package brokerimpl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"octopi/api/protocol"
	"os"
	"path"
)

// Log is a file to which brokers append messages. Not thread-safe, so only one
// goroutine should use the log at a time.
type Log struct {
	os.File
}

// LogEntry is an entry in the log file. The file is a sequence of LogEntries.
type LogEntry struct {
	protocol.Message        // enclosed message
	RequestId        []byte // sha256 of producer seqnum; used to prevent dups
}

// Default file permission.
var PERM os.FileMode = 0666

// NewLog creates/opens a log file with a new file pointer.
func OpenLog(config *Config, topic string, offset int64) (*Log, error) {

	name := path.Join(config.LogDir(), topic)
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, PERM)
	if nil != err {
		return nil, err
	}

	if offset < 0 { // from tail
		file.Seek(0, os.SEEK_END)
	} else { // from offset
		_, err = file.Seek(offset, os.SEEK_SET)
	}

	if nil != err {
		return nil, err
	}

	return &Log{*file}, nil

}

// Reads the next entry from the broker log.
func (log *Log) ReadNext() (*LogEntry, error) {

	// in case of error, revert
	checkpoint, _ := log.Seek(0, os.SEEK_CUR)
	bail := func() { log.Seek(checkpoint, os.SEEK_SET) }

	length, err := log.readLength()
	if nil != err {
		bail()
		return nil, err
	}

	entry, err := log.readEntry(length)
	if nil != err {
		bail()
		return nil, err
	}

	entry.ID = checkpoint
	return entry, entry.validate()

}

// Writes the given entry at the end of the broker log.
func (log *Log) WriteNext(entry *LogEntry) error {

	// in case of error, revert
	checkpoint, _ := log.Seek(0, os.SEEK_CUR)
	bail := func() { log.Seek(checkpoint, os.SEEK_SET) }

	if err := log.writeLength(entry); nil != err {
		bail()
		return err
	}

	if _, err := log.writeEntry(entry); nil != err {
		bail()
		return err
	}

	return nil

}

// readEntry reads the next n bytes and decodes them into a log entry.
func (log *Log) readEntry(n uint32) (*LogEntry, error) {

	buf := make([]byte, n)
	total := uint32(0)

	for { // read exactly n bytes, or until error
		read, err := log.Read(buf[total:])
		if nil != err {
			return nil, err
		}
		total += uint32(read)
		if total >= n {
			break
		}
	}

	var entry LogEntry
	return &entry, entry.decode(buf)

}

// writeEntry writes the entry to the file and returns the number of bytes
// written and an error, if any.
func (log *Log) writeEntry(entry *LogEntry) (int, error) {
	buffer, err := entry.encode()
	if nil != err {
		return 0, err
	}
	return log.Write(buffer)
}

// readLength reads the length of the next entry.
func (log *Log) readLength() (uint32, error) {

	var length uint32
	if err := binary.Read(log, binary.LittleEndian, &length); nil != err {
		return 0, err
	}

	return length, nil

}

// writeLength writes the length of the entry to the broker log.
func (log *Log) writeLength(entry *LogEntry) error {
	length := entry.length()
	return binary.Write(log, binary.LittleEndian, length)
}

// decode decodes the given byte buffer into a log entry.
func (entry *LogEntry) decode(buffer []byte) error {

	reader := bytes.NewReader(buffer)

	// read checksum
	err := binary.Read(reader, binary.LittleEndian, &entry.Checksum)
	if nil != err {
		return err
	}

	// read request ID
	err = binary.Read(reader, binary.LittleEndian, &entry.RequestId)
	if nil != err {
		return err
	}

	// read payload
	entry.Payload = buffer[36:]
	return nil

}

// encode encodes the given entry into a byte array.
func (entry *LogEntry) encode() ([]byte, error) {

	n := entry.length()
	writer := new(bytes.Buffer)

	// write checksum
	err := binary.Write(writer, binary.LittleEndian, entry.Checksum)
	if nil != err {
		return nil, err
	}

	// write request ID
	err = binary.Write(writer, binary.LittleEndian, entry.RequestId)
	if nil != err {
		return nil, err
	}

	// write payload
	_, err = writer.Write(entry.Payload)
	return writer.Bytes()[0:n], err

}

// validate returns nil if the entry's checksum is valid.
func (entry *LogEntry) validate() error {
	expected := crc32.ChecksumIEEE(entry.Payload)
	if entry.Checksum == expected {
		return nil
	}
	return errors.New(fmt.Sprintf("Invalid checksum. Expected %d, was %d.", expected, entry.Checksum))
}

// length returns the length of the entry's encoding in the log file.
func (entry *LogEntry) length() uint32 {
	return uint32(36 + len(entry.Payload))
}
