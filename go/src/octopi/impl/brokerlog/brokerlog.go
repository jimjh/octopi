package brokerlog

// XXX: add header comment

import (
	"fmt"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"octopi/api/protocol"
	"os"
	"sync"
)

type BLogEntry struct {
	ID        int64
	Length    uint32 
	Checksum  uint32 // crc32 checksum of payload
	RequestId []byte // sha256 of producer addr + seq num; used to prevent dups
	Payload   []byte // contents
}

type BLog struct {
	logFile *os.File // file
	hwMark  int64    // end of the file log commit
	lock    sync.Mutex
}

// OpenLog opens the log file at the given path and returns a BLog struct.
func OpenLog(path string) (*BLog, error) {

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)

	if nil != err {
		return nil, err
	}

	blog := &BLog{logFile: file}

	blog.hwMark, err = blog.logFile.Seek(0, os.SEEK_END)

	return blog, err

}

// Read reads from offset ID, useful for recovery
func (blog *BLog) Read(id int64) ([]byte, int64, error) {
	blog.lock.Lock()
	defer blog.lock.Unlock()

	msgLenBytes := make([]byte, 4)                         //4 bytes for message length
	bytesRead, err := blog.logFile.ReadAt(msgLenBytes, id) //offset for ID
	if bytesRead != 4 || nil != err {
		return nil, -1, errors.New("Did not read in correctly")
	}

	var msgLen uint32
	msgLenBuf := bytes.NewBuffer(msgLenBytes)
	err = binary.Read(msgLenBuf, binary.LittleEndian, &msgLen)

	if err!=nil {
		return nil, -1, err
	}

	read := make([]byte, msgLen)
	bytesRead, err = blog.logFile.ReadAt(read, id)
	
	if bytesRead != int(msgLen) {
		return nil, -1, errors.New("Did not read in correctly")
	}

	return read, id+int64(msgLen), nil

}

// reads an actual entry from the broker log 
func (blog *BLog) ReadEntry(id int64) (*BLogEntry, int64, error) {
	b, next, err := blog.Read(id)
	if nil != err {
		return nil, -1, err
	}

	var msgLen uint32
	msgLenBuf := bytes.NewBuffer(b[:4])
	err = binary.Read(msgLenBuf, binary.LittleEndian, &msgLen)

	if nil != err {
		return nil, -1, err
	}

	var cksm uint32
	cksmBuf := bytes.NewBuffer(b[4:8])
	err = binary.Read(cksmBuf, binary.LittleEndian, &cksm)

	if nil != err {
		return nil, -1, err
	}

	hash := b[8:40]

	payload := b[40:]

	ble := &BLogEntry{
		ID:        id,
		Length:    msgLen,
		Checksum:  cksm,
		RequestId: hash,
		Payload:   payload,
	}

	return ble, next, nil
}

// get the latest offset of the file. i.e. the latest messageID
func (blog *BLog) LatestOffset() (int64, error) {
	blog.lock.Lock()
	defer blog.lock.Unlock()
	return blog.logFile.Seek(0, os.SEEK_END)
}

/* write raw bytes to the end of the file. assumes correct formatting. for use in recovery */
func (blog *BLog) WriteBytes(b []byte) (int, error) {
	blog.lock.Lock()
	defer blog.lock.Unlock()
	_, err := blog.logFile.Seek(0, os.SEEK_END)
	if nil != err {
		return 0, err
	}

	return blog.logFile.Write(b)
}

/* writes formatted messages. for general usage */
func (blog *BLog) Write(hostport string, msg protocol.Message) (int, error) {

	/* construct int64 ID of message */
	_, err := blog.logFile.Seek(0, os.SEEK_END)

	if nil != err{
		return -1, err
	}

	/* construct length field */
	var writeLength uint32
	writeLength = 4 + 4 + 32 + uint32(len(msg.Payload))
	lenBuf := new(bytes.Buffer)
	err = binary.Write(lenBuf, binary.LittleEndian, writeLength)
	
	if nil != err{
		return -1, err
	}

	cksmbuf := new(bytes.Buffer)
	binary.Write(cksmbuf, binary.LittleEndian, msg.Checksum)

	/* construct request_id field */
	reqstr := fmt.Sprintf("%d", msg.ID)
	hashstr := hostport + ":" + reqstr
	sha256hash := sha256.New()
	sha256hash.Write([]byte(hashstr))

	/* constructs the total bytes array for consecutive write */
	toWrite := make([]byte, 0, writeLength)
	toWrite = append(toWrite, lenBuf.Bytes()...)
	toWrite = append(toWrite, cksmbuf.Bytes()...)
	toWrite = append(toWrite, sha256hash.Sum(nil)...)
	toWrite = append(toWrite, msg.Payload...)

	blog.lock.Lock()
	defer blog.lock.Unlock()

	return blog.logFile.Write(toWrite)
}

func (blog *BLog) Commit(hwMark int64){
	blog.lock.Lock()
	defer blog.lock.Unlock()
	blog.hwMark = hwMark
}

/* flushes to permanent storage */
func (blog *BLog) Flush() error {
	blog.lock.Lock()
	defer blog.lock.Unlock()
	err := blog.logFile.Sync()
	return err
}

func (blog *BLog) HighWaterMark() int64{
	blog.lock.Lock()
	defer blog.lock.Unlock()
	return blog.hwMark
}

/* closes file */
func (blog *BLog) Close() error {
	blog.lock.Lock()
	defer blog.lock.Unlock()
	return blog.logFile.Close()
}
