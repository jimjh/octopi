package brokerlog

// XXX: add header comment
// XXX: please run GoFormat
// XXX: why in util? I would prefer it to be in impl, then rename BLog* to just Log*.
// XXX: util is more for general utilities that can be reused in other projects.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"octopi/api/protocol"
	"os"
	"strconv"
	"sync"
)

type BLogEntry struct{
	ID int64
	Length    int    // XXX: should be uint32 to fix value at 4 bytes
	Checksum  uint32 // crc32 checksum of payload
	RequestId []byte // sha256 of producer addr + seq num; used to prevent dups
	Payload   []byte // contents
}

type BLog struct {
	logFile *os.File // file
	hwMark  int64    // end of the file log commit. i.e. message ID of the to-be-written-to persistent storage message.
	tail    int64    // messageID of the last written message
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
	blog.tail = -1 // since have not written yet

	return blog, err

}

func (blog *BLog) NextMsgId(id int64)(int64, error){
	blog.lock.Lock()
	defer blog.lock.Unlock()
	msgLenBytes := make([]byte, 4)
	bytesRead , err := blog.logFile.ReadAt(msgLenBytes, id)
	if bytesRead != 4 || nil!=err{
		return -1, errors.New("Next message search failed")
	}
	msgLen64, _ := binary.Varint(msgLenBytes)
	if msgLen64<=0{
		return -1, errors.New("Log format corrupted")
	}
	return id+msgLen64, nil
}

// Read reads from offset ID, useful for recovery
func (blog *BLog) Read(id int64) ([]byte, error) {
	blog.lock.Lock()
	defer blog.lock.Unlock()

	// XXX: should be fixed at uint32
	var msgLen int
	msgLenBytes := make([]byte, 4) //4 bytes for message length
	bytesRead, err := blog.logFile.ReadAt(msgLenBytes, id) //offset for ID
	if bytesRead != 4 || nil!=err{
		return nil, errors.New("Did not read in correctly")
	}

	// XXX: use binary.Write and binary.Read instead?
	msgLen64, _ := binary.Varint(msgLenBytes)
	msgLen = int(msgLen64)

	if msgLen<=0{
		return nil, errors.New("Log format corrupted")
	}
	read := make([]byte, msgLen)
	bytesRead, err = blog.logFile.ReadAt(read, id)
	if bytesRead != msgLen{
		return nil, err
	}
	if bytesRead != msgLen {
		return nil, errors.New("Did not read in correctly")
	}

	return read, nil

}

/* reads an actual entry from the broker log */
func (blog *BLog) ReadEntry(id int64) (*BLogEntry, error) {
	b, err := blog.Read(id)
	if nil != err {
		return nil, err
	}

	var msgLen int
	msgLen64, _ := binary.Varint(b[:4])
	msgLen = int(msgLen64)
	if nil != err {
		return nil, err
	}

	var cksm uint32
	cksmBuf := bytes.NewBuffer(b[4:8])
	err = binary.Read(cksmBuf, binary.LittleEndian, &cksm)

	hash := b[8:40]

	payload := b[40:]

	ble := &BLogEntry{
		ID: id,
		Length: msgLen,
		Checksum: cksm,
		RequestId: hash,
		Payload:   payload,
	}

	return ble, nil
}

/* get the latest offset of the file. i.e. the latest messageID */
func (blog *BLog) LatestOffset() (int64, error) {
	blog.lock.Lock()
	defer blog.lock.Unlock()
	return blog.logFile.Seek(0, os.SEEK_END)
}

/* write raw bytes to the end of the file. assumes correct formatting. for use in recovery */
func (blog *BLog) WriteBytes(b []byte) (int, error) {
	blog.lock.Lock()
	defer blog.lock.Unlock()
	messageID, err := blog.logFile.Seek(0, os.SEEK_END)
	if nil != err {
		return 0, err
	}
	blog.tail = messageID

	return blog.logFile.Write(b)
}

/* writes formatted messages. for general usage */
func (blog *BLog) Write(hostport string, msg protocol.Message) (int, error) {

	/* construct int64 ID of message */
	messageID, _ := blog.logFile.Seek(0, os.SEEK_END)

	/* construct length field */
	var writeLength int
	writeLength = 4 + 4 + 32 + len(msg.Payload)
	lenbuf := make([]byte, 4)
	binary.PutVarint(lenbuf, int64(writeLength))

	cksmbuf := new(bytes.Buffer)
	binary.Write(cksmbuf, binary.LittleEndian, msg.Checksum)

	/* construct request_id field */
	// TODO: use uint32 later
	req_id := int(msg.ID)
	reqstr := strconv.Itoa(req_id)
	hashstr := hostport + ":" + reqstr
	sha256hash := sha256.New()
	sha256hash.Write([]byte(hashstr))

	/* constructs the total bytes array for consecutive write */
	toWrite := make([]byte, 0, writeLength)
	toWrite = append(toWrite, lenbuf...)
	toWrite = append(toWrite, cksmbuf.Bytes()...)
	toWrite = append(toWrite, sha256hash.Sum(nil)...)
	toWrite = append(toWrite, msg.Payload...)

	blog.lock.Lock()
	defer blog.lock.Unlock()

	blog.tail = messageID

	return blog.logFile.Write(toWrite)
}

func (blog *BLog) Tail() int64 {
	return blog.tail
}

/* commits to permanent storage */
func (blog *BLog) Commit() error {
	blog.lock.Lock()
	defer blog.lock.Unlock()
	err := blog.logFile.Sync()
	// XXX: not sure abt this.
	if nil != err {
		return err
	}
	blog.hwMark, err = blog.logFile.Seek(0, os.SEEK_CUR)
	return err
}

/* closes file */
func (blog *BLog) Close() error {
	blog.lock.Lock()
	defer blog.lock.Unlock()
	return blog.logFile.Close()
}
