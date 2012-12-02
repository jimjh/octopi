package main

import (
	"code.google.com/p/go.net/websocket"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"octopi/api/protocol"
	"os"
	"strconv"
)

func main() {

	flag.Parse()
	// parse the register hostport
	regHostPort := flag.String("reg", "localhost:12345", "specifies the HostPort of the register")
	// parse my ID
	myId := flag.String("id", "defaultID", "specifies the ID of the producer")
	// parse the amount of messages to send
	numMsgs := flag.Int("cnt", 100, "specifies the amount of messages to send")
	// register URL
	regURL := "ws://" + *regHostPort + "/" + protocol.REDIRECTOR

	regConn, err := websocket.Dial(regURL, "", *regHostPort)
	checkError(err)

	// receive redirect from register
	var redirect protocol.Ack
	err = websocket.JSON.Receive(regConn, &redirect)
	checkError(err)

	// we were expecting a redirect
	if redirect.Status != protocol.StatusRedirect {
		os.Exit(1)
	}

	leaderHostPort := string(redirect.Payload)
	fmt.Println("Leader HostPort: ", leaderHostPort)
	publishURL := "ws://" + leaderHostPort + "/" + protocol.PUBLISH

	leadConn, err := websocket.Dial(publishURL, "", leaderHostPort)
	checkError(err)

	err = sendMessages(leadConn, *myId, *numMsgs)
	checkError(err)

}

func sendMessages(conn *websocket.Conn, id string, msgCnt int) error {

	for i := 0; i < msgCnt; i++ {
		seqmsg := []byte(strconv.Itoa(i))
		msgToSend := protocol.Message{int64(i), seqmsg, crc32.ChecksumIEEE(seqmsg)}
		req := protocol.ProduceRequest{id, "seqTopic", msgToSend}
		err := websocket.JSON.Send(conn, req)

		if err != nil {
			return err
		}

		var ack protocol.Ack
		err = websocket.JSON.Receive(conn, &ack)

		if err != nil {
			return err
		}

		if ack.Status != protocol.StatusSuccess {
			return errors.New("Incorrect Acknowledgement!")
		}
	}

	return nil
}

func checkError(err error) {
	if nil != err {
		fmt.Println(err)
		os.Exit(1)
	}
}
