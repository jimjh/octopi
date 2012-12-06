package twitproducer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"octopi/impl/producer"
)

const (
	STREAM_URL = "https://stream.twitter.com/1.1/statuses/sample.json"
	METHOD     = "GET"
	TOPIC      = "tweet" // temporary topic that all the tweets go under
)

const (
	BUFSIZ = 5000 // large buffer to make sure contain message
)

type TwitProducer struct {
	producer *producer.Producer
	client   *http.Client
	request  *http.Request
}

// New TwitProducer creates a new producer for a twitter account as supplied by the user
func NewTwitProducer(username string, password string, hostport string, id *string) (*TwitProducer, error) {

	client := &http.Client{}

	request, err := http.NewRequest(METHOD, STREAM_URL, nil)

	if nil != err {
		return nil, err
	}

	// set authentication for user
	request.SetBasicAuth(username, password)

	return &TwitProducer{producer.New(hostport, id), client, request}, nil
}

// RelayMessages relays the requested amount of messages from the producer
// to the lead broker and returns the amount of messages sent
func (tp *TwitProducer) RelayMessages() error {

	response, err := tp.client.Do(tp.request)

	if nil != err {
		return err
	}

	dec := json.NewDecoder(response.Body)
	for {

		var v map[string]interface{}
		if err := dec.Decode(&v); nil != err {
			fmt.Printf("Error reading from stream: %s\n", err.Error())
			continue
		}

		if nil == v["text"] {
			continue
		}

		tweet := v["text"].(string)

		if len(tweet) == 0 {
			continue
		}

		if err := tp.producer.Send(TOPIC, []byte(tweet)); nil != err {
			fmt.Printf("Connection lost.\n")
			return err
		}

	}

	return nil

}
