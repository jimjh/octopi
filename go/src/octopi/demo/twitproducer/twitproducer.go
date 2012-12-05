package twitproducer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"octopi/demo/twitproto"
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
func (tp *TwitProducer) RelayMessages(numMsgs int32) (int32, error) {

	response, err := tp.client.Do(tp.request)

	if nil != err {
		return 0, err
	}

	var i int32 = 0
	for i = 0; i < numMsgs; i++ {
		// reads in a whole json message at a time
		buffer := make([]byte, BUFSIZ)

		nbytes, err := response.Body.Read(buffer)

		// return if we get a read error. could be connection cut off
		if nil != err {
			return i, err
		}

		// truncate the buffer for JSON decoding
		buffer = buffer[:nbytes]
		t := &twitproto.TweetSrc{}

		// unmarshal to retain fields we need and to remove nulls
		err = json.Unmarshal(buffer, t)

		// json error. decrement count and continue to try again
		if nil != err {
			fmt.Printf("Error: %s\n", err.Error())
			i--
			continue
		}

		tweetToSend := getTweet(t)

		// TODO: what do we send?
		//marshalTweet, _ := json.Marshal(tweetToSend)

		//err = tp.Producer.Send(TOPIC, marshalTweet)

		if len(tweetToSend.Text) == 0 {
			i--
			continue
		}
		err = tp.producer.Send(TOPIC, []byte(tweetToSend.Text))

		if nil != err {
			return i, err
		}
	}

	return i, nil
}

// getTweet converts from TweetSrc to actual Tweet by removing use of string pointers
func getTweet(t *twitproto.TweetSrc) twitproto.Tweet {
	tweet := twitproto.Tweet{}

	// tedious check for nils because Go will not allow nil strings
	if t.Text != nil {
		tweet.Text = *t.Text
	}
	if t.Geo != nil {
		tweet.Geo = *t.Geo
	}
	if t.In_reply_to_screen_name != nil {
		tweet.In_reply_to_screen_name = *t.In_reply_to_screen_name
	}
	if t.Source != nil {
		tweet.Source = *t.Source
	}
	if t.Contributors != nil {
		tweet.Contributors = *t.Contributors
	}
	if t.In_reply_to_status_id != nil {
		tweet.In_reply_to_status_id = *t.In_reply_to_status_id
	}
	if t.In_reply_to_user_id != nil {
		tweet.In_reply_to_user_id = *t.In_reply_to_user_id
	}
	if t.Created_at != nil {
		tweet.Created_at = *t.Created_at
	}

	return tweet
}
