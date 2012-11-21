package main

import(
	"code.google.com/p/go.net/websocket"
	"octopi/impl/brokerimpl"
	"octopi/api/messageapi"
	"net/http"
	"fmt"
	"os"
)

var broker *brokerimpl.Broker

func registerBroker(regUrl string, regOrigin string, myHostPort string) *brokerimpl.Broker{
	/* connect to register server */
	regconn, err := websocket.Dial(regUrl, "", regOrigin)
	/* fatal error if connection or messages failed */
	checkError(err)
	
	bri := messageapi.BrokerRegInit{messageapi.BROKER, myHostPort}
	/* send relevant broker information to register */
	err = websocket.JSON.Send(regconn, bri)
	checkError(err)
	
	/* receive register assignments */
	var rbi messageapi.RegBrokerInit
	err = websocket.JSON.Receive(regconn, &rbi)
	checkError(err)
	
	/* create the broker based on JSON from register server */
	b, err := brokerimpl.NewBroker(rbi, regconn)
	checkError(err)
	
	/* close the connection if broker not assigned as leader */
	if b.Role() != messageapi.LEADER{
		regconn.Close()
	}

	return b
}

func ProdHandler(ws *websocket.Conn){
	var pli messageapi.ProdLeadInit
	err := websocket.JSON.Receive(ws, &pli)
	/* close the connection and return if invalid request */
	if nil != err || pli.MessageSrc!=messageapi.PRODUCER{
		ws.Close()
		return
	}
	broker.RegProd(ws, pli)
	//TODO: send catchup if not
	for {
		var pubMsg messageapi.PubMsg
		err := websocket.JSON.Receive(ws, &pubMsg)
		if nil != err{
			broker.RemoveProd(pli)
			ws.Close()
			return
		}
		broker.FollowBroadcast(pubMsg)
		//TODO: send message to consumers
	}
}

func ConsHandler(ws *websocket.Conn){
	var cbi messageapi.ConsBrokerInit
	err := websocket.JSON.Receive(ws, &cbi)
	if nil != err||cbi.MessageSrc!=messageapi.CONSUMER{
		ws.Close()
		return
	}
	//TODO: send catchup if not
	/* failed because topic does not exist */
	err = broker.RegCons(ws, cbi)
	if err!=nil{
		ws.Close()
		return
	}
	
	//TODO: preserve connection through blocking call. use receive?
}

func BrokerHandler(ws *websocket.Conn){
	var fli messageapi.FollowLeadInit
	err := websocket.JSON.Receive(ws, &fli)
	if nil !=err||fli.MessageSrc!=messageapi.BROKER{
		ws.Close()
		return
	}
	
}

func main(){
	//TODO: add command line arguments for register
	broker := registerBroker("", "", "")
	
	if broker.Role()==messageapi.LEADER{
		http.Handle("/prodconn", websocket.Handler(ProdHandler))
		http.Handle("/brokerconn", websocket.Handler(BrokerHandler))
	}
	http.Handle("/consconn", websocket.Handler(ConsHandler))
	http.ListenAndServe(":12345", nil)
}

func checkError(err error){
	if err!=nil{
		fmt.Println("Fatal Error ", err.Error())
		os.Exit(1)
	}
}
