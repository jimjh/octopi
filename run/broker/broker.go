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
	
}

func ConsHandler(ws *websocket.Conn){
	
}

func BrokerHandler(ws *websocket.Conn){
	
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
