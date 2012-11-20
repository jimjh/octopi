package main

import(
	"code.google.com/p/go.net/websocket"
	"octopi/impl/brokerimpl"
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
	
	/* send relevant broker information to register */
	err = websocket.Message.Send(regconn, myHostPort)
	checkError(err)
	
	/* receive register assignments */
	var rba brokerimpl.RegBrokerAssign
	err = websocket.JSON.Receive(regconn, &rba)
	checkError(err)
	
	/* create the broker based on JSON from register server */
	b := brokerimpl.NewBroker(rba, regconn)
	
	/* close the connection if broker not assigned as leader */
	if b.Role() != brokerimpl.LEADER{
		regconn.Close()
	}

	//TODO: continue here!
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
	
	if broker.Role()==brokerimpl.LEADER{
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
