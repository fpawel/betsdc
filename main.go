package main

import (
	"net/url"
	"log"
	"github.com/gorilla/websocket"
)

func main() {


	u := url.URL{Scheme: "wss", Host: "betfairs.herokuapp.com", Path: "/football/prices"}
	//u := url.URL{Scheme: "ws", Host: "localhost:8080", Path: "/football/prices"}

	log.Println("connecting...")

	reconnect:
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	log.Println("connected!")
	defer c.Close()

	for{
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			log.Println("reconnecting...")
			goto reconnect
		}
		log.Printf("recv: %d", len(message))
	}
}
