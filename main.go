package main

import (
	"net/url"
	"log"
	"github.com/gorilla/websocket"
	"github.com/fpawel/betfairs/football"
	"encoding/json"


)

const dbConnStr = `
user=postgres 
password='falena190312' 
dbname=betfairs 
sslmode=disable
host=localhost
port=5432 `


func main() {

	app := newApp()
	defer app.Close()


	u := url.URL{Scheme: "wss", Host: "betfairs.herokuapp.com", Path: "/football/live"}
	log.Println("connecting...")
	mainLoop:
	for {
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		log.Println("connected!")

		for{
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				c.Close()
				log.Println("reconnecting...")
				continue mainLoop
			}
			var game football.GameLive

			if err := json.Unmarshal(message, &game); err != nil {
				log.Println("ERROR format:", err )
				continue
			}
			app.processGameLive(game)
		}
	}
}






