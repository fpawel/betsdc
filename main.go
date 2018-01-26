package main

import (
	"net/url"
	"log"
	"github.com/gorilla/websocket"
	"github.com/fpawel/betfairs/football"
	"encoding/json"

	"time"
)

func main() {

	app := newApp()
	defer app.Close()


	u := url.URL{Scheme: "wss", Host: "betfairs.herokuapp.com", Path: "/football/live"}
	log.Println("connecting...")
	dialer := &*websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Minute
	mainLoop:
	for {

		c, _, err := dialer.Dial(u.String(), nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		log.Println("connected!")

		for{
			app.processGamesWithUnknownStatus()
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", message, string(message), ":", err)
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






