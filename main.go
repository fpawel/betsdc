package main

import (
	"net/url"
	"log"
	"github.com/gorilla/websocket"
	"github.com/fpawel/betfairs/football"
	"encoding/json"

	"time"
	"fmt"
)

func main() {

	app := newApp()
	defer app.Close()


	u := url.URL{Scheme: "wss", Host: "betfairs.herokuapp.com", Path: "/football/live"}
	log.Println("connecting...")
	dialer := &*websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Minute

	//activeEventsUpdatedAt := time.Now()

	mainLoop:
	for {

		c, _, err := dialer.Dial(u.String(), nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		log.Println("connected!")

		for{
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("read: %q: %v\n", string(message), err)
				c.Close()
				log.Println("reconnecting...")
				continue mainLoop
			}
			var games []football.GameLive
			if err := json.Unmarshal(message, &games); err != nil {
				log.Println("ERROR format:", err )
				continue
			}
			for _, game := range games{
				markets := app.db.GetMarkets(game.ID, game.OpenDate)
				if len(markets) == 0{
					if err := app.db.AddGameEvent(game); err != nil {
						fmt.Println("ERROR adding game:", game, err)
						continue
					}
					markets = app.db.GetMarkets(game.ID, game.OpenDate)
					if len(markets) == 0{
						log.Fatal(game, "games not added")
					}
				}
				app.addGamePrices(game, markets)
			}
			//if time.Since(activeEventsUpdatedAt) > 5 * time.Minute{
				app.updateActiveEventsStatus(games)
				//activeEventsUpdatedAt = time.Now()
			//}
		}
	}
}








