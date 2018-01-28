package main

import (
	"fmt"
	"github.com/fpawel/betfairs/aping"
	"github.com/fpawel/betfairs/football"
	"os"

	_ "github.com/lib/pq"
)

type App struct {
	db           BetfairsDB
	apingSession *aping.Session
}

func (x App) Close() error {
	return x.db.Close()
}

func newApp() (x App) {
	return App{
		apingSession : aping.NewSession(os.Getenv("BETFAIR_LOGIN_USER"), os.Getenv("BETFAIR_LOGIN_PASS")),
		db:OpenBetfairsDB(),
	}
}

func (x *App) addGamePrices( game football.GameLive, marketsIDs []int)  {
	gameID := GameID{game.ID, game.OpenDate}

	for _,ids := range get40marketsIDs(marketsIDs) {
		markets, err := x.apingSession.ListMarketBook(ids)
		if err != nil {
			fmt.Println("ERROR reading prices:", game, ids, err)
		}
		for _, market := range markets {
			for _, r := range market.Runners {
				if runnerHasPrices(r) {
					x.db.AddRunnerPrices(game,market,r)
				} else {
					x.db.UpdateRunnerStatus(gameID, market.ID.Int(), r.ID, r.Status)
				}
			}
		}
	}
}

func (x App) updateActiveEventsStatus(excludeGames []football.GameLive ) {
	activeMarkets := x.db.ActiveMarkets()
	for _,game := range excludeGames {
		delete(activeMarkets, GameID{game.ID, game.OpenDate})
	}

	for game, markets1 := range activeMarkets {
		for _,markets := range get40marketsIDs(markets1){
			mbs, err := x.apingSession.ListMarketBook(markets)
			if err != nil {
				fmt.Println("ERROR reading prices:", game, markets, err)
				return
			}
			for _, m := range mbs {
				for _, r := range m.Runners {
					x.db.UpdateRunnerStatus(game, m.ID.Int(), r.ID, r.Status)
				}
			}
		}

	}
}

func runnerHasPrices(r aping.Runner) bool {
	ex := r.ExchangePrices
	return len(ex.AvailableToBack) == 3 && len(ex.AvailableToLay) == 3
}

func intToMarketID(x int) aping.MarketID{
	return aping.MarketID(fmt.Sprintf("1.%d", x))
}

func get40marketsIDs(markets []int) (r [][]aping.MarketID){
	marketsCount := len(markets)
	n := 40
	for i := 0; i<marketsCount; i = n {
		n = i + 40
		if n > marketsCount {
			n = marketsCount
		}
		var xs []aping.MarketID
		for _,id := range markets[i:n]{
			xs = append(xs, intToMarketID(id))
		}
		r = append(r, xs)
	}
	return r
}