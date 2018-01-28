package main

import (
	"log"
	"github.com/jmoiron/sqlx"
	"github.com/fpawel/betfairs/football"
	"github.com/fpawel/betfairs/aping"
	"fmt"
	"io/ioutil"
	"github.com/fpawel/betfairs/event2"
	"strings"
	"net/http"
	"encoding/json"
	"time"
	"os"
)

type GameID struct {
	EventID int `db:"event_id"`
	OpenDate time.Time `db:"open_date"`
}

type GameMarketID struct {
	GameID
	MarketID int `db:"market_id"`
}

type GameRunnerID struct {
	GameMarketID
	SelectionID aping.RunnerID `db:"selection_id"`
}

type BetfairsDB struct {
	*sqlx.DB
}

func OpenBetfairsDB() BetfairsDB {
	connStr := os.Getenv("BETFAIRS_DB_CONN_STR")
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		log.Fatalln(err)
	}
	return BetfairsDB{db}

}


func (x BetfairsDB) AddRunnerPrices(game football.GameLive, m aping.MarketBook, r aping.Runner) {
	if !runnerHasPrices(r) {
		log.Fatal("wrong runner prices")
	}
	ex := r.ExchangePrices
	b := ex.AvailableToBack
	l := ex.AvailableToLay



	_, err := x.NamedExec(
		`
SELECT add_runner_prices
( 	
	:event_id, :open_date, :market_id, :selection_id,
	:status,
	:game_minute, :score_home, :score_away,
	:price_back0, :price_back1, :price_back2,
	:size_back0, :size_back1, :size_back2,
	:price_lay0, :price_lay1, :price_lay2,
	:size_lay0, :size_lay1, :size_lay2,
	:total_matched, :total_available, :last_price_traded	
) `,
		map[string]interface{}{
			"event_id":     game.ID,
			"open_date":    game.OpenDate,
			"market_id":    m.ID.Int(),
			"selection_id": r.ID,
			"status": r.Status,
			"game_minute":  game.Minute,
			"score_home":   game.ScoreHome,
			"score_away":   game.ScoreAway,
			"price_back0":  b[0].Price,
			"price_back1":  b[1].Price,
			"price_back2":  b[2].Price,
			"price_lay0":   l[0].Price,
			"price_lay1":   l[1].Price,
			"price_lay2":   l[2].Price,
			"size_back0":   b[0].Size,
			"size_back1":   b[1].Size,
			"size_back2":   b[2].Size,
			"size_lay0":    l[0].Size,
			"size_lay1":    l[1].Size,
			"size_lay2":    l[2].Size,
			"total_matched": m.TotalMatched,
			"total_available": m.TotalAvailable,
			"last_price_traded": r.LastPriceTraded,


		})
	if err != nil {
		log.Fatal("dbAddRunnerPrices:", game, r, err)
	}
}

func (x BetfairsDB) GetMarkets(gameID int, openDate time.Time ) (marketsIDs []int) {
	err := x.Select(&marketsIDs, "SELECT * FROM get_markets_ids_by_event_id($1, $2)", gameID, openDate)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func (x BetfairsDB) AddGameEvent(game football.GameLive) error {
	r, err := http.Get(fmt.Sprintf("https://betfairs.herokuapp.com/event/%d", game.ID))
	if err != nil {
		return  err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return  err
	}
	var gameEvent event2.Event
	if err := json.Unmarshal(b, &gameEvent); err != nil {
		log.Fatal(err)
	}
	_, err = x.NamedExec(
		`SELECT add_event( :eventID, :openDate, :competitionID, :competitionName, :home, :away, :countryCode);`,
		map[string]interface{}{
			"eventID":         gameEvent.ID,
			"openDate":        gameEvent.OpenDate,
			"competitionID":   gameEvent.CompetitionID,
			"competitionName": gameEvent.CompetitionName,
			"home":            gameEvent.Home,
			"away":            gameEvent.Away,
			"countryCode":     gameEvent.CountryCode,
		})
	if err != nil {
		log.Fatal(game, err)
	}
	for _, market := range gameEvent.Markets {
		if strings.ToLower(market.Name) == "азиатский гандикап" {
			continue
		}
		_, err = x.NamedExec(
			`SELECT add_market( :eventID, :openDate, :marketID, :marketName);`,
			map[string]interface{}{
				"eventID":    gameEvent.ID,
				"openDate":   gameEvent.OpenDate,
				"marketID":   market.ID,
				"marketName": market.Name,
			})
		if err != nil {
			log.Fatal(game, market, err)
		}
		for _, runner := range market.Runners {
			_, err = x.NamedExec(
				`SELECT add_runner( :eventID, :openDate, :marketID, :runnerID, :runnerName);`,
				map[string]interface{}{
					"eventID":    gameEvent.ID,
					"openDate":   gameEvent.OpenDate,
					"marketID":   market.ID,
					"runnerID":   runner.ID,
					"runnerName": runner.Name,
				})
			if err != nil {
				log.Fatal(game, market, runner, err)
			}

		}

	}
	return nil
}

//func (x BetfairsDB) RunnerStatus(runnerID GameRunnerID) string {
//
//	var xs []string
//	if err := x.Select(&xs, `
//	SELECT status
//		FROM runners
//		WHERE
//			event_id = $1 AND
//			open_date = $2 AND
//			market_id = $3 AND
//			selection_id = $4
//		LIMIT 1`, runnerID.EventID, runnerID.OpenDate, runnerID.MarketID, runnerID.SelectionID); err != nil  {
//		log.Fatal(err)
//	}
//
//	if len(xs) == 0 {
//		log.Fatal("runner not found", runnerID)
//	}
//
//	return xs[0]
//}

func (x BetfairsDB) ActiveMarkets()(markets map [GameID] []int){

	var xs []struct{
		GameID
		MarketID int `db:"market_id"`
	}
	if err := x.Select(&xs, "SELECT * FROM active_markets"); err != nil {
		log.Fatal(err)
	}

	ys := make( map [GameID] map[int] bool )

	for _,y := range xs {
		if ys[y.GameID] == nil {
			ys[y.GameID] = make(map[int] bool)
		}
		ys[y.GameID][y.MarketID] = true
	}

	markets = make( map [GameID] []int )
	for gameID, ms := range ys {
		for m := range ms{
			markets[gameID] = append(markets[gameID], m)
		}
	}

	return
}

func (x BetfairsDB) UpdateRunnerStatus(gameID GameID, marketID int, runnerID aping.RunnerID, status string){


		_, err := x.NamedExec(
			`	SELECT update_runner_status	( :event_id, :open_date, :market_id, :selection_id, :status ) `,
			map[string]interface{}{
				"event_id":     gameID.EventID,
				"open_date":    gameID.OpenDate,
				"market_id":    marketID,
				"status":       status,
				"selection_id": runnerID,
			})
		if err != nil {
			log.Fatal(gameID, marketID, runnerID, status, err)
		}
}