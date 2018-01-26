package main

import (
	"encoding/json"
	"fmt"
	"github.com/fpawel/betfairs/aping"
	"github.com/fpawel/betfairs/event2"
	"github.com/fpawel/betfairs/football"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
	"time"
)

type App struct {
	db           *sqlx.DB
	apingSession *aping.Session
}

func (x App) Close() error {
	return x.db.Close()
}

func newApp() (x App) {

	x.apingSession = aping.NewSession(os.Getenv("BETFAIR_LOGIN_USER"), os.Getenv("BETFAIR_LOGIN_PASS"))
	fmt.Println(x.apingSession.GetSession())

	var err error
	connStr := os.Getenv("BETFAIRS_DB_CONN_STR")
	x.db, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		log.Fatalln(err)
	}
	return
}

func (x App) processGameLive(game football.GameLive) {
	var mids []int
	if err := x.db.Select(&mids, "SELECT * FROM get_markets_ids_by_event_id($1, $2)", game.ID, game.OpenDate); err != nil {
		log.Fatal(err)
	}
	if len(mids) == 0 {
		if err := dbAddGameEvent(x.db, game); err != nil {
			fmt.Println("ERROR adding game:", game, err)
			return
		}
	}
	var marketIDs []aping.MarketID
	for _, n := range mids {
		marketIDs = append(marketIDs, aping.MarketID(fmt.Sprintf("1.%d", n)))
	}
	mbs, err := x.apingSession.ListMarketBook(marketIDs)
	if err != nil {
		fmt.Println("ERROR reading prices:", game, err)
		return
	}
	for _, m := range mbs {
		dbUpdateMarket(x.db, game, m)
		for _, r := range m.Runners {
			if runnerHasPrices(r) {
				dbAddRunnerPrices(x.db, game, m, r)
			}
		}
	}
}

func dbAddGameEvent(db *sqlx.DB, game football.GameLive) error {
	r, err := http.Get(fmt.Sprintf("https://betfairs.herokuapp.com/event/%d", game.ID))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	var evt event2.Event
	if err := json.Unmarshal(b, &evt); err != nil {
		log.Fatal(err)
	}
	_, err = db.NamedExec(
		`SELECT add_event( :eventID, :openDate, :competitionID, :competitionName, :home, :away, :countryCode);`,
		map[string]interface{}{
			"eventID":         evt.ID,
			"openDate":        evt.OpenDate,
			"competitionID":   evt.CompetitionID,
			"competitionName": evt.CompetitionName,
			"home":            evt.Home,
			"away":            evt.Away,
			"countryCode":     evt.CountryCode,
		})
	if err != nil {
		log.Fatal(game, err)
	}
	for _, mrkt := range evt.Markets {
		_, err = db.NamedExec(
			`SELECT add_market( :eventID, :openDate, :marketID, :marketName);`,
			map[string]interface{}{
				"eventID":    evt.ID,
				"openDate":   evt.OpenDate,
				"marketID":   mrkt.ID,
				"marketName": mrkt.Name,
			})
		if err != nil {
			log.Fatal(game, mrkt, err)
		}
		for _, rnr := range mrkt.Runners {
			_, err = db.NamedExec(
				`SELECT add_runner( :eventID, :openDate, :marketID, :runnerID, :runnerName);`,
				map[string]interface{}{
					"eventID":    evt.ID,
					"openDate":   evt.OpenDate,
					"marketID":   mrkt.ID,
					"runnerID":   rnr.ID,
					"runnerName": rnr.Name,
				})
			if err != nil {
				log.Fatal(game, mrkt, rnr, err)
			}

		}

	}
	return nil
}

func dbAddRunnerPrices(db *sqlx.DB, game football.GameLive, m aping.MarketBook, r aping.Runner) {
	if !runnerHasPrices(r) {
		log.Fatal("wrong runner prices")
	}
	ex := r.ExchangePrices
	b := ex.AvailableToBack
	l := ex.AvailableToLay
	_, err := db.NamedExec(
		`
SELECT add_runner_prices
( 	
	:event_id, :open_date, :market_id, :selection_id,
	:status, 
	:game_minute, :score_home, :score_away,
	:price_back0, :price_back1, :price_back2,
	:size_back0, :size_back1, :size_back2,
	:price_lay0, :price_lay1, :price_lay2,
	:size_lay0, :size_lay1, :size_lay2	
) `,
		map[string]interface{}{
			"event_id":     game.ID,
			"open_date":    game.OpenDate,
			"market_id":    m.ID.Int(),
			"status":       r.Status,
			"selection_id": r.ID,
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
		})
	if err != nil {
		log.Fatal(game, m, r, err)
	}
}

func dbUpdateMarket(db *sqlx.DB, game football.GameLive, m aping.MarketBook) {
	_, err := db.NamedExec(
		`
SELECT update_market_total_matched(
  :event_id,
  :open_date,
  :market_id,
  :total_matched
)`,
		map[string]interface{}{
			"event_id":      game.ID,
			"open_date":     game.OpenDate,
			"market_id":     m.ID.Int(),
			"total_matched": m.TotalMatched,
		})
	if err != nil {
		log.Fatal(err)
	}
}

func (x App) processGamesWithUnknownStatus() {
	type id struct {
		EventID int `db:"event_id"`
		OpenDate time.Time `db:"open_date"`
	}
	var exs []struct{
		id
		MarketID int `db:"market_id"`
	}
	if err := x.db.Select(&exs, "SELECT * FROM events_unknown_status"); err != nil {
		log.Fatal(err)
	}

	if len(exs) == 0 {
		return
	}



	m := make(map[id][]aping.MarketID)
	for _,y := range exs{
		m[y.id] = append(m[y.id], aping.MarketID(fmt.Sprintf("1.%d", y.MarketID)))
	}
	for evt, marketIDs := range m {
		mbs, err := x.apingSession.ListMarketBook(marketIDs)
		if err != nil {
			fmt.Println("ERROR reading prices:", evt, marketIDs, err)
			return
		}
		for _, m := range mbs {
			for _, r := range m.Runners {
				_, err := x.db.NamedExec(
					`
			SELECT update_runner_status
			( 	
				:event_id, :open_date, :market_id, :selection_id,
				:status
		  	) `,
					map[string]interface{}{
						"event_id":     evt.EventID,
						"open_date":    evt.OpenDate,
						"market_id":    m.ID.Int(),
						"status":       r.Status,
						"selection_id": r.ID,
					})
				if err != nil {
					log.Fatal(evt, m, r, err)
				}
			}
		}
	}
}

func runnerHasPrices(r aping.Runner) bool {
	ex := r.ExchangePrices
	return len(ex.AvailableToBack) == 3 && len(ex.AvailableToLay) == 3
}