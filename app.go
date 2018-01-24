package main

import (
	"github.com/jmoiron/sqlx"
	"github.com/fpawel/betfairs/aping/listMarketCatalogue"
	"github.com/fpawel/betfairs/aping/listMarketBook"
	"os"
	"fmt"
	"log"
	"io/ioutil"
	"github.com/fpawel/betfairs/event2"
	"github.com/fpawel/betfairs/football"
	"github.com/fpawel/betfairs/aping"
	"net/http"
	"encoding/json"

	_ "github.com/lib/pq"
	"time"
)

type App struct{
	db *sqlx.DB
	mcr *listMarketCatalogue.Reader
	mbr *listMarketBook.Reader
}

func (x App) Close() error {
	return x.db.Close()
}

func newApp() (x App) {

	apingSession := aping.NewSession(os.Getenv("BETFAIR_LOGIN_USER"), os.Getenv("BETFAIR_LOGIN_PASS"))
	fmt.Println(apingSession.GetSession())

	x.mcr = listMarketCatalogue.New(apingSession)
	x.mbr = listMarketBook.New(apingSession)

	var err error
	x.db, err = sqlx.Connect("postgres", dbConnStr)
	if err != nil {
		log.Fatalln(err)
	}
	return
}


func (x App) processGameLive( game football.GameLive ) {
	var mids []int
	x.db.Select(&mids, "SELECT * FROM get_markets_ids_by_event_id($1, $2)", game.ID, game.OpenDate)
	if mids == nil {
		if err := x.addGameEvent(game.ID); err != nil {
			fmt.Println("ERROR adding game:", game, err )
			return
		}
	}
	var marketIDs []aping.MarketID
	for _,n := range mids{
		marketIDs = append(marketIDs, aping.MarketID(fmt.Sprintf("1.%d", n)))
	}
	mbs,err := x.mbr.Read(marketIDs, 5 * time.Second)
	if err != nil {
		fmt.Println("ERROR reading prices:", game, err )
		return
	}
	for _,m := range mbs{
		for _,r := range m.Runners{
			for n,p := range r.ExchangePrices.AvailableToBack{
				_,err = x.db.NamedExec(
					`
	INSERT INTO prices 
		( 	created_at, event_id, open_date, market_id, selection_id, side, price_index, 
			game_minute, score_home, score_away, 
			market_total_matched, market_total_available, 
			runner_last_price_traded, 
			price, size)
	VALUES 
		( 	current_date, :event_id, :open_date, :market_id, :selection_id, :side, :price_index,
			:game_minute, :score_home, :score_away, 
			:market_total_matched, :market_total_available, 
			:runner_last_price_traded, 
			:price, :size) `,
					map[string]interface{}{
						"event_id":         game.ID,
						"open_date":        game.OpenDate,
						"market_id":   	m.ID.Int(),
						"selection_id": r.ID,
						"side":            "B",
						"price_index":  n,
						"game_minute":     game.Minute,
						"score_home": game.ScoreHome,
						"score_away": game.ScoreAway,
						"market_total_matched":m.TotalMatched,
						"market_total_available":m.TotalAvailable,
						"runner_last_price_traded":r.LastPriceTraded,
						"price":p.Price,
						"size":p.Size,
					})
				if err != nil {
					log.Fatal(err )
				}
			}
		}
	}

}

func (x App) addGameEvent(gameID int) error {
	r,err := http.Get(fmt.Sprintf("https://betfairs.herokuapp.com/event/%d", gameID))
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
		log.Fatal(err )
	}
	_,err = x.db.NamedExec(
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
		log.Fatal(err )
	}
	for _, mrkt := range evt.Markets{
		_,err = x.db.NamedExec(
			`SELECT add_market( :eventID, :openDate, :marketID, :marketName);`,
			map[string]interface{}{
				"eventID":    evt.ID,
				"openDate":   evt.OpenDate,
				"marketID":   mrkt.ID,
				"marketName": mrkt.Name,
			})
		if err != nil {
			log.Fatal(err )
		}
		for _, rnr := range mrkt.Runners{
			_,err = x.db.NamedExec(
				`SELECT add_runner( :eventID, :openDate, :marketID, :runnerID, :runnerName);`,
				map[string]interface{}{
					"eventID":    evt.ID,
					"openDate":   evt.OpenDate,
					"marketID":   mrkt.ID,
					"runnerID":   rnr.ID,
					"runnerName": rnr.Name,
				})
			if err != nil {
				log.Fatal(err )
			}

		}

	}
	return nil
}


