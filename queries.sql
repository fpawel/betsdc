CREATE TYPE RUNNER_STATUS -- статус опции рынка
AS ENUM ('ACTIVE', 'REMOVED', 'WINNER', 'LOSER', 'HIDDEN', 'UNKNOWN');

TRUNCATE prices, runners, runner_names, markets, events, competitions, teams CASCADE;

drop TABLE prices, runners, runner_names, markets, events, competitions, teams CASCADE;
drop TABLE prices, runners, runner_names, markets CASCADE ;
drop TABLE prices,  runners CASCADE ;

CREATE TABLE IF NOT EXISTS teams ( -- команды
  team_id SERIAL PRIMARY KEY,     -- идентификатор команды
  team_name TEXT UNIQUE NOT NULL CONSTRAINT team_name_is_not_empty_string CHECK (team_name <> '')  -- имя команды
);

CREATE TABLE IF NOT EXISTS competitions ( -- чемпионаты
  competition_id INT UNIQUE NOT NULL CONSTRAINT positive_competition_id CHECK (competition_id > 0),   -- идентификатор чемпионата
  competition_name TEXT NOT NULL CONSTRAINT competition_name_is_not_empty_string CHECK (competition_name <> ''),       -- имя чемпионата
  PRIMARY KEY (competition_id)
);

CREATE TABLE IF NOT EXISTS events ( -- неизменяемые характеристики футбольного матча
  event_id INT NOT NULL CONSTRAINT positive_event_id CHECK (event_id > 0),     -- идентификатор матча
  open_date TIMESTAMP NOT NULL,  -- дата матча
  competition_id INT NOT NULL,  -- чемпионат
  home_id INT NOT NULL,     -- команда дома
  away_id INT NOT NULL,     -- команда в гостях
  country_code VARCHAR(2) ,  -- код страны

  FOREIGN KEY (competition_id)
  REFERENCES competitions (competition_id),
  FOREIGN KEY (home_id)
  REFERENCES teams (team_id),
  FOREIGN KEY (away_id)
  REFERENCES teams (team_id),
  PRIMARY KEY (event_id, open_date)
);


CREATE TABLE IF NOT EXISTS markets ( -- рынки
  event_id INT NOT NULL CONSTRAINT positive_event_id CHECK (event_id > 0),     -- идентификатор матча
  open_date TIMESTAMP NOT NULL,  -- дата матча
  market_id INT NOT NULL CONSTRAINT positive_market_id CHECK (market_id > 0) DEFAULT 0,   -- идентификатор рынка
  market_name TEXT NOT NULL CONSTRAINT market_name_is_not_empty_string CHECK (market_name <> ''),-- имя рынка
  total_matched NUMERIC NOT NULL CONSTRAINT positive_total_matched CHECK (total_matched > 0 OR total_matched = 0) DEFAULT 0, -- совокупный объём совпавших пари
  updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
  FOREIGN KEY (event_id,open_date)
  REFERENCES events (event_id,open_date) ON DELETE CASCADE,
  PRIMARY KEY (event_id, open_date, market_id)
);
CREATE TABLE IF NOT EXISTS runner_names (  -- наименование опции
  selection_id INT NOT NULL CONSTRAINT not_negative_selection_id CHECK (selection_id > -1),-- идентификатор опции рынка
  runner_name TEXT NOT NULL CONSTRAINT runner_name_is_not_empty_string CHECK (runner_name <> ''),-- имя опции
  PRIMARY KEY (selection_id)
);

CREATE TABLE IF NOT EXISTS runners (  -- опции на рынках
  event_id INT NOT NULL CONSTRAINT positive_event_id CHECK (event_id > 0),    -- идентификатор матча
  open_date TIMESTAMP NOT NULL,  -- дата матча
  market_id INT NOT NULL CONSTRAINT positive_market_id CHECK (market_id > 0),   -- идентификатор рынка
  selection_id INT NOT NULL CONSTRAINT not_negative_selection_id CHECK (selection_id > -1),-- идентификатор опции рынка
  status RUNNER_STATUS NOT NULL DEFAULT 'ACTIVE',  -- статус опции
  updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
  FOREIGN KEY (selection_id)
  REFERENCES runner_names (selection_id)  ON DELETE CASCADE,
  FOREIGN KEY (event_id, open_date, market_id)
  REFERENCES markets (event_id, open_date, market_id ) ON DELETE CASCADE,
  PRIMARY KEY (event_id, open_date, market_id, selection_id)
);

CREATE TABLE IF NOT EXISTS prices(  --
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- временная метка, фомируемая в момент добавления записи
  event_id INT NOT NULL CONSTRAINT positive_event_id CHECK (event_id > 0),    -- идентификатор матча
  open_date TIMESTAMP NOT NULL,  -- дата матча
  market_id INT NOT NULL CONSTRAINT positive_market_id CHECK (market_id > 0),   -- идентификатор рынка
  selection_id INT NOT NULL CONSTRAINT not_negative_selection_id CHECK (selection_id > -1),-- идентификатор опции рынка
  game_minute SMALLINT NOT NULL CONSTRAINT positive_game_minute CHECK (game_minute > -1), -- минута матча
  score_home SMALLINT NOT NULL CONSTRAINT positive_score_home CHECK (score_home > -1), -- счёт матча: домашняя команда
  score_away SMALLINT NOT NULL CONSTRAINT positive_score_away CHECK (score_away > -1), -- счёт матча: команда в гостях
  price_back0 NUMERIC NOT NULL CONSTRAINT valid_price_back0 CHECK (price_back0 > 1 AND price_back0 <= 1000),
  price_back1 NUMERIC NOT NULL CONSTRAINT valid_price_back1 CHECK (price_back1 > 1 AND price_back1 <= 1000),
  price_back2 NUMERIC NOT NULL CONSTRAINT valid_price_back2 CHECK (price_back2 > 1 AND price_back2 <= 1000),
  size_back0 NUMERIC NOT NULL CONSTRAINT positive_size_back0 CHECK (size_back0 > 0),
  size_back1 NUMERIC NOT NULL CONSTRAINT positive_size_back1 CHECK (size_back1 > 0),
  size_back2 NUMERIC NOT NULL CONSTRAINT positive_size_back2 CHECK (size_back2 > 0),
  price_lay0 NUMERIC NOT NULL CONSTRAINT valid_price_lay0 CHECK (price_lay0 > 1 AND price_lay0 <= 1000),
  price_lay1 NUMERIC NOT NULL CONSTRAINT valid_price_lay1 CHECK (price_lay1 > 1 AND price_lay1 <= 1000),
  price_lay2 NUMERIC NOT NULL CONSTRAINT valid_price_lay2 CHECK (price_lay2 > 1 AND price_lay2 <= 1000),
  size_lay0 NUMERIC NOT NULL CONSTRAINT positive_size_lay0 CHECK (size_lay0 > 0),
  size_lay1 NUMERIC NOT NULL CONSTRAINT positive_size_lay1 CHECK (size_lay1 > 0),
  size_lay2 NUMERIC NOT NULL CONSTRAINT positive_size_lay2 CHECK (size_lay2 > 0),
  FOREIGN KEY (event_id, open_date, market_id, selection_id)
  REFERENCES runners (event_id, open_date, market_id, selection_id) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION add_team( the_team_name text )
  RETURNS INT AS $$
-- создать запись в таблице teams если запись со значением поля team_name, равным the_team_name, не существует
-- вернуть team_id записи, соответствующий значению the_team_name
DECLARE
  the_team_id INT;
BEGIN
  SELECT team_id INTO the_team_id
  FROM teams
  WHERE team_name= the_team_name
  LIMIT 1;
  IF the_team_id IS NULL THEN
    INSERT INTO teams (team_name)
    VALUES (the_team_name)
    RETURNING team_id INTO the_team_id;
  END IF;
  RETURN the_team_id;
END
$$ LANGUAGE plpgsql;
SELECT team('Спартак');

CREATE OR REPLACE FUNCTION add_competition(the_competition_id INT, the_competition_name text )
  -- создать запись в таблице competitions если ключ (the_competition_id) не существует
  RETURNS VOID AS $$
BEGIN
  INSERT INTO competitions (competition_id, competition_name)
  VALUES (the_competition_id, the_competition_name) ON CONFLICT (competition_id) DO NOTHING ;
END
$$ LANGUAGE plpgsql;

SELECT competition(8596554, 'Israeli Liga Bet - North B');
SELECT competition(821269, 'Spanish Tercera Division');

CREATE OR REPLACE FUNCTION event_id_exits(
  the_event_id INT,
  the_open_date TIMESTAMP
)
  RETURNS BOOLEAN AS $$
BEGIN
  RETURN exists(SELECT  * FROM events WHERE event_id = the_event_id AND open_date = the_open_date);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_event(
  the_event_id INT,
  the_open_date TIMESTAMP,
  the_competition_id INT,
  the_competition_name text,
  the_home text,
  the_away text,
  the_country_code VARCHAR(2)
)
  -- создать запись в таблице events если ключ (the_event_id,the_open_date) не существует
  RETURNS VOID AS $$
BEGIN
  PERFORM add_competition(the_competition_id, the_competition_name);
  INSERT INTO events(event_id, open_date, competition_id,  home_id, away_id, country_code)
  VALUES (
    the_event_id,
    the_open_date,
    the_competition_id,
    add_team(the_home),
    add_team(the_away),
    the_country_code);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_markets_ids_by_event_id(
  the_event_id  INT,
  the_open_date TIMESTAMP
)
  RETURNS TABLE (market_id INT) AS $$
BEGIN
  RETURN QUERY SELECT markets.market_id FROM markets WHERE markets.event_id = the_event_id AND markets.open_date = the_open_date;
END
$$ LANGUAGE plpgsql;

SELECT add_event(
    8596554, '2018-01-20T10:00:00Z',
    821269, 'Spanish Tercera Division',
    'Budaorsi', 'Пакш',
    'RU'
);

SELECT add_event(
    8591154, '2018-02-20T10:00:00Z',
    821229, 'Лига Европы',
    'Милан', 'Реал',
    'EN'
);

SELECT event_id_exits(8596554, '2018-01-20T10:00:00Z');
SELECT event_id_exits(8596554, '2018-01-20T10:00:00Z');
SELECT event_id_exits(8591154, '2018-02-20T10:00:00Z');

SELECT add_market(8591154, '2018-02-20T10:00:00Z', 100, 'Результат');
SELECT add_market(8591154, '2018-02-20T10:00:00Z', 101, 'Счёт');
SELECT * FROM get_markets_ids_by_event_id(8591154, '2018-02-20T10:00:00Z');
SELECT * FROM get_markets_ids_by_event_id(596554, '2018-01-20T10:00:00Z');

CREATE OR REPLACE FUNCTION add_market(
  the_event_id INT,
  the_open_date TIMESTAMP,
  the_market_id INT,
  the_market_name text
)
  RETURNS VOID AS $$
-- создать запись в таблице markets если ключ (the_event_id,the_open_date,the_market_id) не существует
BEGIN
  INSERT INTO markets(event_id, open_date, market_id, market_name)
  VALUES ( the_event_id, the_open_date, the_market_id, the_market_name);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_runner(
  the_event_id INT,
  the_open_date TIMESTAMP,
  the_market_id INT,
  the_selection_id INT,
  the_runner_name TEXT
)
  RETURNS VOID AS $$
-- создать запись в таблице runners если данный ключ не существует в ней
-- иначе устанвить соответствующее ключу значение status
BEGIN
  INSERT INTO runner_names(selection_id, runner_name)
  VALUES (
    the_selection_id,
    the_runner_name
  ) ON CONFLICT (selection_id) DO NOTHING ;

  INSERT INTO runners(event_id, open_date, market_id, selection_id, status)
  VALUES (
    the_event_id,
    the_open_date,
    the_market_id,
    the_selection_id,
    'ACTIVE'
  );
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_runner_prices(
  the_event_id INT,
  the_open_date TIMESTAMP,
  the_market_id INT,
  the_selection_id INT,
  the_status RUNNER_STATUS,
  the_game_minute SMALLINT,
  the_score_home SMALLINT,
  the_score_away SMALLINT,
  the_price_back0 NUMERIC,
  the_price_back1 NUMERIC,
  the_price_back2 NUMERIC,
  the_size_back0 NUMERIC,
  the_size_back1 NUMERIC,
  the_size_back2 NUMERIC,
  the_price_lay0 NUMERIC,
  the_price_lay1 NUMERIC,
  the_price_lay2 NUMERIC,
  the_size_lay0 NUMERIC,
  the_size_lay1 NUMERIC,
  the_size_lay2 NUMERIC
)
  RETURNS VOID AS $$
BEGIN
  PERFORM update_runner_status(the_event_id, the_open_date, the_market_id, the_selection_id, the_status );
  INSERT INTO prices(
    event_id, open_date, market_id, selection_id,
    game_minute, score_home, score_away,
    price_back0, price_back1, price_back2,
    size_back0, size_back1, size_back2,
    price_lay0, price_lay1, price_lay2,
    size_lay0, size_lay1, size_lay2
  )
  VALUES(
    the_event_id, the_open_date, the_market_id, the_selection_id,
                  the_game_minute, the_score_home, the_score_away,
                  the_price_back0, the_price_back1, the_price_back2,
                  the_size_back0, the_size_back1, the_size_back2,
    the_price_lay0, the_price_lay1, the_price_lay2,
    the_size_lay0, the_size_lay1, the_size_lay2
  );
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_runner_status(
  the_event_id INT,
  the_open_date TIMESTAMP,
  the_market_id INT,
  the_selection_id INT,
  the_status RUNNER_STATUS
)
  RETURNS VOID AS $$
BEGIN
  UPDATE runners
  SET status = the_status, updated_at = current_timestamp
  WHERE
    event_id = the_event_id AND
    open_date = the_open_date AND
    market_id = the_market_id AND
    selection_id = the_selection_id;

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_market_total_matched(
  the_event_id INT,
  the_open_date TIMESTAMP,
  the_market_id INT,
  the_total_matched NUMERIC
)
  RETURNS VOID AS $$
BEGIN
  UPDATE markets
  SET total_matched = the_total_matched, updated_at = current_timestamp
  WHERE event_id = the_event_id AND open_date = the_open_date AND market_id = the_market_id;
END
$$ LANGUAGE plpgsql;

SELECT count(*) FROM ( SELECT * FROM prices ) as aa;
SELECT * FROM prices ORDER BY created_at DESC LIMIT 100;

WITH AA as (
    SELECT * FROM prices_details
    ORDER BY created_at DESC LIMIT 500
)
SELECT * FROM AA ORDER BY created_at ASC ;


WITH prices_ AS (
    SELECT * FROM prices
    --WHERE market_id = 139311020 AND selection_id = 4202993
    ORDER BY created_at DESC LIMIT 500
)
SELECT
  p.created_at
  , p.market_id, p.selection_id
  , et.home, et.away, et.competition
  , m.market_name, m.total_matched
  , r.runner_name, rn.status
  , p.game_minute, p.score_home, p.score_away
  , p.price_back0, p.price_back1, p.price_back2
  --, p.price_lay0, p.price_lay1, p.price_lay2
  , p.size_back0,p.size_back1,p.size_back2
--, p.size_lay0,p.size_lay1,p.size_lay2
FROM prices_ p
  INNER JOIN event_teams_competition et
    ON
      et.event_id = p.event_id AND
      et.open_date = p.open_date
  INNER JOIN markets m
    ON
      m.market_id = p.market_id AND
      m.event_id = p.event_id AND
      m.open_date = p.open_date
  INNER JOIN runners rn
    ON
      rn.selection_id = p.selection_id AND
      rn.market_id = p.market_id AND
      rn.event_id = p.event_id AND
      rn.open_date = p.open_date
  INNER JOIN runner_names r
    ON
      r.selection_id = p.selection_id ;

CREATE VIEW event_teams_competition AS
  SELECT e.event_id, e.open_date, th.team_name as home, ta.team_name as away, c2.competition_name as competition
  FROM events e
    INNER JOIN teams th ON e.home_id = th.team_id
    INNER JOIN teams ta ON e.away_id = ta.team_id
    INNER JOIN competitions c2 ON e.competition_id = c2.competition_id;


CREATE VIEW events_unknown_status AS
  SELECT event_id, open_date, market_id
  FROM runners
  WHERE status = 'ACTIVE'
        AND current_timestamp - updated_at > INTERVAL '1 hour';

SELECT * FROM events_unknown_status;

SELECT event_id, market_id
FROM runners
WHERE status = 'ACTIVE'
      AND current_timestamp - updated_at > INTERVAL '1 hour';