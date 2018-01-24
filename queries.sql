CREATE TYPE RUNNER_STATUS -- статус опции рынка
AS ENUM ('ACTIVE', 'REMOVED', 'WINNER', 'LOSER', 'HIDDEN', 'UNKNOWN');

CREATE TYPE SIDE  -- тип ставки
AS ENUM (
  'B',  -- ставка ЗА
  'L'   -- ставка ПРОТИВ
);

TRUNCATE  runners, markets, events, competitions, teams CASCADE ;

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
  open_date DATE NOT NULL,  -- дата матча
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
  open_date DATE NOT NULL,  -- дата матча
  market_id INT NOT NULL CONSTRAINT positive_market_id CHECK (market_id > 0),   -- идентификатор рынка
  market_name TEXT NOT NULL CONSTRAINT market_name_is_not_empty_string CHECK (market_name <> ''),-- имя рынка
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
  open_date DATE NOT NULL,  -- дата матча
  market_id INT NOT NULL CONSTRAINT positive_market_id CHECK (market_id > 0),   -- идентификатор рынка
  selection_id INT NOT NULL CONSTRAINT not_negative_selection_id CHECK (selection_id > -1),-- идентификатор опции рынка
  status RUNNER_STATUS NOT NULL,  -- статус опции
  FOREIGN KEY (event_id, open_date, market_id)
  REFERENCES markets (event_id, open_date, market_id ) ON DELETE CASCADE,
  PRIMARY KEY (event_id, open_date, market_id, selection_id)
);

CREATE TABLE IF NOT EXISTS prices(  --
  created_at DATE,          -- временная метка, фомируемая в момент добавления записи
  event_id INT NOT NULL CONSTRAINT positive_event_id CHECK (event_id > 0),    -- идентификатор матча
  open_date DATE NOT NULL,  -- дата матча
  market_id INT NOT NULL CONSTRAINT positive_market_id CHECK (market_id > 0),   -- идентификатор рынка
  selection_id INT NOT NULL CONSTRAINT not_negative_selection_id CHECK (selection_id > -1),-- идентификатор опции рынка
  side SIDE NOT NULL,       -- вид ставки: ЗА или ПРОТИВ
  price_index SMALLINT NOT NULL CONSTRAINT price_index_form_0_to_2 CHECK (price_index > -1 AND price_index < 3), -- позиция котировки в списке котировок
  game_minute SMALLINT NOT NULL CONSTRAINT positive_game_minute CHECK (game_minute > -1), -- минута матча
  score_home SMALLINT NOT NULL CONSTRAINT positive_score_home CHECK (score_home > -1), -- счёт матча: домашняя команда
  score_away SMALLINT NOT NULL CONSTRAINT positive_score_away CHECK (score_away > -1), -- счёт матча: команда в гостях
  market_total_matched NUMERIC NOT NULL, -- совокупный объём совпавших пари на рынке опции, к которой относится котировка
  market_total_available NUMERIC NOT NULL, -- совокупный объём доступных пари на рынке опции, к которой относится котировка
  runner_last_price_traded NUMERIC NOT NULL, -- последний сыгрынный коэффициент для опции, к которой относится котировка
  price NUMERIC NOT NULL, -- значение доступного коэффициента
  size NUMERIC NOT NULL,  -- значение доступной ставки в долларах
  FOREIGN KEY (event_id, open_date, market_id, selection_id)
  REFERENCES runners (event_id, open_date, market_id, selection_id) ON DELETE CASCADE
);


INSERT INTO prices
( created_at, event_id, open_date, market_id, selection_id, side, price_index, game_minute, score_home, score_away, market_total_matched, market_total_available, runner_last_price_traded, price, size)
VALUES (current_date, '2018-01-20T10:00:00Z');


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
  the_open_date DATE
)
  RETURNS BOOLEAN AS $$
BEGIN
  RETURN exists(SELECT  * FROM events WHERE event_id = the_event_id AND open_date = the_open_date);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_event(
  the_event_id INT,
  the_open_date DATE,
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
  the_open_date DATE
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
  the_open_date DATE,
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
  the_open_date DATE,
  the_market_id INT,
  the_selection_id INT,
  the_runner_name TEXT
)
  RETURNS VOID AS $$
-- создать запись в таблице runners если данный ключ не существует в ней
-- иначе устанвить соответствующее ключу значение status
BEGIN
  INSERT INTO runners(event_id, open_date, market_id, selection_id, status)
  VALUES (
    the_event_id,
    the_open_date,
    the_market_id,
    the_selection_id,
    'ACTIVE'
  );
  INSERT INTO runner_names(selection_id, runner_name)
  VALUES ( the_selection_id, the_runner_name )
  ON CONFLICT (selection_id) DO NOTHING;
END
$$ LANGUAGE plpgsql;

