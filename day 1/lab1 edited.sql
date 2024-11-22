--select * from player_seasons limit 5;

-- to save attributes that will be different for different playes

--create type season_stats as (
--	season integer,
--	gp integer,
--	pts real,
--	reb real,
--	ast real
--)

--create type scoring_class as enum('star','good','average','bad')


--created players to avoid duplication on attributes that don't change and using some attributes for cumulative 
--create table players(
--	player_name text,
--	height text,
--	college text,
--	country text,
--	draft_year text,
--	draft_round text,
--	draft_number text,
--	season_stats season_stats[],
--	scoring_class scoring_class,
--	years_since_last_season integer,
--	current_season integer,
--  is_active boolean,
--	primary key(player_name,current_season)
--)

--drop table players

-- an easier way to create a series to generate the date 
INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;
-- using this we can provide the expanded columns
-- can be used to avoid joins
--select player_name,(unnest(season_stats)::season_stats).* from players where current_season = 2001 and player_name = 'Michael Jordan'

select player_name,
	(season_stats[1]::season_stats).pts as first_season,
	season_stats  as season_stats,
	(season_stats[cardinality(season_stats)])  as latest_season
from players 
where current_season = 2001
