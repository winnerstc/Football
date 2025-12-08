


--------------Drop fact table-------------
drop table fact_player_stats;

-----------create fact table-----------
CREATE TABLE Fact_Player_Stats (
player_key        INT,
college_key       STRING,
team_key          STRING,
position_key      INT,
passing_yards     INT,
passing_tds       INT,
rushing_yards     INT,
rushing_tds       INT,
receiving_yards   INT,
receiving_tds     INT,
total_tackles     INT,
sacks             INT,
ints              INT,
fgs_made          INT,
kick_return_tds   INT,
punt_return_tds   INT,
years_played      STRING,
good_years        INT
)
STORED AS ORC;


---------------create/replace views--------------------------
--------pre conditioning the inserts for the table-----------------
CREATE OR REPLACE VIEW v_passing AS
SELECT CAST(player_id AS INT)              AS player_id,
       SUM(COALESCE(passing_yards,0))      AS passing_yards,
       SUM(COALESCE(td_passes,0))          AS passing_tds,
       COUNT(DISTINCT year)                AS years
FROM   nfl_passing_silver
GROUP  BY player_id;

CREATE OR REPLACE VIEW v_rushing AS
SELECT CAST(player_id AS INT)              AS player_id,
       SUM(COALESCE(rushing_yards,0))      AS rushing_yards,
       SUM(COALESCE(rushing_tds,0))        AS rushing_tds,
       COUNT(DISTINCT year)                AS years
FROM   nfl_rushing_silver
GROUP  BY player_id;

CREATE OR REPLACE VIEW v_receiving AS
SELECT CAST(player_id AS INT)              AS player_id,
       SUM(COALESCE(receiving_yards,0))    AS receiving_yards,
       SUM(COALESCE(receiving_tds,0))      AS receiving_tds,
       COUNT(DISTINCT year)                AS years
FROM   nfl_receiving_silver
GROUP  BY player_id;

CREATE OR REPLACE VIEW v_defense AS
SELECT CAST(player_id AS INT)              AS player_id,
       SUM(COALESCE(total_tackles,0))      AS total_tackles,
       SUM(COALESCE(sacks,0))              AS sacks,
       SUM(COALESCE(ints,0))               AS ints,
       COUNT(DISTINCT year)                AS years
FROM   nfl_defensive_silver
GROUP  BY player_id;

CREATE OR REPLACE VIEW v_kicking AS
SELECT CAST(player_id AS INT)              AS player_id,
       SUM(COALESCE(fgs_made,0))           AS fgs_made,
       COUNT(DISTINCT year)                AS years
FROM   nfl_kicking_silver
GROUP  BY player_id;

CREATE OR REPLACE VIEW v_returns AS
SELECT CAST(player_id AS INT)              AS player_id,
       SUM(COALESCE(kick_returns_for_tds,0)) AS kick_return_tds,
       SUM(COALESCE(punt_returns_for_tds,0)) AS punt_return_tds,
       COUNT(DISTINCT year)                  AS years
FROM   nfl_returns_silver
GROUP  BY player_id;

--------------------insert into the fact table----------------------



WITH stats0 AS (
    SELECT COALESCE(p.player_id,
                    r.player_id,
                    c.player_id,
                    d.player_id,
                    k.player_id,
                    t.player_id) AS player_id,
           COALESCE(p.passing_yards ,0)  AS passing_yards,
           COALESCE(p.passing_tds   ,0)  AS passing_tds,
           COALESCE(r.rushing_yards ,0)  AS rushing_yards,
           COALESCE(r.rushing_tds   ,0)  AS rushing_tds,
           COALESCE(c.receiving_yards,0) AS receiving_yards,
           COALESCE(c.receiving_tds ,0)  AS receiving_tds,
           COALESCE(d.total_tackles ,0)  AS total_tackles,
           COALESCE(d.sacks         ,0)  AS sacks,
           COALESCE(d.ints          ,0)  AS ints,
           COALESCE(k.fgs_made      ,0)  AS fgs_made,
           COALESCE(t.kick_return_tds ,0) AS kick_return_tds,
           COALESCE(t.punt_return_tds ,0) AS punt_return_tds
    FROM v_passing  p
    FULL JOIN v_rushing   r ON p.player_id = r.player_id
    FULL JOIN v_receiving c ON COALESCE(p.player_id, r.player_id) = c.player_id
    FULL JOIN v_defense   d ON COALESCE(p.player_id, r.player_id, c.player_id) = d.player_id
    FULL JOIN v_kicking   k ON COALESCE(p.player_id, r.player_id, c.player_id, d.player_id) = k.player_id
    FULL JOIN v_returns   t ON COALESCE(p.player_id, r.player_id, c.player_id, d.player_id, k.player_id) = t.player_id
),
position_map AS (
    SELECT player_id,
           CASE
             WHEN passing_yards >= GREATEST(passing_yards,rushing_yards,receiving_yards,sacks,ints,fgs_made,kick_return_tds,punt_return_tds) THEN 1
             WHEN rushing_yards >= GREATEST(passing_yards,rushing_yards,receiving_yards,sacks,ints,fgs_made,kick_return_tds,punt_return_tds) THEN 2
             WHEN receiving_yards>=GREATEST(passing_yards,rushing_yards,receiving_yards,sacks,ints,fgs_made,kick_return_tds,punt_return_tds) THEN 3
             WHEN sacks          >=GREATEST(passing_yards,rushing_yards,receiving_yards,sacks,ints,fgs_made,kick_return_tds,punt_return_tds) THEN 4
             WHEN ints           >=GREATEST(passing_yards,rushing_yards,receiving_yards,sacks,ints,fgs_made,kick_return_tds,punt_return_tds) THEN 5
             WHEN fgs_made       >=GREATEST(passing_yards,rushing_yards,receiving_yards,sacks,ints,fgs_made,kick_return_tds,punt_return_tds) THEN 6
             WHEN kick_return_tds+punt_return_tds >=GREATEST(passing_yards,rushing_yards,receiving_yards,sacks,ints,fgs_made,kick_return_tds,punt_return_tds) THEN 7
             ELSE 1
           END AS position_key
    FROM stats0
),
 ------------ year span (raw tables) ------------ 
years_span AS (
    SELECT player_id,
           CAST(MIN(year) AS STRING) AS y_min,
           CAST(MAX(year) AS STRING) AS y_max
           FROM (
                 SELECT player_id, year FROM nfl_passing_silver
                 UNION ALL
                 SELECT player_id, year FROM nfl_rushing_silver
                 UNION ALL
                 SELECT player_id, year FROM nfl_receiving_silver
                 UNION ALL
                 SELECT player_id, year FROM nfl_defensive_silver
                 UNION ALL
                 SELECT player_id, year FROM nfl_kicking_silver
                 UNION ALL
                 SELECT player_id, year FROM nfl_returns_silver
                ) u
           GROUP BY player_id
       ),
/* ------------ good years via views ------------ */
good_years AS (
    SELECT s.player_id,
           GREATEST(COALESCE(p.years,0),
                    COALESCE(r.years,0),
                    COALESCE(c.years,0),
                    COALESCE(d.years,0),
                    COALESCE(k.years,0),
                    COALESCE(t.years,0)) AS good_years
    FROM (SELECT DISTINCT player_id FROM stats0) s
    LEFT JOIN v_passing  p ON s.player_id = p.player_id
    LEFT JOIN v_rushing  r ON s.player_id = r.player_id
    LEFT JOIN v_receiving c ON s.player_id = c.player_id
    LEFT JOIN v_defense  d ON s.player_id = d.player_id
    LEFT JOIN v_kicking  k ON s.player_id = k.player_id
    LEFT JOIN v_returns  t ON s.player_id = t.player_id
),
/* ---------- team fallback (unchanged) ---------- */
all_teams AS (
    SELECT player_id, team FROM nfl_passing_silver
    UNION ALL
    SELECT player_id, team FROM nfl_rushing_silver
    UNION ALL
    SELECT player_id, team FROM nfl_receiving_silver
    UNION ALL
    SELECT player_id, team FROM nfl_defensive_silver
    UNION ALL
    SELECT player_id, team FROM nfl_kicking_silver
    UNION ALL
    SELECT player_id, team FROM nfl_returns_silver
),
best_team AS (
    SELECT player_id,
           FIRST_VALUE(team) OVER (PARTITION BY player_id ORDER BY cnt DESC, team) AS team
    FROM (
          SELECT player_id, team, COUNT(*) AS cnt
          FROM all_teams
          GROUP BY player_id, team
         ) x
),
player_team AS (
    SELECT CAST(pl.player_id AS INT) AS player_id,
           COALESCE(pl.current_team, bt.team) AS final_team
    FROM nfl_players_silver pl
    LEFT JOIN best_team bt ON pl.player_id = bt.player_id
)
/* ---------- final insert ------------*/
INSERT INTO Fact_Player_Stats
SELECT
    s.player_id                                           AS player_key,
    cd.school                                             AS college_key,
    td.team_name                                          AS team_key,
    pm.position_key,
    s.passing_yards,
    s.passing_tds,
    s.rushing_yards,
    s.rushing_tds,
    s.receiving_yards,
    s.receiving_tds,
    s.total_tackles,
    s.sacks,
    s.ints,
    s.fgs_made,
    s.kick_return_tds,
    s.punt_return_tds,
    CONCAT(COALESCE(ys.y_min,''), '-', COALESCE(ys.y_max,'')) AS years_played,
    gy.good_years
FROM stats0 s
JOIN player_team pt ON s.player_id = pt.player_id
JOIN nfl_players_silver pl ON s.player_id = CAST(pl.player_id AS INT)
LEFT JOIN College_Dim cd ON pl.college = cd.school
LEFT JOIN Teams_Dim td ON pt.final_team = td.team_name
LEFT JOIN position_map pm ON s.player_id = pm.player_id
LEFT JOIN good_years gy ON s.player_id = gy.player_id
LEFT JOIN years_span ys ON s.player_id = ys.player_id;


drop table fix_college;

-- 1.  one-time build (ALL players, no NULL filter)
CREATE TABLE fix_college
STORED AS ORC
AS
SELECT DISTINCT                      -- or GROUP BY if you prefer
       f.player_key,
       p.college AS college_key
FROM   Fact_Player_Stats f
JOIN   nfl_players_silver p
  ON   CAST(p.player_id AS INT) = f.player_key;

-- 2.  overwrite, but only update the NULL college_key rows
INSERT OVERWRITE TABLE Fact_Player_Stats
SELECT f.player_key,
       COALESCE(fc.college_key, f.college_key) AS college_key,
       f.team_key,
       f.position_key,
       f.passing_yards,
       f.passing_tds,
       f.rushing_yards,
       f.rushing_tds,
       f.receiving_yards,
       f.receiving_tds,
       f.total_tackles,
       f.sacks,
       f.ints,
       f.fgs_made,
       f.kick_return_tds,
       f.punt_return_tds,
       f.years_played,
       f.good_years
FROM   Fact_Player_Stats f
LEFT JOIN (
        SELECT player_key,
               MAX(college_key) AS college_key
        FROM   fix_college
        GROUP  BY player_key          -- guarantees 1 row per player
) fc
ON f.player_key = fc.player_key;
---------------------update external star schema tables-----------------------

INSERT overwrite table positions_dim_ext
SELECT * FROM positions_dim;



INSERT overwrite table college_dim_ext
SELECT * FROM college_dim;

INSERT overwrite table teams_dim_ext
SELECT * FROM teams_dim;



INSERT overwrite table players_dim_ext
SELECT * FROM players_dim;




INSERT overwrite table fact_player_stats_ext
SELECT * FROM fact_player_stats;



