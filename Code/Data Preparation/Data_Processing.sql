#Batter Points

SET @single = 3
SET @double=5;
SET @triple=8;
SET @hr=10   ;
SET @rbi=2   ;
SET @run=2   ;
SET @walk=2  ;
SET @hbp=2   ;
SET @sb=5    ;

#Pitcher Points
SET @ip=2.25; #innings pitched
SET @so=2; #strikeout
SET @win=4;
SET @era=-2; #earned run allowed
SET @ha=-0.6; #hit against (allowed a hit)
SET @walk=-0.6;
SET @hb=-0.6; #hit batter
SET @cg=2.5; #complete game
SET @cgs=2.5; #complete game shutout
SET @nh=5; #no hitter

DROP TABLE at_bat_level_2016;
CREATE TABLE at_bat_level_2016
	SELECT seq_events, 
	       E.YEAR_ID, 
	       E.GAME_ID,
	       GAME_DATE, 
	       bat_id, 
	       PA_BALL_CT, 
	       PA_SWINGMISS_STRIKE_CT, 
	       BAT_HOME_ID, 
	       BAT_FATE_ID, 
	       EVENT_CD, 
	       RBI_CT, 
	       event_tx, 
	       BAT_LINEUP_ID, 
	       BASE1_RUN_ID, 
	       BASE2_RUN_ID, 
	       BASE3_RUN_ID,
	       RUN1_SB_FL,
	       RUN2_SB_FL, 
	       RUN3_SB_FL, 
	       ab_fl, 
	       PR_Run1_fl, 
	       PR_Run2_fl, 
	       PR_Run3_fl, 
	       INN_CT, 
	       AWAY_SCORE_CT, 
	       HOME_SCORE_CT, 
	       HOME_TEAM_ID, 
	       BAT_HAND_CD, 
	       PIT_HAND_CD, 
	       PIT_ID,
	       TEMP_PARK_CT, 
	       WIND_DIRECTION_PARK_CD, 
	       WIND_SPEED_PARK_CT, 
	       PRECIP_PARK_CD, 
	       SKY_PARK_CD,
	       WIN_PIT_ID,
	       park_id,
	       SUM(CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS hits,
		SUM(ab_fl = 'T') OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS at_bats,
		SUM(CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT)/SUM(ab_fl = 'T') OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS batting_average,
		SUM(CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gamehits,
		SUM(ab_fl) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY seq_events, bat_id, GAME_ID, PIT_HAND_CD) AS gameab,
		SUM(CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY seq_events, bat_id, GAME_ID, PIT_HAND_CD)/SUM(ab_fl) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY seq_events, bat_id, GAME_ID, PIT_HAND_CD) AS gameba,
		SUM(CASE WHEN EVENT_CD = '20' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gamesingles,
		SUM(CASE WHEN EVENT_CD = '21' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gamedoubles,
		SUM(CASE WHEN EVENT_CD = '22' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gametriples,
		SUM(CASE WHEN EVENT_CD = '23' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gamehr,
		SUM(CASE WHEN ab_fl = 'T' OR EVENT_CD IN (14, 15, 16) THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gamepa,
		SUM(CASE WHEN EVENT_CD = '15' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gameibb,
		SUM(CASE WHEN EVENT_CD = '14' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gameubb,
		SUM(PA_BALL_CT) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gameballs,
		SUM(PA_SWINGMISS_STRIKE_CT) OVER (PARTITION BY bat_id, GAME_ID, PIT_HAND_CD ORDER BY GAME_DATE, bat_id, PIT_HAND_CD) AS gameswingstrikes,

	       -- Add the case expression for hit_fl
	       CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END AS hit_fl,
	       -- Add the case expression for pitchingpoints
	       CASE 
		 WHEN EVENT_CD = "3" THEN 2
		 WHEN EVENT_CD IN ("20", "21", "22", "23") THEN -0.6 #hit_fl logic
		 WHEN EVENT_CD IN ("14", "16") THEN -0.6
		 ELSE 0
	       END AS pitchingpoints,
	       -- Add the case expression for fantasypoints
		      CASE 
		 WHEN EVENT_CD = "23" THEN 12
		 WHEN EVENT_CD = "20" THEN 3 + RBI_CT * 3
		 WHEN EVENT_CD = "21" THEN 6 + RBI_CT * 3
		 WHEN EVENT_CD IN ("14", "16") THEN 3 + RBI_CT * 3
		 WHEN EVENT_CD = "22" THEN 9 + RBI_CT * 3
		 ELSE 0
	       END AS fantasypoints
	        
	FROM EVENTS E
	LEFT JOIN (
	  SELECT year_id, 
		 GAME_ID, 
		 TEMP_PARK_CT, 
		 WIND_DIRECTION_PARK_CD, 
		 WIND_SPEED_PARK_CT, 
		 PRECIP_PARK_CD, 
		 SKY_PARK_CD,
		 WIN_PIT_ID,
		 park_id
	  FROM games
	) G
	ON E.GAME_ID = G.GAME_ID
	WHERE E.year_id= '2016'
;

CREATE INDEX idx_at_bat_level_2016
ON at_bat_level_2016 (seq_events, GAME_ID);

SELECT seq_events, YEAR_ID, GAME_ID, GAME_DATE, BAT_HAND_CD, PIT_HAND_CD, bat_id, hit_fl, hits, gamehits
FROM at_bat_level_2016
ORDER BY bat_id, GAME_DATE, hits
;

DROP TABLE game_matchup_level_2016;
CREATE TABLE game_matchup_level_2016
SELECT	YEAR_ID, 
	       GAME_ID,
	       GAME_DATE, 
	       BAT_HAND_CD, 
	       PIT_HAND_CD,
	       bat_id,
	       #PIT_ID,		   
	       MAX(BAT_HOME_ID             ) AS BAT_HOME_ID,  
	       SUM(RBI_CT                  ) AS RBI_CT, 
	       MAX(BAT_LINEUP_ID           ) AS  BAT_LINEUP_ID,    
	       MAX(AWAY_SCORE_CT           ) AS  AWAY_SCORE_CT, 
	       MAX(HOME_SCORE_CT           ) AS  HOME_SCORE_CT, 
	       MAX(HOME_TEAM_ID  	       ) AS  HOME_TEAM_ID, 	      
	       MAX(TEMP_PARK_CT            ) AS  TEMP_PARK_CT, 
	       MAX(WIND_DIRECTION_PARK_CD  ) AS  WIND_DIRECTION_PARK_CD,
	       MAX(WIND_SPEED_PARK_CT      ) AS  WIND_SPEED_PARK_CT, 
	       MAX(PRECIP_PARK_CD          ) AS  PRECIP_PARK_CD, 
	       MAX(SKY_PARK_CD             ) AS  SKY_PARK_CD,
	       MAX(WIN_PIT_ID              ) AS  WIN_PIT_ID,
	       MAX(park_id                 ) AS  park_id,
	       MAX( gamehits               ) AS  gamehits         ,
	       MAX( gameab                 ) AS  gameab           ,
	       MAX( gamesingles            ) AS  gamesingles      ,
	       MAX( gamedoubles            ) AS  gamedoubles      ,
	       MAX( gametriples            ) AS  gametriples      ,
	       MAX( gamehr                 ) AS  gamehr           ,
	       MAX( gamepa                 ) AS  gamepa           ,
	       MAX( gameibb                ) AS  gameibb          ,
	       MAX( gameubb                ) AS  gameubb          ,
	       MAX( gameballs              ) AS  gameballs        ,
	       MAX( gameswingstrikes       ) AS  gameswingstrikes ,
	       MAX( pitchingpoints         ) AS  pitchingpoints   ,
	       MAX( fantasypoints          ) AS  fantasypoints    
	FROM at_bat_level_2016
	GROUP BY YEAR_ID, GAME_ID, BAT_HAND_CD, PIT_HAND_CD, bat_id#, PIT_ID	
	;
	
	CREATE INDEX game_matchup_level_2016
	ON game_matchup_level_2016 (YEAR_ID, GAME_ID, BAT_HAND_CD, PIT_HAND_CD, bat_id);
	
CREATE TABLE RR_matchup_2016
SELECT *
FROM game_matchup_level_2016
WHERE bat_hand_cd='R' AND PIT_HAND_CD='R'

SELECT
    YEAR_ID,
    g.bat_id,
    g.GAME_DATE,
    g.PIT_HAND_CD,
    gamehits,
    gamehits_l1,
    gamehits_l2,
    gamehits_l3,
    gamehits_l4,
    gamehits_l5,
    gamehits_l1+gamehits_l2 AS rollinghits_l2
FROM
    game_matchup_level_2016 g
LEFT JOIN
    (
        SELECT
            bat_id,
            GAME_DATE,
            PIT_HAND_CD ,
	    LAG(gamehits, 1) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l1,
	    LAG(gamehits, 2) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l2,
            LAG(gamehits, 3) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l3,
            LAG(gamehits, 4) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l4,
            LAG(gamehits, 5) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l5,
            LAG(gamehits, 6) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l6,
            LAG(gamehits, 7) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l7,
            LAG(gamehits, 8) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l8,
            LAG(gamehits, 9) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l9,
            LAG(gamehits, 10) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l0
FROM
       game_matchup_level_2016
        WHERE
            bat_hand_cd='R'
            AND PIT_HAND_CD='R'
    ) rr
        ON g.bat_id=rr.bat_id
        AND g.GAME_DATE=rr.GAME_DATE
        AND g.PIT_HAND_CD=rr.PIT_HAND_CD
WHERE
    g.bat_hand_cd='R'
    AND g.PIT_HAND_CD='R'
ORDER BY
    g.bat_id,
    g.GAME_DATE;



SELECT bat_id, GAME_DATE, gamehits,bat_hand_cd, PIT_HAND_CD
, LAG(gamehits, 1) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l1
, LAG(gamehits, 2) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l2
, LAG(gamehits, 3) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l3
, LAG(gamehits, 4) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l4
, LAG(gamehits, 5) OVER (ORDER BY bat_id, GAME_DATE) AS gamehits_l5
FROM RR_matchup_2016
ORDER BY bat_id, GAME_DATE

SELECT bat_id, GAME_DATE, PIT_HAND_CD, gamehits,
SUM(LAG(gamehits, 2)) OVER (PARTITION BY bat_id, PIT_HAND_CD ORDER BY gamehits) AS rollinghits_2
FROM game_matchup_level_2016
GROUP BY bat_id, GAME_DATE, PIT_HAND_CD, gamehits
ORDER BY bat_id, GAME_DATE, PIT_HAND_CD



SELECT YEAR_ID, GAME_ID, GAME_DATE, bat_id, PIT_HAND_CD, gamehits,
       SUM(gamehits) OVER (PARTITION BY GAME_DATE, bat_id, PIT_HAND_CD ORDER BY bat_id, GAME_DATE, PIT_HAND_CD DESC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_sum
FROM game_matchup_level_2016
ORDER BY bat_id, GAME_DATE, PIT_HAND_CD



SELECT YEAR_ID, GAME_ID, BAT_HAND_CD, PIT_HAND_CD, bat_id, PIT_ID, event_cd
FROM EVENTS
WHERE year_id='2016' AND game_id='ANA201604040' AND bat_id='calhk001' AND pit_id='arrij001'
;

SELECT *
FROM at_bat_level_2016
WHERE year_id='2016' AND game_id='ANA201604040' AND bat_id='calhk001' AND pit_id='arrij001'
;

