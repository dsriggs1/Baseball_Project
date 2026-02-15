#Batter Points

SET @single = 3;
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
	       DAYNIGHT_PARK_CD,
		SUM(CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_hits,
		SUM(CASE WHEN ab_fl= 'T' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_ab,
		SUM(CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT)/NULLIF(SUM(CASE WHEN ab_fl= 'T' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY bat_id, GAME_DATE, INN_CT), 0) AS game_ba,
		SUM(CASE WHEN EVENT_CD = '20' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_singles,
		SUM(CASE WHEN EVENT_CD = '21' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_doubles,
		SUM(CASE WHEN EVENT_CD = '22' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_triples,
		SUM(CASE WHEN EVENT_CD = '23' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_hr,
		SUM(CASE WHEN ab_fl = 'T' OR EVENT_CD IN (14, 15, 16) THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_pa,
		SUM(CASE WHEN EVENT_CD = '15' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_ibb,
		SUM(CASE WHEN EVENT_CD = '14' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_ubb,
		SUM(PA_BALL_CT) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_balls,
		SUM(PA_SWINGMISS_STRIKE_CT) OVER (PARTITION BY bat_id, GAME_ID ORDER BY GAME_DATE, bat_id, INN_CT) AS game_swing_strikes,
		SUM(CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_hits,
		SUM(CASE WHEN ab_fl= 'T' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_ab,
		SUM(CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT)/NULLIF(SUM(CASE WHEN ab_fl= 'T' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT), 0) AS season_ba,
		SUM(CASE WHEN EVENT_CD = '20' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_singles,
		SUM(CASE WHEN EVENT_CD = '21' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_doubles,
		SUM(CASE WHEN EVENT_CD = '22' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_triples,
		SUM(CASE WHEN EVENT_CD = '23' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_hr,
		SUM(CASE WHEN ab_fl = 'T' OR EVENT_CD IN (14, 15, 16) THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_pa,
		SUM(CASE WHEN EVENT_CD = '15' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_ibb,
		SUM(CASE WHEN EVENT_CD = '14' THEN 1 ELSE 0 END) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_ubb,
		SUM(PA_BALL_CT) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_balls,
		SUM(PA_SWINGMISS_STRIKE_CT) OVER (PARTITION BY bat_id ORDER BY bat_id, GAME_DATE, INN_CT) AS season_swing_strikes,	
		

	       -- Add the case expression for hit_fl
	       CASE WHEN EVENT_CD IN ("20", "21", "22", "23") THEN 1 ELSE 0 END AS hit_fl,
	       -- Add the case expression for pitchingpoints
	       CASE 
		 WHEN EVENT_CD = "3" THEN 2
		 WHEN EVENT_CD IN ("20", "21", "22", "23") THEN -0.6 #hit_fl logic
		 WHEN EVENT_CD IN ("14", "16") THEN -0.6
		 ELSE 0
	       END AS pitching_points,
	       -- Add the case expression for fantasypoints
		      CASE 
		 WHEN EVENT_CD = "23" THEN @hr
		 WHEN EVENT_CD = "20" THEN @single + RBI_CT * @rbi
		 WHEN EVENT_CD = "21" THEN @double + RBI_CT * @rbi
		 WHEN EVENT_CD IN ("14", "16") THEN @walk + RBI_CT * @rbi
		 WHEN EVENT_CD = "22" THEN @triple + RBI_CT * @rbi
		 ELSE 0
	       END AS batting_points,
	       CASE
		 WHEN BAT_HAND_CD = 'R' AND PIT_HAND_CD = 'R' THEN 1
		 WHEN BAT_HAND_CD = 'R' AND PIT_HAND_CD = 'L' THEN 2
		 WHEN BAT_HAND_CD = 'L' AND PIT_HAND_CD = 'R' THEN 3
		 ELSE 4
	       END AS MATCHUP_IN
	        
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
		 park_id,
		 DAYNIGHT_PARK_CD
	  FROM games
	) G
	ON E.GAME_ID = G.GAME_ID
	WHERE E.year_id= '2016'
;

CREATE INDEX idx_at_bat_level_2016
ON at_bat_level_2016 (seq_events, GAME_ID);

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
	       MAX(HOME_TEAM_ID  	   ) AS  HOME_TEAM_ID, 	      
	       MAX(TEMP_PARK_CT            ) AS  TEMP_PARK_CT, 
	       MAX(WIND_DIRECTION_PARK_CD  ) AS  WIND_DIRECTION_PARK_CD,
	       MAX(WIND_SPEED_PARK_CT      ) AS  WIND_SPEED_PARK_CT, 
	       MAX(PRECIP_PARK_CD          ) AS  PRECIP_PARK_CD, 
	       MAX(SKY_PARK_CD             ) AS  SKY_PARK_CD,
	       MAX(WIN_PIT_ID              ) AS  WIN_PIT_ID,
	       MAX(park_id                 ) AS  park_id,
	       MAX(DAYNIGHT_PARK_CD	   ) AS  DAYNIGHT_PARK_CD ,	
	       MAX( game_hits               ) AS  game_hits         ,
	       MAX( game_ab                 ) AS  game_ab           ,
	       MAX( game_singles            ) AS  game_singles      ,
	       MAX( game_doubles            ) AS  game_doubles      ,
	       MAX( game_triples            ) AS  game_triples      ,
	       MAX( game_hr                 ) AS  game_hr           ,
	       MAX( game_pa                 ) AS  game_pa           ,
	       MAX( game_ibb                ) AS  game_ibb          ,
	       MAX( game_ubb                ) AS  game_ubb          ,
	       MAX( game_balls              ) AS  game_balls        ,
	       MAX( game_swing_strikes       ) AS  game_swing_strikes ,
	       MAX( pitching_points         ) AS  pitching_points   ,
	       MAX( batting_points          ) AS  batting_points    
	       
	FROM at_bat_level_2016
	GROUP BY YEAR_ID, GAME_ID, GAME_DATE, BAT_HAND_CD, PIT_HAND_CD, bat_id#, PIT_ID	
	;
	
	CREATE INDEX game_matchup_level_2016
	ON game_matchup_level_2016 (YEAR_ID, GAME_ID, BAT_HAND_CD, PIT_HAND_CD, bat_id);
	
	DROP TABLE stolen_bases_2016;
	CREATE TABLE stolen_bases_2016
	SELECT GAME_ID
	      , GAME_DATE
	      , BASE_STL_ID
	      , STOLEN_BASE_FL
	      , PIT_HAND_CD
	      , SUM(STOLEN_BASE_POINTS) AS STOLEN_BASE_POINTS
	FROM (
		SELECT GAME_ID
		, GAME_DATE
		, RUN1_SB_FL
		, RUN2_SB_FL
		, RUN3_SB_FL
		, BASE1_RUN_ID
		, BASE2_RUN_ID
		, BASE3_RUN_ID
		, PIT_HAND_CD
		, event_cd
		, CASE
		  WHEN RUN1_SB_FL = 'T' AND RUN2_SB_FL = 'T' THEN BASE1_RUN_ID
		  WHEN RUN1_SB_FL = 'T' AND RUN3_SB_FL = 'T' THEN BASE1_RUN_ID
		  WHEN RUN2_SB_FL = 'T' AND RUN3_SB_FL = 'T' THEN BASE2_RUN_ID
		  WHEN RUN1_SB_FL = 'T' THEN BASE1_RUN_ID
		  WHEN RUN2_SB_FL = 'T' THEN BASE2_RUN_ID
		  ELSE BASE3_RUN_ID
		END AS BASE_STL_ID
		, 3 AS STOLEN_BASE_POINTS
		, 1 AS STOLEN_BASE_FL
		FROM EVENTS
		WHERE event_cd='4' AND year_ID='2016'
		UNION
		SELECT GAME_ID
		, GAME_DATE
		, RUN1_SB_FL
		, RUN2_SB_FL
		, RUN3_SB_FL
		, BASE1_RUN_ID
		, BASE2_RUN_ID
		, BASE3_RUN_ID
		, PIT_HAND_CD
		, event_cd
		, CASE
		  WHEN RUN1_SB_FL = 'T' AND RUN2_SB_FL = 'T' THEN BASE2_RUN_ID
		  WHEN RUN1_SB_FL = 'T' AND RUN3_SB_FL = 'T' THEN BASE3_RUN_ID
		  WHEN RUN2_SB_FL = 'T' AND RUN3_SB_FL = 'T' THEN BASE3_RUN_ID
		  WHEN RUN1_SB_FL = 'T' THEN BASE1_RUN_ID
		  WHEN RUN2_SB_FL = 'T' THEN BASE2_RUN_ID
		  ELSE BASE3_RUN_ID
		  END AS BASE_STL_ID
		, 3 AS STOLEN_BASE_POINTS
		, 1 AS STOLEN_BASE_FL
		FROM EVENTS
		WHERE event_cd='4' AND year_ID='2016'
	) AS sb
	GROUP BY 1,2,3,4,5
	;

	CREATE INDEX stolen_bases_2016
	ON stolen_bases_2016 (GAME_ID, BASE_STL_ID);
	
	
	
