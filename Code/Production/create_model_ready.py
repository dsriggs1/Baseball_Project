import datetime as datetime

import pandas as pd
import polars as pl
import requests
import sqlalchemy
from bs4 import BeautifulSoup
from pybaseball import playerid_reverse_lookup
from pybaseball import statcast


class Database:
    def __init__(self, uid, pwd, host, db, port):
        self.uid = uid
        self.pwd = pwd
        self.host = host
        self.db = db
        self.port = port

    def db_connect(self):
        """
        Create a connection to a MySQL database using sqlalchemy.
        """
        engine = sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(self.uid, self.pwd, self.host, self.db))
        return engine

    def db_connect_polars(self):
        """
        Create a connection to a MySQL database to pull data for a Polars DataFrame.
        """
        uri = ('mysql://{}:{}@{}:{}/{}'.format(self.uid, self.pwd, self.host, self.port, self.db))
        return uri

    def db_insert(self, df, table_name, engine):
        """
        Insert a dataframe into a MySQL database.
        Args:
            df (pd.DataFrame): The dataframe to insert into the database.
            table_name (str): The name of the table to insert the data into.
            engine (sqlalchemy.engine.base.Engine): The sqlalchemy engine object to use to connect to the database.
        """
        df.to_sql(table_name, engine, if_exists='append',
                  index=False)  # if_exists='append' means that if the table already exists, the data will be appended to it

    def db_delete_rows(self, table_name, engine, where_clause):
        """
            Delete rows from the specified table that match the given WHERE clause.

            Args:
                table_name (str): The name of the table to delete rows from.
                engine (Engine): The SQLAlchemy engine to use for connecting to the database.
                where_clause (str): The WHERE clause to use when deleting rows.

            Returns:
                None
            """
        with engine.connect() as con:
            con.execute(f'DELETE FROM {table_name} WHERE {where_clause}')

    def db_pull(self, query, uri):
        """
        Pull data from a MySQL database.
        Args:
            query (str): SQL query to define what data to populate the polars df with.
            uri (str): The uri object to use to connect to the database.

        Returns:
            Data from mysql table as a polars dataframe.
        """
        df = pl.read_sql(query, uri)
        return df


class WebScrape(Database):
    def __init__(self, uid, pwd, host, db, port):
        super().__init__(uid, pwd, host, db, port)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

    def get_weather(self, city):
        """
        This function takes a city name as input and returns a dataframe containing the weather information for that city.

        city: str
            The name of the city for which to get weather information.

        returns: pd.DataFrame
            A dataframe containing the weather information for the specified city.
        """

        if not isinstance(city, str):
            raise TypeError("'city' must be a string")

        city = city + " weather"
        res = requests.get(
            f'https://www.google.com/search?q={city}&oq={city}&aqs=chrome.0.35i39l2j0l4j46j69i60.6128j1j7&sourceid=chrome&ie=UTF-8',
            headers=self.headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        Location = soup.select('#wob_loc')[0].getText()
        TIME = soup.select('#wob_dts')[0].getText()
        WEATHER_PARK_CD = soup.select('#wob_dc')[0].getText()
        TEMP_PARK_CT = soup.select('#wob_tm')[0].getText()
        WIND_SPEED_PARK_CT = soup.select('#wob_ws')[0].getText().strip()
        HUMIDITY_PARK_CT = soup.select('#wob_hm')[0].getText().strip()
        df = pd.DataFrame(
            {'Location': Location, 'TIME': TIME, 'WEATHER_PARK_CD': WEATHER_PARK_CD, 'TEMP_PARK_CT': TEMP_PARK_CT,
             'WIND_SPEED_PARK_CT': WIND_SPEED_PARK_CT,
             'HUMIDITY_PARK_CT': HUMIDITY_PARK_CT}, index=[0])

        df["WIND_SPEED_PARK_CT"] = df["WIND_SPEED_PARK_CT"].str.replace("mph", "")
        df["WIND_SPEED_PARK_CT"] = pd.to_numeric(df["WIND_SPEED_PARK_CT"])

        df["HUMIDITY_PARK_CT"] = df["HUMIDITY_PARK_CT"].str.replace("%", "")
        return df

    def get_lineups(self, date):

        """
            Scrape the lineup information for a given date from the baseballpress.com website.

            Parameters:
            date (str): The date of the game for which the lineup is being scraped.

            Returns:
            tuple: A tuple containing two pandas dataframes, one for the batters and one for the pitchers.
            """
        if not isinstance(date, str):
            raise TypeError("'date' must be a string in the format 'YYYYMMDD'")

        res = requests.get(f"https://www.baseballpress.com/lineups/{date}", headers=self.headers)

        soup = BeautifulSoup(res.text, 'html.parser')

        date_obj = datetime.strptime(date, '%Y-%m-%d')
        yyyymmdd = date_obj.strftime('%Y%m%d')

        player_long = soup.find_all('div', attrs={'class': 'player'})
        players = soup.find_all(class_='player-link')
        TEAMS = soup.select("a.mlb-team-logo.bc")

        player_long = list(map(lambda player: player.text, player_long))
        players = list(map(lambda player: player['data-bref'], players))
        players = pd.DataFrame(data={'players': players, 'player_long': player_long})
        batters = players[players['player_long'].str.contains(r'^\d\. ')]
        pitchers = players[~players['player_long'].str.contains(r'\d\. ')]
        pitchers['PIT_HAND_CD'] = pitchers['player_long'].str.extract(r'\b(R|L|S)\b')

        TEAMS = list(map(lambda TEAM: TEAM.text, TEAMS))
        TEAMS = list(map(lambda TEAM: TEAM.strip(), TEAMS))

        AWAY_HOME = ["AWAY_TEAM_ID", "HOME_TEAM_ID"]

        # batters = pd.DataFrame(batters, columns=['player'])
        batters['TEAM'] = [TEAM for TEAM in TEAMS for i in range(9)]
        batters['BAT_LINEUP_ID'] = batters['player_long'].str.extract(r'(\d)')
        batters['POSITION'] = batters['player_long'].str.extract(r'\b(CF|RF|LF|1B|2B|3B|SS|C|DH)\b')
        batters['BAT_HAND_CD'] = batters['player_long'].str.extract(r'\b(R|L|S)\b')

        position_mapping = {
            'P': 1,
            'C': 2,
            '1B': 3,
            '2B': 4,
            '3B': 5,
            'SS': 6,
            'LF': 7,
            'CF': 8,
            'RF': 9,
            'DH': 10,
            0: 999
        }

        mlb_teams = {
            "Angels": "ANA",
            "Astros": "HOU",
            "Athletics": "OAK",
            "Blue Jays": "TOR",
            "Braves": "ATL",
            "Brewers": "MIL",
            "Cardinals": "SLN",
            "Cubs": "CHI",
            "Diamondbacks": "ARI",
            "Dodgers": "LAN",
            "Giants": "SFN",
            "Guardians": "CLE",
            "Mariners": "SEA",
            "Marlins": "MIA",
            "Mets": "NYN",
            "Nationals": "WAS",
            "Orioles": "BAL",
            "Padres": "SDN",
            "Phillies": "PHI",
            "Pirates": "PIT",
            "Rangers": "TEX",
            "Rays": "TBA",
            "Red Sox": "BOS",
            "Reds": "CIN",
            "Rockies": "COL",
            "Royals": "KCA",
            "Tigers": "DET",
            "Twins": "MIN",
            "White Sox": "CHA",
            "Yankees": "NYA",
        }

        batters.fillna(0, inplace=True)

        batters['BAT_FLD_CD'] = [position_mapping[POSITION] for POSITION in batters['POSITION']]

        batters.rename(columns={'players': 'BAT_ID'}, inplace=True)
        pitchers.rename(columns={'players': 'PIT_ID'}, inplace=True)

        AWAY_TEAM_ID = [team for team in TEAMS[::2]]
        HOME_TEAM_ID = [team for team in TEAMS if team not in AWAY_TEAM_ID]

        batters['AWAY_TEAM_ID'] = [team for team in AWAY_TEAM_ID for i in range(18)]
        batters['HOME_TEAM_ID'] = [team for team in HOME_TEAM_ID for i in range(18)]

        pitchers['AWAY_TEAM_ID'] = [team for team in AWAY_TEAM_ID for i in range(2)]
        pitchers['HOME_TEAM_ID'] = [team for team in HOME_TEAM_ID for i in range(2)]

        batters['GAME_ID'] = [mlb_teams[team] for team in batters['HOME_TEAM_ID']]
        batters['GAME_ID'] = batters['GAME_ID'] + yyyymmdd

        pitchers['GAME_ID'] = [mlb_teams[team] for team in pitchers['HOME_TEAM_ID']]
        pitchers['GAME_ID'] = pitchers['GAME_ID'] + yyyymmdd

        batters.drop(columns=['player_long'], axis=1, inplace=True)
        pitchers.drop(columns=['player_long'], axis=1, inplace=True)

        return batters, pitchers

    def get_stats(self, date):
        """
        Use the pybaseball package to get the stats for a given date.
        :param date:
        :return stats:
        """

        if not isinstance(date, str):
            raise TypeError("'date' must be a string in the format 'YYYYMMDD'")

        date_obj = datetime.strptime(date, '%Y-%m-%d')
        yyyymmdd = date_obj.strftime('%Y%m%d')

        stats = statcast(date, date)

        stats.columns = stats.columns.str.upper()

        mapping = {'STAND': 'BAT_HAND_CD',
                   'P_THROWS': 'PIT_HAND_CD',
                   'AWAY_TEAM': 'AWAY_TEAM_ID',
                   'HOME_TEAM': 'HOME_TEAM_ID',
                   'GAME_YEAR': 'YEAR_ID'}

        stats = stats.rename(columns=mapping)

        mlb_teams = {
            "LAA": "ANA",
            "HOU": "HOU",
            "OAK": "OAK",
            "TOR": "TOR",
            "ATL": "ATL",
            "MIL": "MIL",
            "STL": "SLN",
            "CHC": "CHI",
            "AZ": "ARI",
            "LAD": "LAN",
            "SF": "SFN",
            "CLE": "CLE",
            "SEA": "SEA",
            "MIA": "MIA",
            "NYM": "NYN",
            "WSH": "WAS",
            "BAL": "BAL",
            "SD": "SDN",
            "PHI": "PHI",
            "PIT": "PIT",
            "TEX": "TEX",
            "TB": "TBA",
            "BOS": "BOS",
            "CIN": "CIN",
            "COL": "COL",
            "KC": "KCA",
            "DET": "DET",
            "MIN": "MIN",
            "CWS": "CHA",
            "NYY": "NYA",
        }

        stats['HOME_TEAM_ID'] = [mlb_teams[team] for team in stats['HOME_TEAM_ID']]
        stats['AWAY_TEAM_ID'] = [mlb_teams[team] for team in stats['AWAY_TEAM_ID']]

        stats['GAME_ID'] = stats['HOME_TEAM_ID'] + yyyymmdd

        pitcher_ids = list(stats['PITCHER'].unique())
        batter_ids = list(stats['BATTER'].unique())

        pitchers = playerid_reverse_lookup(pitcher_ids, key_type='mlbam')
        batters = playerid_reverse_lookup(batter_ids, key_type='mlbam')

        stats = pd.merge(stats, pitchers, left_on='PITCHER', right_on='key_mlbam', how='left')
        stats = pd.merge(stats, batters, left_on='BATTER', right_on='key_mlbam', how='left')

        return stats
