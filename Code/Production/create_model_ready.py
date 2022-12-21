import pandas as pd
import requests
import sqlalchemy
from bs4 import BeautifulSoup


class Database:
    def __init__(self, uid, pwd, host, db):
        self.uid = uid
        self.pwd = pwd
        self.host = host
        self.db = db

    def db_connect(self):
        """
        Create a connection to a MySQL database using sqlalchemy.
        """
        engine = sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(self.uid, self.pwd, self.host, self.db))
        return engine

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


class Weather(Database):
    def __init__(self, uid, pwd, host, db):
        super().__init__(uid, pwd, host, db)
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

    def get_lineups(self, url, date):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

        """
            Scrape the lineup information for a given date from the baseballpress.com website.

            Parameters:
            url (str): The url of the webpage to be scraped.
            date (str): The date of the game for which the lineup is being scraped.

            Returns:
            tuple: A tuple containing two pandas dataframes, one for the batters and one for the pitchers.
            """

        res = requests.get(url, headers=headers)

        soup = BeautifulSoup(res.text, 'html.parser')

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

        batters['TEAM'] = [TEAM for TEAM in TEAMS for i in range(9)]
        batters['BAT_LINEUP_ID'] = batters['player_long'].str.extract(r'(\d)')
        batters['POSITION'] = batters['player_long'].str.extract(r'\b(CF|RF|LF|1B|2B|3B|SS|C|DH)\b')
        batters['BAT_HAND_CD'] = batters['player_long'].str.extract(r'\b(R|L|S)\b')

        self.position_mapping = {
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

        batters.fillna(0, inplace=True)
        # Create a new column called 'BAT_FLD_CD' that maps the positions to the corresponding numbers
        batters['BAT_FLD_CD'] = [position_mapping[POSITION] for POSITION in batters['POSITION']]

        AWAY_HOME = [item for sublist in [AWAY_HOME] * (len(batters) // len(AWAY_HOME) + 1) for item in sublist]
        batters['AWAY_HOME_ID'] = [AWAY_HOME[i // 9] for i in range(len(batters))]
        AWAY_HOME = ["AWAY_TEAM_ID", "HOME_TEAM_ID"]
        AWAY_HOME = [item for sublist in [AWAY_HOME] * (len(pitchers) // len(AWAY_HOME) + 1) for item in sublist]
        pitchers['AWAY_HOME_ID'] = [AWAY_HOME[i // 1] for i in range(len(pitchers))]

        batters.rename(columns={'players': 'BAT_ID'}, inplace=True)
        pitchers.rename(columns={'players': 'PIT_ID'}, inplace=True)

        return batters, pitchers
