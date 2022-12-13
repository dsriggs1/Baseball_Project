from bs4 import BeautifulSoup
import requests
import pandas as pd
headers = {
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

cities=["Baltimore"
, "Boston"
, "Tampa Bay"
, "Toronto"
, "Cleveland"
, "Detroit"
, "Kansas City"
, "Minnesota"
, "Houston"
, "Oakland"
, "Seattle"
, "Arlington"
, "Atlanta"
, "Miami"
, "New York"
, "Philadelphia"
, "Washington"
, "Chicago"
, "Cincinnati"
, "Milwaukee"
, "Pittsburgh"
, "St. Louis "
, "Arizona"
, "Colorado"
, "Los Angeles"
, "San Diego"
, "San Francisco"]

def get_weather(city):
    """
    This function takes a city name as input and returns a dataframe containing the weather information for that city.

    city: str
        The name of the city for which to get weather information.

    returns: pd.DataFrame
        A dataframe containing the weather information for the specified city.
    """
    city = city + " weather"
    res = requests.get(
        f'https://www.google.com/search?q={city}&oq={city}&aqs=chrome.0.35i39l2j0l4j46j69i60.6128j1j7&sourceid=chrome&ie=UTF-8',
        headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser') 
    Location = soup.select('#wob_loc')[0].getText()
    TIME = soup.select('#wob_dts')[0].getText()
    WEATHER_PARK_CD = soup.select('#wob_dc')[0].getText()
    TEMP_PARK_CT = soup.select('#wob_tm')[0].getText()
    WIND_SPEED_PARK_CT = soup.select('#wob_ws')[0].getText().strip()
    HUMIDITY_PARK_CT = soup.select('#wob_hm')[0].getText().strip()
    df = pd.DataFrame({'Location': Location, 'TIME': TIME, 'WEATHER_PARK_CD': WEATHER_PARK_CD, 'TEMP_PARK_CT': TEMP_PARK_CT, 'WIND_SPEED_PARK_CT': WIND_SPEED_PARK_CT,
                       'HUMIDITY_PARK_CT': HUMIDITY_PARK_CT}, index=[0])

    df["WIND_SPEED_PARK_CT"] = df["WIND_SPEED_PARK_CT"].str.replace("mph", "")
    df["WIND_SPEED_PARK_CT"] = pd.to_numeric(df["WIND_SPEED_PARK_CT"])

    df["HUMIDITY_PARK_CT"] = df["HUMIDITY_PARK_CT"].str.replace("%", "")
    return df

df=pd.concat(map(get_weather, cities))