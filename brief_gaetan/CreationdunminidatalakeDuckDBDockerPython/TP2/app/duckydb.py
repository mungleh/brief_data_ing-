import requests
import pandas as pd
import duckdb as dd
from datetime import datetime


con = dd.connect("../outputs/weather.db")
API_KEY = "5ff3a6fbcdbfaeb35aba10dcab3e7496"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"


def logging(log):
    with open('../outputs/TP2_logging.txt', 'a') as f:
        f.write(str(log) + "\n")

def get_weather_data(city):
    params = {"q": city + ",FR", "appid": API_KEY, "units": "metric"}
    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        data = response.json()
        return {
            "city": data["name"],
            "description": data["weather"][0]["description"],
            "temperature": data["main"]["temp"],
            "latitude": data["coord"]["lat"],
            "longitude": data["coord"]["lon"],
        }
    else:
        logging(f"Error fetching data for {city}:", response.json() + "\n")
        return None


french_cities = [
    "Paris",
    "Marseille",
    "Lyon",
    "Toulouse",
    "Nice",
    "Nantes",
    "Strasbourg",
    "Montpellier",
    "Bordeaux",
    "Lille",
    "Rennes",
    "Reims",
    "Le Havre",
    "Saint-Étienne",
    "Toulon",
    "Strasbourg",
    "Huez",
    "Pouilly-le-Monial",
]

logging("lancement du job:" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

weather_data = [
    get_weather_data(city) for city in french_cities if get_weather_data(city)
]

df = pd.DataFrame(weather_data)


# GROOOOOS NNETTOYAAAGE ICIIIII WOOOOOW
try:
    df['city'] = df['city'].str.replace('Arrondissement de ', '')
except Exception as e:
    logging(f"Erreur pendant le nettoyage: {e}")

logging("\"Arrondissement de\" dans les noms de ville supprimé")


#insertion duckduckgo
con.sql(f'''
    CREATE TABLE if not exists weather AS
        SELECT * FROM df;
''')

logging(con.sql(f'''
    SELECT * FROM df;
'''))

con.sql('''
    COPY weather TO '../outputs/weather.parquet' (FORMAT 'parquet');
''')
