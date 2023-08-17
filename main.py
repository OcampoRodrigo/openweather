from datetime import datetime
import requests
import pandas as pd
from pandas import json_normalize
import json
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine 
from config import Config

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

load_dotenv()
api_key = os.getenv("API_KEY")

os.makedirs("data_analytics/openweather", exist_ok = True)


# Función para obtener los datos de clima de una ciudad y convertirlos a CSV
def get_weather_data(city, coordinates):
    params = {
        "q": city,
        "appid": api_key  
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        df = json_normalize(data)
        df_weather = json_normalize(data, record_path="weather")
        df = df.drop(columns=["weather"])
        df = pd.concat([df,df_weather], axis=1)
        date_str = datetime.utcfromtimestamp(data["dt"]).strftime('%Y%m%d')
        file_path = f"data_analytics/openweather/tiempodiario_{date_str}.csv"
        df.to_csv(file_path, index=False, mode="a")
        print(f"Datos de clima de {city} almacenados en {file_path}.")
    else:
        print(f"Error al obtener los datos de clima de {city}.")


# Lista de ciudades y coordenadas correspondientes
cityList = ["London", "New York", "Cordoba", "Taipei", "Buenos Aires", "Mexico city", "Dublin", "Tiflis", "Bogota", "Tokio"]
coordList = ["lat=31&lon=64", "lat=40&lon=-73", "lat=-31&lon=-64", "lat=25&lon=64", "lat=-34&lon=-58", "lat=19&lon=-99", "lat=53&lon=6", 
             "lat=41&lon=44", "lat=4&lon=74", "lat=35&lon=139"]

#Obtenemos los datos para cada ciudad y los guardamos en csv:
for city, coord in zip(cityList,coordList):
    get_weather_data(city, coord)

#

# Leer las credenciales de conexión a la base de datos desde el archivo config.py
    config = Config()

# Establecer la conexión a la base de datos PostgreSQL utilizando SQLalchemy
#engine = create_engine(config.SQLALCHEMY_DATABASE_URI)

# Función para cargar datos de CSV a la base de datos
def load_data_to_database(csv_path):
    df = pd.read_csv(csv_path)
    table_name = "weather_data"
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Datos cargados en la tabla '{table_name}'.")

# Ruta de los archivos CSV generados previamente
csv_paths = []
#csv_paths.append(file_path)
# Cargar datos de todos los archivos CSV a la base de datos
#for csv_path in csv_paths:
#    load_data_to_database(csv_path)