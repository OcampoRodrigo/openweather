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

#Variable para trackear los headers(nombre de las columnas) en el archivo csv y cargarlos solo una vez
headers_written = False
key_columns = []
#Creamos el directorio para guardar los archivos csv
directory_path = "data_analytics/openweather"
os.makedirs(directory_path, exist_ok = True)
# Función para obtener los datos de clima de una ciudad y convertirlos a CSV
def get_weather_data(city, coordinates):
    params = {
        "q": city,
        "appid": "7d562f62c8e7c442135ef6e459443cee",  # Agregar tu API Key de OpenWeatherMap aquí
        "units": "metric" # Para obtener la temperatura en Celsius
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        global headers_written, key_columns
        data = response.json()
        df = json_normalize(data)            
        df_weather = json_normalize(data, record_path=["weather"][0])      
        df = df.drop(columns=["weather"])                       
        date_str = datetime.utcfromtimestamp(data["dt"]).strftime('%Y%m%d')
        file_path = f"data_analytics/openweather/tiempodiario_{date_str}.csv"
        if os.path.exists(file_path):
            headers_written = True                
        if not headers_written:                            
            df = pd.concat([df_weather,df], axis=1) 
            print(df.columns)
            key_columns = ["name","main.temp","main.humidity","wind.speed","dt","main", "description", "icon"]
            df = df[key_columns]                 
            df.to_csv(file_path, index=False, mode="a")
            headers_written = True                          
            print(f"Datos de clima de {city} almacenados en {file_path}. Con headers")          
        else:                                   
            df = pd.concat([df_weather,df], axis=1)   
            key_columns = ["name","main.temp","main.humidity","wind.speed","dt","main", "description", "icon"] 
            df = df[key_columns]
            df.to_csv(file_path, index=False, mode="a", header=False)
            print(f"Datos de clima de {city} almacenados en {file_path}.")            
    else:
        print(f"Error al obtener los datos de clima de {city}.", response.status_code)
# Función para cargar datos de CSV a la base de datos
def load_data_to_database(csv_path):
    # Leer las credenciales de conexión a la base de datos desde el archivo config.py
    config = Config()
    # Establecer la conexión a la base de datos PostgreSQL utilizando SQLalchemy
    engine = create_engine(config.SQLALCHEMY_DATABASE_URI)
    column_mapping = {
        'name': 'city',
        'main.temp': 'temperature',
        'main.humidity': 'humidity',
        'wind.speed': 'wind_speed',
        'dt': 'date',
        'main' : 'main_weather',
        'description' : 'weather_description',
        'icon' : 'weather_icon'
    }
    subset_columns = list(column_mapping.keys())    
    df = pd.read_csv(csv_path, usecols=subset_columns)    
     # Convert Unix timestamp to pandas datetime object
    df['dt'] = pd.to_datetime(df['dt'], unit='s', utc=True)
    # Rename the columns using the mapping
    df.rename(columns=column_mapping, inplace=True)         
    table_name = "weather_data"
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Datos cargados en la tabla '{table_name}'.")