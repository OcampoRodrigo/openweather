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
        "appid": "7d562f62c8e7c442135ef6e459443cee"  # Agregar tu API Key de OpenWeatherMap aquí
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        global headers_written, key_columns
        data = response.json()
        df = json_normalize(data)
        df_weather = json_normalize(data, record_path="weather")        
        df = df.drop(columns=["weather"])                
        date_str = datetime.utcfromtimestamp(data["dt"]).strftime('%Y%m%d')
        file_path = f"data_analytics/openweather/tiempodiario_{date_str}.csv"                
        if not headers_written:                            
            df = pd.concat([df_weather,df], axis=1)                    
            df.to_csv(file_path, index=False, mode="a")
            headers_written = True          
            #Guardamos las columnas de la 1er respuesta,
            # ya que hay variaciones en la respuesta de la API
            key_columns = df.columns.tolist()      
            print(key_columns)    
            print(f"Datos de clima de {city} almacenados en {file_path}. Con headers")
          
        else:                    
            # Si no esta dentro de las columnas de la 1er respuesta, dropeamos columnas extra
            # para tener consistencia en el archivo csv          
            columns_to_drop = [col for col in df.columns if col not in key_columns]
            print(columns_to_drop)
            df = df.drop(columns=columns_to_drop)          
            df = pd.concat([df_weather,df], axis=1)        
            df.to_csv(file_path, index=False, mode="a", header=False)
            print(f"Datos de clima de {city} almacenados en {file_path}.")
            print(df.shape)
    else:
        print(f"Error al obtener los datos de clima de {city}.", response.status_code)


# Lista de ciudades y coordenadas correspondientes
cityList = ["London", "New York", "Cordoba", "Taipei", "Buenos Aires", "Mexico city", "Dublin", "Tiflis", "Bogota", "Tokio"]
coordList = ["lat=31&lon=64", "lat=40&lon=-73", "lat=-31&lon=-64", "lat=25&lon=64", "lat=-34&lon=-58", "lat=19&lon=-99", "lat=53&lon=6", 
             "lat=41&lon=44", "lat=4&lon=74", "lat=35&lon=139"]

#Obtenemos los datos para cada ciudad y los guardamos en csv:
for city, coord in zip(cityList,coordList):
    get_weather_data(city, coord)

# Leer las credenciales de conexión a la base de datos desde el archivo config.py
config = Config()

# Establecer la conexión a la base de datos PostgreSQL utilizando SQLalchemy
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)

# Función para cargar datos de CSV a la base de datos
def load_data_to_database(csv_path):
    column_mapping = {
        'name': 'city',
        'main.temp': 'temperature',
        'main.humidity': 'humidity',
        'wind.speed': 'wind_speed',
        'dt': 'date'
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

# Ruta de los archivos CSV generados previamente
csv_paths = []
for filename in os.listdir(directory_path):
    # Check if the item is a file (not a directory)
    if os.path.isfile(os.path.join(directory_path, filename)):
        # Add the full path of the file to the list
        csv_paths.append(os.path.join(directory_path, filename))

# Cargar datos de todos los archivos CSV a la base de datos
for csv_path in csv_paths:
    load_data_to_database(csv_path)
    print("load succesful")
    