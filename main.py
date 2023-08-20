import pandas as pd
from weather_functions import get_weather_data, load_data_to_database,load_dotenv
import os
from sqlalchemy import create_engine 
from config import Config

# Lista de ciudades y coordenadas correspondientes
cityList = ["London", "New York", "Cordoba", "Taipei", "Buenos Aires", "Mexico city", "Dublin", "Tiflis", "Bogota", "Tokio"]
coordList = ["lat=31&lon=64", "lat=40&lon=-73", "lat=-31&lon=-64", "lat=25&lon=64", "lat=-34&lon=-58", "lat=19&lon=-99", "lat=53&lon=6", 
             "lat=41&lon=44", "lat=4&lon=74", "lat=35&lon=139"]
#Directorio donde se guardaran los archivos
directory_path = "data_analytics/openweather"
#Obtenemos los datos para cada ciudad y los guardamos en csv:
for city, coord in zip(cityList,coordList):
    get_weather_data(city, coord)
# Ruta de los archivos CSV generados previamente
csv_paths = []
for filename in os.listdir(directory_path):
    # Check if the item is a file (not a directory)
    if os.path.isfile(os.path.join(directory_path, filename)):
        if filename not in csv_paths:
            # Add the full path of the file to the list
            csv_paths.append(os.path.join(directory_path, filename)) 
# Cargar datos de todos los archivos CSV a la base de datos
for csv_path in csv_paths:
    load_data_to_database(csv_path)
    print("load succesful")
    