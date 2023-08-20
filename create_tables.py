import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from config import Config  # Import your database configuration
import os
# Define your SQLAlchemy base class
Base = declarative_base()
# Define your new table class with the desired subset of columns
class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True)
    city = Column(String)
    temperature = Column(Float)
    humidity = Column(Float)
    wind_speed = Column(Float)
    date = Column(DateTime)
    main_weather = Column(String)
    weather_description = Column(String)
    weather_icon = Column(String) 

# Establecer la conexion a la base de datos
config = Config()
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)
# Crear la nueva tabla
Base.metadata.create_all(engine)

## Commit the changes to the database session
Session = sessionmaker(bind=engine)
session = Session()
session.commit()



 