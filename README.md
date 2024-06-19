# openweather
## Final project: Specialization in data analytics
This application was developed as the final project for data analytics for the [INFORMATORIO](https://empleo.chaco.gob.ar/informatorio#/) program.
It involves 4 scripts:
- **weather_functions.py**: Two functions are defined:
 --get_weather_data(city, coordinates): Receives a city and its cordinates as parameters, and makes a get request to
the [Current weather data API from Openweather](https://openweathermap.org/current). Since the response is a JSON, we manipulate the data using Pandas, and save the relevant
information for our project in a csv. 🔍💻
 --load_data_to_database(csv_path): Receives a csv path as a parameter and stablishes a connection to the database and loads the data in the csv into the corresponding table in the
 database. 💾
- **create_tables** : Stablishes a connection to the database, and then defines and creates all the tables  that will be used to store the information.
- **main_py**: Uses the functiones defined above to save the current weather into the database for a pre-defined list of cities.
- **app.py**: Handles all the routes in the application.


