from lib import DatabaseConnector
import requests
import pandas as pd


doris_config = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "raw",
    "port": 9030
}
doris_adapter = DatabaseConnector.MySQLAdapter(doris_config)
doris_adapter.connect()


mysql_config = {
    "host": "localhost",
    "user": "etluser",
    "password": "etlpass",
    "database": "coal_mining",
    "port": 3306
}
mysql_adapter = DatabaseConnector.MySQLAdapter(mysql_config)
mysql_adapter.connect()


def etl_weather_to_doris(etl_date_str, doris):
    api_url = f"https://api.open-meteo.com/v1/forecast?latitude=2.0167&longitude=117.3000&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta&past_days=0&start_date={etl_date_str}&end_date={etl_date_str}"
    
    response = requests.get(api_url)
    response.raise_for_status()
    data_json = response.json()

    latitude = data_json.get('latitude')
    longitude = data_json.get('longitude')
    generationtime_ms = data_json.get('generationtime_ms')
    utc_offset_seconds = data_json.get('utc_offset_seconds')
    timezone = data_json.get('timezone')
    timezone_abbreviation = data_json.get('timezone_abbreviation')
    elevation = data_json.get('elevation')

    daily = data_json.get('daily', {})
    if not daily or not daily.get('time'):
        raise ValueError(f"No daily data found for date {etl_date_str}")

    times = daily.get('time', [])
    temps = daily.get('temperature_2m_mean', [])
    precipitation = daily.get('precipitation_sum', [])

    data = []
    for time, temp, prec in zip(times, temps, precipitation):
        data.append((time, latitude, longitude, generationtime_ms, utc_offset_seconds, timezone, timezone_abbreviation, elevation, temp, prec))

    insert_stmt = (
        "INSERT INTO raw.weather (time, latitude, longitude, generationtime_ms, utc_offset_seconds, "
        "timezone, timezone_abbreviation, elevation, temperature_2m_mean, precipitation_sum) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    result = doris.execute_many(insert_stmt, data)

    return result

def insert_equipment_to_doris(df, dt, doris):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(df.head())

    df_filtered = df[df['timestamp'] == dt]
    print(df_filtered)
    cols = df_filtered.columns.tolist()
    placeholders = ','.join(['%s'] * len(cols))
    insert_stmt = f"INSERT INTO equipment_sensors ({','.join(cols)}) VALUES ({placeholders})"

    data = [tuple(row) for row in df_filtered.itertuples(index=False, name=None)]

    result = doris.execute_many(insert_stmt, data)
    return result

equipment_df = pd.read_csv('./data/input/equipment_sensors.csv')
print("success read")

result = insert_equipment_to_doris(equipment_df, '2024-07-01', doris_adapter)
print("Equipment insert result:", result)

try:
    result = etl_weather_to_doris('2024-07-01', doris_adapter)
    print("Weather ETL Success:", result)
except Exception as e:
    print("Weather ETL Failed:", str(e))

doris_adapter.close()
mysql_adapter.close()