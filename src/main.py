from lib import DatabaseConnector
import requests
import pandas as pd
import logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from datetime import datetime, timedelta

def iterate_dates(start_date_str, end_date_str, date_format="%Y-%m-%d"):
    start_date = datetime.strptime(start_date_str, date_format).date()
    end_date = datetime.strptime(end_date_str, date_format).date()
    
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)

start = '2025-05-28'
end = '2025-07-01'
for dt in iterate_dates(start, end):
    print(dt)
    doris_raw_config = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "raw",
        "port": 9030
    }
    doris_raw_adapter = DatabaseConnector.MySQLAdapter(doris_raw_config)
    doris_raw_adapter.connect()


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
        api_url = (
            f"https://api.open-meteo.com/v1/forecast?latitude=2.0167&longitude=117.3000"
            f"&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta"
            f"&past_days=0&start_date={etl_date_str}&end_date={etl_date_str}"
        )
        
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

        # Prepare data for insert and DataFrame
        data = []
        for time, temp, prec in zip(times, temps, precipitation):
            data.append({
                'time': time,
                'latitude': latitude,
                'longitude': longitude,
                'generationtime_ms': generationtime_ms,
                'utc_offset_seconds': utc_offset_seconds,
                'timezone': timezone,
                'timezone_abbreviation': timezone_abbreviation,
                'elevation': elevation,
                'temperature_2m_mean': temp,
                'precipitation_sum': prec
            })

        # Insert into Doris
        insert_stmt = (
            "INSERT INTO raw.weather (time, latitude, longitude, generationtime_ms, utc_offset_seconds, "
            "timezone, timezone_abbreviation, elevation, temperature_2m_mean, precipitation_sum) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        insert_data = [
            (row['time'], row['latitude'], row['longitude'], row['generationtime_ms'],
            row['utc_offset_seconds'], row['timezone'], row['timezone_abbreviation'],
            row['elevation'], row['temperature_2m_mean'], row['precipitation_sum']) for row in data
        ]
        result = doris.execute_many(insert_stmt, insert_data)
        
        df = pd.DataFrame(data)
        return df, result

    def insert_equipment_to_doris(df, dt, doris):
        df_filtered = df[df['timestamp'].dt.date == pd.to_datetime(dt).date()]
        cols = df_filtered.columns.tolist()
        placeholders = ','.join(['%s'] * len(cols))
        insert_stmt = f"INSERT INTO equipment_sensors ({','.join(cols)}) VALUES ({placeholders})"

        data = [tuple(row) for row in df_filtered.itertuples(index=False, name=None)]

        result = doris.execute_many(insert_stmt, data)
        return result

    def get_todays_production_logs(mysql_connector, dt):
        query = f"SELECT * FROM production_logs WHERE date = '{dt}'"
        
        results = mysql_connector.execute_query(query)
        if results:
            df = pd.DataFrame(results)
        else:
            columns = ['log_id', 'date', 'mine_id', 'shift', 'tons_extracted', 'quality_grade']
            df = pd.DataFrame(columns=columns)
        return df

    def insert_production_logs_to_doris(df, doris_raw_adapter):
        if df.empty:
            print("No production logs to insert.")
            return 0

        insert_stmt = (
            "INSERT INTO production_logs (log_id, date, mine_id, shift, tons_extracted, quality_grade) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        
        # Convert dataframe rows to list of tuples for batch insert
        data_to_insert = [
            (
                row['log_id'], row['date'], row['mine_id'], 
                row['shift'], row['tons_extracted'], row['quality_grade']
            )
            for _, row in df.iterrows()
        ]
        
        result = doris_raw_adapter.execute_many(insert_stmt, data_to_insert)
        return result

    equipment_df = pd.read_csv('./data/input/equipment_sensors.csv')


    equipment_df['timestamp'] = pd.to_datetime(equipment_df['timestamp'])

    result = insert_equipment_to_doris(equipment_df, dt, doris_raw_adapter)
    print("Equipment insert result:", result)

    df_weather = {}
    try:
        df_weather, result = etl_weather_to_doris(dt, doris_raw_adapter)
        print("Weather ETL Success:", result)
    except Exception as e:
        print("Weather ETL Failed:", str(e))

    prod_log = get_todays_production_logs(mysql_adapter, dt)

    insert_production_logs_to_doris(prod_log, doris_raw_adapter)

    doris_raw_adapter.close()
    mysql_adapter.close()


    doris_raw_config.execute_query(f"""
    insert into transform.equipment_utilization_daily
    SELECT 
        DATE(timestamp) AS usage_date,
        equipment_id,
        ROUND(
        SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
        ) AS utilization_percentage,
        SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) active_count,
        SUM(CASE WHEN status = 'idle' THEN 1 ELSE 0 END) idle_count,
        SUM(CASE WHEN status = 'maintenance' THEN 1 ELSE 0 END) maintenance_count,
        sum(fuel_consumption ) fuel_consumption
    FROM 
        raw.equipment_sensors
    where date(timestamp) = '{dt}'
    GROUP BY 
        DATE(timestamp),
        equipment_id
    ORDER BY 
        usage_date,
        equipment_id;
    """)


    doris_raw_config.execute_query(f"""
    insert into transform.production_summary_daily 
    SELECT
        date,
        pl.mine_id,
        m.mine_name,
        m.location,
        SUM(tons_extracted) AS tons_extracted,
        SUM(quality_grade * tons_extracted) / SUM(tons_extracted) AS quality_grade
    FROM raw.production_logs pl
    LEFT JOIN raw.mines m ON pl.mine_id = m.mine_id
    where date = '{dt}'
    GROUP BY date, pl.mine_id, m.mine_name, m.location;
    """)

    doris_raw_config.execute_query(f"""
    insert into data_mart.production_weather_summary_daily 
    select
        pl.date,
        sum(e.fuel_consumption) fuel_consumption,
        sum(pl.tons_extracted) tons_extracted,
        sum(e.fuel_consumption)/sum(pl.tons_extracted) fuel_efficiency,
        sum(precipitation_sum) precipitation_sum,
        sum(pl.tons_extracted)/sum(precipitation_sum) weather_impact
    from transform.production_summary_daily pl 
    left join `transform`.equipment_utilization_daily e on pl.date = e.usage_date 
    left join raw.weather w on w.time =  pl.date
    where date = '{dt}'
    GROUP BY pl.date;
    """)



