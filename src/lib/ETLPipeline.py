import logging
import requests
import pandas as pd

class ETLPipeline:
    def __init__(self, dt, doris_adapter, mysql_adapter):
        self.dt = dt
        self.doris = doris_adapter
        self.mysql = mysql_adapter

    def run(self):
        logging.info(f"Starting ETL for {self.dt}")
        self._load_equipment_data()
        self._load_weather_data()
        self._load_production_logs()
        self._transform_to_aggregate()
        self._build_data_mart()
        self.cleanup()
        logging.info("ETL complete.")

    def _load_equipment_data(self):
        df = pd.read_csv('../data/input/equipment_sensors.csv')
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df = df[df['timestamp'].dt.date == pd.to_datetime(self.dt).date()]
        expected_ids = ['TR001', 'TR002', 'TR003', 'TR004', 'TR005']

        date_base = pd.to_datetime(self.dt).date()
        all_hours = pd.date_range(
            start=pd.Timestamp(date_base),
            end=pd.Timestamp(date_base) + pd.Timedelta(hours=23),
            freq='H'
        )
        
        full_records = []
        for hour in all_hours:
            mask = (df['timestamp'] >= hour) & (df['timestamp'] < hour + pd.Timedelta(hours=1))
            hour_df = df[mask]

            if hour_df.empty:
                for eid in expected_ids:
                    full_records.append({
                        'timestamp': hour,
                        'equipment_id': eid,
                        'status': 'unknown',
                        'fuel_consumption': 0
                    })
            else:
                for eid in expected_ids:
                    matched = hour_df[hour_df['equipment_id'] == eid]
                    if matched.empty:
                        full_records.append({
                            'timestamp': hour_df['timestamp'].iloc[0],  
                            'equipment_id': eid,
                            'status': 'unknown',
                            'fuel_consumption': 0
                        })
                    else:
                        full_records.append(matched.iloc[0].to_dict())

        final_df = pd.DataFrame(full_records)

        cols = final_df.columns.tolist()
        placeholders = ','.join(['%s'] * len(cols))
        insert_stmt = f"INSERT INTO raw.equipment_sensors ({','.join(cols)}) VALUES ({placeholders})"
        data = [tuple(row) for row in final_df.itertuples(index=False, name=None)]
        self.doris.execute_many(insert_stmt, data)

    def _load_weather_data(self):
        try:
            json_data = self._fetch_weather_json()
            df = self._transform_weather_json(json_data)
            print(df)
            self._insert_weather(df)
        except Exception as e:
            logging.error(f"Failed to load weather data: {e}")

    def _fetch_weather_json(self):
        url = (
            f"https://api.open-meteo.com/v1/forecast?latitude=2.0167&longitude=117.3000"
            f"&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta"
            f"&start_date={self.dt}&end_date={self.dt}"
        )
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def _transform_weather_json(self, json_data):
        daily = json_data.get("daily", {})
        if not daily or not daily.get("time"):
            raise ValueError("No weather data found")

        common = {
            "latitude": json_data["latitude"],
            "longitude": json_data["longitude"],
            "generationtime_ms": json_data["generationtime_ms"],
            "utc_offset_seconds": json_data["utc_offset_seconds"],
            "timezone": json_data["timezone"],
            "timezone_abbreviation": json_data["timezone_abbreviation"],
            "elevation": json_data["elevation"],
        }

        return pd.DataFrame([
            {**common, "time": time, "temperature_2m_mean": temp, "precipitation_sum": prec}
            for time, temp, prec in zip(
                daily["time"],
                daily["temperature_2m_mean"],
                daily["precipitation_sum"]
            )
        ])
    def _insert_weather(self, df):
        stmt = """
            INSERT INTO raw.weather (
                latitude, longitude, generationtime_ms, utc_offset_seconds,
                timezone, timezone_abbreviation, elevation, time, temperature_2m_mean, precipitation_sum
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        print(values)
        print(stmt)
        self.doris.execute_many(stmt, values)

    def _load_production_logs(self):
        query = f"SELECT * FROM production_logs WHERE date = '{self.dt}'"
        results = self.mysql.execute_query(query)
        if not results:
            logging.info("No production logs found.")
            return
        df = pd.DataFrame(results)
        df['tons_extracted'] = df['tons_extracted'].apply(lambda x: max(x, 0))
        stmt = """
            INSERT INTO raw.production_logs (
                log_id, date, mine_id, shift, tons_extracted, quality_grade
            ) VALUES (%s, %s, %s, %s, %s, %s);
        """
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]
        self.doris.execute_many(stmt, data)

    def _transform_to_aggregate(self):
        self.doris.execute_query(f"""
            INSERT INTO transform.equipment_utilization_daily
            SELECT 
                DATE(timestamp) AS usage_date,
                equipment_id,
                ROUND(SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
                SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'idle' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'maintenance' THEN 1 ELSE 0 END),
                SUM(fuel_consumption)
            FROM raw.equipment_sensors
            WHERE DATE(timestamp) = '{self.dt}'
            GROUP BY DATE(timestamp), equipment_id;
        """)

        self.doris.execute_query(f"""
            INSERT INTO transform.production_summary_daily
            SELECT
                pl.date,
                pl.mine_id,
                m.mine_name,
                m.location,
                SUM(tons_extracted ),
                SUM(quality_grade * tons_extracted ) / SUM(tons_extracted)
            FROM raw.production_logs pl
            LEFT JOIN raw.mines m ON pl.mine_id = m.mine_id
            WHERE pl.date = '{self.dt}'
            GROUP BY pl.date, pl.mine_id, m.mine_name, m.location;
        """)

    def _build_data_mart(self):
        self.doris.execute_query(f"""
            INSERT INTO data_mart.production_weather_summary_daily
            SELECT
                pl.date,
                SUM(e.fuel_consumption),
                SUM(pl.tons_extracted),
                SUM(e.fuel_consumption) / SUM(pl.tons_extracted),
                SUM(w.precipitation_sum),
                SUM(pl.tons_extracted) / SUM(w.precipitation_sum)
            FROM transform.production_summary_daily pl
            LEFT JOIN transform.equipment_utilization_daily e ON pl.date = e.usage_date
            LEFT JOIN raw.weather w ON w.time = pl.date
            WHERE pl.date = '{self.dt}'
            GROUP BY pl.date;
        """)

    def cleanup(self):
        self.mysql.close()
        self.doris.close()