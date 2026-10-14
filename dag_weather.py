"""
DAG: Cuaca Jakarta (Open-Meteo API) - Tabel PENDUKUNG
Tidak perlu API key.
"""
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import mysql.connector

JAKARTA_LAT = -6.2088
JAKARTA_LON = 106.8456
DAYS_BACK = 30

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='etl_weather_jakarta',
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=['tugas', 'mysql', 'etl', 'weather', 'sdg', 'api'],
) as dag:

    create_table = MySqlOperator(
        task_id='create_table_weather',
        conn_id='mysql-local',
        sql="""
        CREATE TABLE IF NOT EXISTS weather_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            latitude DECIMAL(10,6),
            longitude DECIMAL(10,6),
            temperature_c DECIMAL(5,2),
            humidity_pct DECIMAL(5,2),
            wind_speed_kmh DECIMAL(6,2),
            precipitation_mm DECIMAL(6,2),
            recorded_at DATETIME,
            UNIQUE KEY uq_weather (latitude, longitude, recorded_at)
        );
        """,
    )

    def extract_transform():
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=DAYS_BACK)

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": JAKARTA_LAT,
            "longitude": JAKARTA_LON,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            "timezone": "Asia/Jakarta",
        }
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        humidity = hourly.get("relative_humidity_2m", [])
        wind = hourly.get("wind_speed_10m", [])
        precip = hourly.get("precipitation", [])

        rows = []
        for i in range(len(times)):
            rows.append({
                "latitude": JAKARTA_LAT,
                "longitude": JAKARTA_LON,
                "temperature_c": temps[i] if i < len(temps) else None,
                "humidity_pct": humidity[i] if i < len(humidity) else None,
                "wind_speed_kmh": wind[i] if i < len(wind) else None,
                "precipitation_mm": precip[i] if i < len(precip) else None,
                "recorded_at": times[i],
            })

        df = pd.DataFrame(rows)
        df = df.dropna(subset=["recorded_at"])
        print(f"Total baris data cuaca: {len(df)}")

        transformed_path = "/usr/local/airflow/dags/data/transformed_weather.csv"
        df.to_csv(transformed_path, index=False)
        print(f"Data disimpan ke {transformed_path}")

    extract_transform_task = PythonOperator(
        task_id='extract_transform_weather',
        python_callable=extract_transform,
    )

    
    def load_to_mysql():
        mysql_conn = BaseHook.get_connection('mysql-local')
        conn = mysql.connector.connect(
            host=mysql_conn.host,
            user=mysql_conn.login,
            password=mysql_conn.password,
            database=mysql_conn.schema
        )
        cursor = conn.cursor()

        transformed_path = "/usr/local/airflow/dags/data/transformed_weather.csv"
        df = pd.read_csv(transformed_path)

        count = 0

        try:
            for _, row in df.iterrows():
                recorded_at = pd.to_datetime(row['recorded_at']).to_pydatetime()

                # Pastikan timestamp tersedia di dim_time.
                cursor.execute("""
                    INSERT INTO dim_time (time_value)
                    VALUES (%s)
                    ON DUPLICATE KEY UPDATE
                        time_id = LAST_INSERT_ID(time_id)
                """, (recorded_at,))

                time_id = cursor.lastrowid

                # Ubah nilai NaN menjadi NULL untuk MySQL.
                def clean(value):
                    return None if pd.isna(value) else value

                cursor.execute("""
                    INSERT INTO weather_data (
                        latitude, longitude, temperature_c, humidity_pct,
                        wind_speed_kmh, precipitation_mm, recorded_at, time_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        temperature_c = VALUES(temperature_c),
                        humidity_pct = VALUES(humidity_pct),
                        wind_speed_kmh = VALUES(wind_speed_kmh),
                        precipitation_mm = VALUES(precipitation_mm)
                """, (
                    clean(row['latitude']),
                    clean(row['longitude']),
                    clean(row['temperature_c']),
                    clean(row['humidity_pct']),
                    clean(row['wind_speed_kmh']),
                    clean(row['precipitation_mm']),
                    recorded_at,
                    time_id
                ))

                count += 1

            conn.commit()
            print(f"Selesai memuat {count} baris ke tabel weather_data")

        except Exception:
            conn.rollback()
            raise

        finally:
            cursor.close()
            conn.close()

    load_to_mysql_task = PythonOperator(
        task_id='load_to_mysql_weather',
        python_callable=load_to_mysql,
    )

    create_table >> extract_transform_task >> load_to_mysql_task
