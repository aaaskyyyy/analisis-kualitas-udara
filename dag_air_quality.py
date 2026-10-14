"""
DAG: Kualitas Udara Jakarta (Open-Meteo Air Quality API)
Tabel UTAMA untuk projek analisis kualitas udara.
TIDAK PERLU API KEY - pengganti OpenAQ yang bermasalah.
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
    dag_id='etl_air_quality_jakarta',
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=['tugas', 'mysql', 'etl', 'air-quality', 'sdg', 'api'],
) as dag:

    create_table = MySqlOperator(
        task_id='create_table_air_quality',
        conn_id='mysql-local',
        sql="""
        CREATE TABLE IF NOT EXISTS air_quality (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            latitude DECIMAL(10,6),
            longitude DECIMAL(10,6),
            pm2_5 DECIMAL(10,4),
            pm10 DECIMAL(10,4),
            ozone DECIMAL(10,4),
            nitrogen_dioxide DECIMAL(10,4),
            sulphur_dioxide DECIMAL(10,4),
            carbon_monoxide DECIMAL(10,4),
            us_aqi DECIMAL(10,2),
            measured_at DATETIME,
            UNIQUE KEY uq_measurement (latitude, longitude, measured_at)
        );
        """,
    )

    def extract_transform():
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=DAYS_BACK)

        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude": JAKARTA_LAT,
            "longitude": JAKARTA_LON,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "hourly": "pm2_5,pm10,ozone,nitrogen_dioxide,sulphur_dioxide,carbon_monoxide,us_aqi",
            "timezone": "Asia/Jakarta",
        }
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        rows = []
        for i in range(len(times)):
            rows.append({
                "latitude": JAKARTA_LAT,
                "longitude": JAKARTA_LON,
                "pm2_5": hourly.get("pm2_5", [None]*len(times))[i],
                "pm10": hourly.get("pm10", [None]*len(times))[i],
                "ozone": hourly.get("ozone", [None]*len(times))[i],
                "nitrogen_dioxide": hourly.get("nitrogen_dioxide", [None]*len(times))[i],
                "sulphur_dioxide": hourly.get("sulphur_dioxide", [None]*len(times))[i],
                "carbon_monoxide": hourly.get("carbon_monoxide", [None]*len(times))[i],
                "us_aqi": hourly.get("us_aqi", [None]*len(times))[i],
                "measured_at": times[i],
            })

        df = pd.DataFrame(rows)
        df = df.dropna(subset=["measured_at", "pm2_5"])
        print(f"Total baris data kualitas udara: {len(df)}")

        transformed_path = "/usr/local/airflow/dags/data/transformed_air_quality.csv"
        df.to_csv(transformed_path, index=False)
        print(f"Data disimpan ke {transformed_path}")

    extract_transform_task = PythonOperator(
        task_id='extract_transform_air_quality',
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

        transformed_path = "/usr/local/airflow/dags/data/transformed_air_quality.csv"
        df = pd.read_csv(transformed_path)

        count = 0

        try:
            for _, row in df.iterrows():
                measured_at = pd.to_datetime(row['measured_at']).to_pydatetime()

                # Pastikan timestamp tersedia di dim_time.
                cursor.execute("""
                    INSERT INTO dim_time (time_value)
                    VALUES (%s)
                    ON DUPLICATE KEY UPDATE
                        time_id = LAST_INSERT_ID(time_id)
                """, (measured_at,))

                time_id = cursor.lastrowid

                # Ubah nilai NaN menjadi NULL untuk MySQL.
                def clean(value):
                    return None if pd.isna(value) else value

                cursor.execute("""
                    INSERT INTO air_quality (
                        latitude, longitude, pm2_5, pm10, ozone,
                        nitrogen_dioxide, sulphur_dioxide, carbon_monoxide,
                        us_aqi, measured_at, time_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        pm2_5 = VALUES(pm2_5),
                        pm10 = VALUES(pm10),
                        ozone = VALUES(ozone),
                        nitrogen_dioxide = VALUES(nitrogen_dioxide),
                        sulphur_dioxide = VALUES(sulphur_dioxide),
                        carbon_monoxide = VALUES(carbon_monoxide),
                        us_aqi = VALUES(us_aqi)
                """, (
                    clean(row['latitude']),
                    clean(row['longitude']),
                    clean(row['pm2_5']),
                    clean(row['pm10']),
                    clean(row['ozone']),
                    clean(row['nitrogen_dioxide']),
                    clean(row['sulphur_dioxide']),
                    clean(row['carbon_monoxide']),
                    clean(row['us_aqi']),
                    measured_at,
                    time_id
                ))

                count += 1

            conn.commit()
            print(f"Selesai memuat {count} baris ke tabel air_quality")

        except Exception:
            conn.rollback()
            raise

        finally:
            cursor.close()
            conn.close()

    load_to_mysql_task = PythonOperator(
        task_id='load_to_mysql_air_quality',
        python_callable=load_to_mysql,
    )

    create_table >> extract_transform_task >> load_to_mysql_task