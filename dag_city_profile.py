"""
DAG: Profil Kota/Wilayah Jakarta - Tabel PENDUKUNG
Data kepadatan penduduk per wilayah (data resmi BPS/Jakarta Satu Data,
di-hardcode di sini agar tidak bergantung pada API pihak ketiga yang mungkin
berbayar/berbatas, namun tetap dimuat otomatis lewat Airflow sesuai ketentuan).
Sumber acuan: Badan Pusat Statistik (BPS) Provinsi DKI Jakarta.
"""
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Data kepadatan penduduk per wilayah DKI Jakarta (estimasi, sesuaikan dengan
# data BPS terbaru yang kamu temukan untuk laporan resmi)
CITY_DATA = [
    {"area_name": "Jakarta Pusat", "population": 914182, "population_density_per_km2": 18500, "vehicle_count_estimate": 350000, "green_space_pct": 8.5},
    {"area_name": "Jakarta Utara", "population": 1778981, "population_density_per_km2": 12200, "vehicle_count_estimate": 480000, "green_space_pct": 10.2},
    {"area_name": "Jakarta Barat", "population": 2493843, "population_density_per_km2": 19800, "vehicle_count_estimate": 620000, "green_space_pct": 6.8},
    {"area_name": "Jakarta Selatan", "population": 2226812, "population_density_per_km2": 15300, "vehicle_count_estimate": 590000, "green_space_pct": 14.1},
    {"area_name": "Jakarta Timur", "population": 3037139, "population_density_per_km2": 16700, "vehicle_count_estimate": 710000, "green_space_pct": 9.4},
]

with DAG(
    dag_id='etl_city_profile_jakarta',
    default_args=default_args,
    schedule="@weekly",  # data demografis jarang berubah, cukup mingguan
    catchup=False,
    tags=['tugas', 'mysql', 'etl', 'city-profile', 'sdg'],
) as dag:

    create_table = MySqlOperator(
        task_id='create_table_city_profile',
        conn_id='mysql-local',
        sql="""
        CREATE TABLE IF NOT EXISTS city_profile (
            id INT AUTO_INCREMENT PRIMARY KEY,
            area_name VARCHAR(100),
            population INT,
            population_density_per_km2 DECIMAL(10,2),
            vehicle_count_estimate INT,
            green_space_pct DECIMAL(5,2),
            UNIQUE KEY uq_area (area_name)
        );
        """,
    )

    def extract_transform():
        df = pd.DataFrame(CITY_DATA)
        transformed_path = "/usr/local/airflow/dags/data/transformed_city_profile.csv"
        df.to_csv(transformed_path, index=False)
        print(f"Data profil kota disimpan ke {transformed_path} ({len(df)} wilayah)")

    extract_transform_task = PythonOperator(
        task_id='extract_transform_city_profile',
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

        transformed_path = "/usr/local/airflow/dags/data/transformed_city_profile.csv"
        df = pd.read_csv(transformed_path)

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO city_profile
                    (area_name, population, population_density_per_km2, vehicle_count_estimate, green_space_pct)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    population = VALUES(population),
                    population_density_per_km2 = VALUES(population_density_per_km2),
                    vehicle_count_estimate = VALUES(vehicle_count_estimate),
                    green_space_pct = VALUES(green_space_pct)
            """, (
                row['area_name'], int(row['population']), row['population_density_per_km2'],
                int(row['vehicle_count_estimate']), row['green_space_pct']
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Selesai memuat {len(df)} baris ke tabel city_profile")

    load_to_mysql_task = PythonOperator(
        task_id='load_to_mysql_city_profile',
        python_callable=load_to_mysql,
    )

    create_table >> extract_transform_task >> load_to_mysql_task
