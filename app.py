import streamlit as st
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import seaborn as sns
import joblib

# ============================================================
# KONFIGURASI HALAMAN
# ============================================================

st.set_page_config(
    page_title="Dashboard Kualitas Udara Jakarta",
    page_icon="🌫️",
    layout="wide"
)


# ============================================================
# KONEKSI DATABASE
# ============================================================

@st.cache_resource
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="root",
        database="airflow_db"
    )


# ============================================================
# MEMUAT DATA
# ============================================================

@st.cache_data
def load_data():

    conn = get_connection()

    df_air = pd.read_sql(
        "SELECT * FROM air_quality",
        conn
    )

    df_weather = pd.read_sql(
        "SELECT * FROM weather_data",
        conn
    )

    df_city = pd.read_sql(
        "SELECT * FROM city_profile",
        conn
    )

    df_air["measured_at"] = pd.to_datetime(
        df_air["measured_at"]
    )

    df_weather["recorded_at"] = pd.to_datetime(
        df_weather["recorded_at"]
    )

    return df_air, df_weather, df_city


# ============================================================
# LOAD DATA
# ============================================================

try:
    df_air, df_weather, df_city = load_data()

except Exception as e:
    st.error("Gagal terhubung ke database.")
    st.code(str(e))
    st.stop()


# ============================================================
# INTEGRASI DATA
# ============================================================

df_merged = pd.merge(
    df_air,
    df_weather,
    on="time_id",
    how="inner"
)

# ============================================================
# JUDUL DASHBOARD
# ============================================================

st.title("🌫️ Dashboard Kualitas Udara Jakarta")

st.markdown(
    """
    Dashboard ini menyajikan analisis kualitas udara Jakarta
    berdasarkan data kualitas udara dan kondisi cuaca yang
    tersimpan pada database MySQL.
    """
)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Filter Data")

min_date = df_merged["measured_at"].dt.date.min()
max_date = df_merged["measured_at"].dt.date.max()

date_range = st.sidebar.date_input(
    "Rentang tanggal",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) == 2:

    start_date = pd.Timestamp(date_range[0])
    end_date = pd.Timestamp(date_range[1]) + pd.Timedelta(days=1)

    df_filtered = df_merged[
        (df_merged["measured_at"] >= start_date) &
        (df_merged["measured_at"] < end_date)
    ]

else:

    df_filtered = df_merged.copy()


# ============================================================
# KPI
# ============================================================

st.subheader("Ringkasan Data")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Jumlah Data",
        f"{len(df_filtered):,}"
    )

with col2:
    st.metric(
        "Rata-rata PM2.5",
        f"{df_filtered['pm2_5'].mean():.2f} µg/m³"
    )

with col3:
    st.metric(
        "Rata-rata AQI",
        f"{df_filtered['us_aqi'].mean():.2f}"
    )

with col4:
    st.metric(
        "PM2.5 Tertinggi",
        f"{df_filtered['pm2_5'].max():.2f} µg/m³"
    )


# ============================================================
# TAB DASHBOARD
# ============================================================

tab1, tab2, tab3 = st.tabs(
    [
        "📊 Overview",
        "🌦️ Analisis",
        "🤖 Machine Learning"
    ]
)


# ============================================================
# TAB 1 - OVERVIEW
# ============================================================

with tab1:

    st.subheader("Tren Konsentrasi PM2.5")

    hourly_pm25 = (
        df_filtered
        .groupby(
            df_filtered["measured_at"].dt.hour
        )["pm2_5"]
        .mean()
        .reset_index()
    )

    hourly_pm25.columns = [
        "hour",
        "avg_pm2_5"
    ]

    fig, ax = plt.subplots(figsize=(12, 5))

    sns.lineplot(
        data=hourly_pm25,
        x="hour",
        y="avg_pm2_5",
        marker="o",
        ax=ax
    )

    ax.set_title(
        "Rata-rata Konsentrasi PM2.5 Berdasarkan Jam"
    )

    ax.set_xlabel("Jam")
    ax.set_ylabel("PM2.5 (µg/m³)")

    ax.set_xticks(range(24))

    st.pyplot(fig)


    st.subheader("Data Terbaru")

    display_columns = [
        "measured_at",
        "pm2_5",
        "us_aqi",
        "temperature_c",
        "humidity_pct",
        "wind_speed_kmh",
        "precipitation_mm"
    ]

    st.dataframe(
        df_filtered[display_columns]
        .sort_values("measured_at", ascending=False)
        .head(10),
        use_container_width=True
    )


# ============================================================
# TAB 2 - ANALISIS
# ============================================================

with tab2:

    st.subheader("Distribusi Konsentrasi PM2.5")

    fig, ax = plt.subplots(figsize=(10, 5))

    sns.histplot(
        data=df_filtered,
        x="pm2_5",
        kde=True,
        bins=30,
        ax=ax
    )

    ax.set_title(
        "Distribusi Konsentrasi PM2.5"
    )

    ax.set_xlabel("PM2.5 (µg/m³)")
    ax.set_ylabel("Jumlah Data")

    st.pyplot(fig)


    st.subheader("Hubungan Faktor Cuaca dengan PM2.5")

    weather_cols = [
        "temperature_c",
        "humidity_pct",
        "wind_speed_kmh",
        "precipitation_mm"
    ]

    weather_corr = (
        df_filtered[
            ["pm2_5"] + weather_cols
        ]
        .corr()["pm2_5"]
        .drop("pm2_5")
        .sort_values(ascending=False)
    )

    fig, ax = plt.subplots(figsize=(8, 5))

    weather_corr.plot(
        kind="bar",
        ax=ax
    )

    ax.set_title(
        "Korelasi Faktor Cuaca dengan PM2.5"
    )

    ax.set_xlabel("Faktor Cuaca")
    ax.set_ylabel("Koefisien Korelasi")

    ax.axhline(
        0,
        linewidth=1
    )

    plt.xticks(rotation=0)

    st.pyplot(fig)


    st.subheader("Hubungan Polutan dengan PM2.5")

    pollutant_cols = [
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "carbon_monoxide",
        "ozone"
    ]

    pollutant_corr = (
        df_filtered[
            ["pm2_5"] + pollutant_cols
        ]
        .corr()["pm2_5"]
        .drop("pm2_5")
        .sort_values(ascending=False)
    )

    fig, ax = plt.subplots(figsize=(8, 5))

    pollutant_corr.plot(
        kind="bar",
        ax=ax
    )

    ax.set_title(
        "Korelasi Polutan dengan PM2.5"
    )

    ax.set_xlabel("Polutan")
    ax.set_ylabel("Koefisien Korelasi")

    ax.axhline(
        0,
        linewidth=1
    )

    plt.xticks(rotation=0)

    st.pyplot(fig)


@st.cache_resource
def load_model():
    return joblib.load("best_model_aqi.joblib")

try:
    model_artifact = load_model()
    best_model = model_artifact["model"]
    best_name = model_artifact["model_name"]
    best_accuracy = model_artifact["accuracy"]
    feature_cols = model_artifact["feature_columns"]

except Exception as e:
    st.error(f"Gagal memuat model: {e}")
    st.stop()
    
# ============================================================
# TAB 3 - MACHINE LEARNING
# ============================================================

with tab3:

    st.subheader("Klasifikasi Tingkat AQI")

    median_aqi = df_filtered["us_aqi"].median()

    st.metric(
        "Median AQI",
        f"{median_aqi:.0f}"
    )

    df_ml = df_filtered.copy()

    df_ml["aqi_category"] = (
        df_ml["us_aqi"]
        .apply(
            lambda x:
            "Lebih Rendah"
            if x < median_aqi
            else "Lebih Tinggi"
        )
    )

    category_counts = (
        df_ml["aqi_category"]
        .value_counts()
        .reset_index()
    )

    category_counts.columns = [
        "Kategori AQI",
        "Jumlah Data"
    ]

    st.dataframe(
        category_counts,
        use_container_width=True
    )


    st.subheader(
        "Distribusi Kategori AQI"
    )

    fig, ax = plt.subplots(figsize=(7, 5))

    sns.countplot(
        data=df_ml,
        x="aqi_category",
        ax=ax
    )

    ax.set_title(
        "Distribusi Kategori AQI"
    )

    ax.set_xlabel("Kategori AQI")
    ax.set_ylabel("Jumlah Data")

    st.pyplot(fig)


    st.subheader("Model Terbaik")

    st.metric(
        "Algoritma",
        best_name,
        f"Akurasi: {best_accuracy:.2%}"
    )