# Analisis dan Visualisasi Kualitas Udara Jakarta

Proyek ini membangun alur pengumpulan, penyimpanan, analisis, dan visualisasi data kualitas udara Jakarta. Data kualitas udara dan cuaca dikumpulkan secara otomatis menggunakan Apache Airflow, disimpan dalam MySQL, kemudian dianalisis menggunakan Python dan ditampilkan melalui dashboard Streamlit. Proyek ini juga membandingkan beberapa algoritma *machine learning* untuk mengklasifikasikan kategori AQI berdasarkan kondisi cuaca.

## Daftar Isi

- [Tujuan Proyek](#tujuan-proyek)
- [Pertanyaan Analisis](#pertanyaan-analisis)
- [Teknologi yang Digunakan](#teknologi-yang-digunakan)
- [Struktur Repository](#struktur-repository)
- [Sumber dan Isi Data](#sumber-dan-isi-data)
- [Alur Kerja Proyek](#alur-kerja-proyek)
- [Persiapan dan Konfigurasi](#persiapan-dan-konfigurasi)
- [Menjalankan Apache Airflow](#menjalankan-apache-airflow)
- [Menjalankan Notebook Analisis](#menjalankan-notebook-analisis)
- [Menjalankan Dashboard Streamlit](#menjalankan-dashboard-streamlit)
- [Metode Machine Learning](#metode-machine-learning)
- [Hasil Analisis](#hasil-analisis)
- [Catatan Teknis Penting](#catatan-teknis-penting)
- [Pemecahan Masalah](#pemecahan-masalah)

## Tujuan Proyek

Tujuan proyek ini adalah:

1. Mengumpulkan data kualitas udara dan cuaca Jakarta dari API secara otomatis.
2. Mengelola proses *Extract, Transform, Load* (ETL) menggunakan Apache Airflow.
3. Menyimpan data dalam database MySQL.
4. Menganalisis pola konsentrasi PM2.5 berdasarkan waktu, kondisi cuaca, dan polutan lainnya.
5. Membuat dashboard interaktif untuk menyajikan hasil analisis.
6. Membandingkan algoritma *machine learning* untuk mengklasifikasikan kategori AQI berdasarkan variabel cuaca.

## Pertanyaan Analisis

Proyek ini berusaha menjawab empat pertanyaan berikut:

1. Pada jam berapa dalam sehari kualitas udara Jakarta berdasarkan konsentrasi PM2.5 berada pada kondisi terburuk?
2. Bagaimana hubungan suhu, kelembapan, kecepatan angin, dan curah hujan terhadap konsentrasi PM2.5?
3. Bagaimana hubungan NO2, SO2, CO, dan ozon terhadap konsentrasi PM2.5?
4. Dapatkah kategori AQI, yaitu **Lebih Rendah** dan **Lebih Tinggi** dari median AQI, diklasifikasikan berdasarkan kondisi cuaca menggunakan *machine learning*?

## Teknologi yang Digunakan

| Teknologi | Kegunaan |
|---|---|
| Python | Bahasa pemrograman untuk ETL, analisis, dan pemodelan. |
| Apache Airflow | Mengatur jadwal dan menjalankan pipeline ETL melalui DAG. |
| Astro CLI | Menjalankan lingkungan Apache Airflow secara lokal. |
| Docker | Menjalankan container yang dibutuhkan oleh lingkungan Airflow. |
| MySQL | Menyimpan data kualitas udara, cuaca, profil wilayah, dan referensi waktu. |
| Pandas dan NumPy | Mengolah dan mengintegrasikan data. |
| Matplotlib dan Seaborn | Membuat visualisasi dan grafik analisis. |
| Scikit-learn | Melatih dan mengevaluasi model klasifikasi. |
| Joblib | Menyimpan dan memuat model *machine learning*. |
| Streamlit | Membuat dashboard interaktif. |
| Jupyter Notebook | Menjalankan eksplorasi dan analisis data. |

## Struktur Repository

```text
analisis-kualitas-udara-main/
├── README.md
├── analisis_kualitas_udara.ipynb
├── app.py
├── dag_air_quality.py
├── dag_weather.py
├── dag_city_profile.py
└── requirements.txt
```

Keterangan berkas:

| Berkas | Keterangan |
|---|---|
| `dag_air_quality.py` | DAG untuk mengambil data kualitas udara dari Open-Meteo Air Quality API dan memuatnya ke tabel `air_quality`. |
| `dag_weather.py` | DAG untuk mengambil data cuaca historis dari Open-Meteo Historical Weather API dan memuatnya ke tabel `weather_data`. |
| `dag_city_profile.py` | DAG untuk memuat data profil wilayah Jakarta ke tabel `city_profile`. |
| `analisis_kualitas_udara.ipynb` | Notebook untuk pengambilan data dari MySQL, pemeriksaan data, integrasi, EDA, visualisasi, dan pemodelan. |
| `app.py` | Aplikasi dashboard Streamlit yang membaca data MySQL dan menampilkan hasil analisis. |
| `requirements.txt` | Daftar sebagian dependensi Python untuk lingkungan proyek. |

**Catatan:** Berkas `best_model_aqi.joblib`, konfigurasi Docker/Astro tambahan, dan berkas konfigurasi koneksi database tidak terdapat dalam arsip repository yang diperiksa. Model perlu dibuat dari notebook, sedangkan lingkungan Airflow dan koneksi MySQL perlu dikonfigurasi sebelum pipeline dijalankan.

## Sumber dan Isi Data

### 1. Data Kualitas Udara

Sumber: [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api)

Data diambil untuk koordinat Jakarta:

- Latitude: `-6.2088`
- Longitude: `106.8456`
- Rentang pengambilan: sekitar 30 hari terakhir.
- Zona waktu API: `Asia/Jakarta`.

Variabel yang diambil meliputi:

- PM2.5 dan PM10.
- Ozon.
- Nitrogen dioksida (NO2).
- Sulfur dioksida (SO2).
- Karbon monoksida (CO).
- US AQI.
- Waktu pengukuran.

Data disimpan dalam tabel `air_quality`.

### 2. Data Cuaca

Sumber: [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)

Data diambil menggunakan koordinat dan rentang waktu yang sama dengan pengambilan data kualitas udara. Variabel yang digunakan meliputi:

- Suhu (`temperature_c`).
- Kelembapan (`humidity_pct`).
- Kecepatan angin (`wind_speed_kmh`).
- Curah hujan (`precipitation_mm`).
- Waktu pencatatan.

Data disimpan dalam tabel `weather_data`.

### 3. Data Profil Wilayah

Data profil wilayah disimpan dalam tabel `city_profile`. Berdasarkan kode `dag_city_profile.py`, data ini **ditulis langsung di dalam kode (hard-coded)** pada variabel `CITY_DATA`, bukan diambil secara langsung dari API saat DAG berjalan.

Data mencakup lima wilayah administrasi Jakarta, dengan atribut:

- Nama wilayah.
- Jumlah penduduk.
- Kepadatan penduduk per km².
- Estimasi jumlah kendaraan.
- Persentase ruang terbuka hijau.

Komentar pada kode menyebut BPS Provinsi DKI Jakarta sebagai sumber acuan. Namun, nilai pada `CITY_DATA` ditandai sebagai estimasi dan perlu diverifikasi kembali terhadap publikasi resmi sebelum digunakan sebagai data statistik resmi.

## Alur Kerja Proyek

Secara umum, alur proyek adalah:

```text
Open-Meteo Air Quality API ──> DAG Air Quality ──> MySQL: air_quality
                                                       │
Open-Meteo Historical Weather API ─> DAG Weather ─> MySQL: weather_data
                                                       │
Data profil dalam CITY_DATA ───────> DAG City Profile ─> MySQL: city_profile
                                                       │
                                                       ▼
                                         Notebook Python / Streamlit
                                                       │
                         ┌─────────────────────────────┴──────────────────────┐
                         ▼                                                    ▼
                 Analisis dan EDA                                  Dashboard interaktif
                         │
                         ▼
                 Model klasifikasi
                         │
                         ▼
                best_model_aqi.joblib
```

### DAG Apache Airflow

Terdapat tiga DAG utama:

| DAG ID | Jadwal | Tugas |
|---|---|---|
| `etl_air_quality_jakarta` | Harian (`@daily`) | Membuat tabel, mengambil data kualitas udara, menyimpan hasil transformasi sementara, dan memuat data ke MySQL. |
| `etl_weather_jakarta` | Harian (`@daily`) | Membuat tabel, mengambil data cuaca historis, menyimpan hasil transformasi sementara, dan memuat data ke MySQL. |
| `etl_city_profile_jakarta` | Mingguan (`@weekly`) | Membuat tabel, menyiapkan data profil wilayah dari `CITY_DATA`, dan memuatnya ke MySQL. |

Ketiga DAG menggunakan pola tugas **Create Table → Extract/Transform → Load**. Data sementara pada kode DAG disimpan sebagai CSV di direktori `/usr/local/airflow/dags/data/` di lingkungan Airflow.

## Persiapan dan Konfigurasi

### 1. Prasyarat

Siapkan perangkat lunak berikut:

- Python.
- MySQL Server.
- Docker Desktop.
- Astro CLI.
- Apache Airflow melalui lingkungan Astro.
- Jupyter Notebook atau Visual Studio Code.
- Streamlit.

Untuk menjalankan Airflow secara lokal di Windows, Docker Desktop perlu berjalan. Konfigurasi WSL 2 dapat digunakan sesuai pengaturan Docker Desktop.

### 2. Unduh Repository

```bash
git clone <URL-REPOSITORY>
cd analisis-kualitas-udara-main
```

Jika repository sudah diunduh dalam bentuk ZIP, ekstrak arsip kemudian buka folder proyek melalui terminal atau Visual Studio Code.

### 3. Buat Database MySQL

Buat database yang digunakan oleh kode proyek:

```sql
CREATE DATABASE airflow_db;
```

Aplikasi Streamlit dan notebook pada kode yang tersedia menggunakan konfigurasi koneksi berikut:

- Host: `localhost`
- Port: `3306`
- User: `root`
- Password: `root`
- Database: `airflow_db`

Sesuaikan konfigurasi tersebut dengan pengaturan MySQL pada perangkat masing-masing. Hindari menggunakan kredensial bawaan ini untuk lingkungan produksi.

### 4. Konfigurasi Koneksi MySQL di Airflow

DAG menggunakan Airflow Connection dengan ID `mysql-local`. Buat koneksi tersebut melalui Airflow UI:

1. Buka Airflow UI.
2. Masuk ke menu **Admin → Connections**.
3. Tambahkan koneksi baru.
4. Isi Connection ID dengan `mysql-local`.
5. Pilih tipe koneksi MySQL.
6. Isi host, port, username, password, dan schema sesuai konfigurasi MySQL.
7. Simpan koneksi.

Pastikan MySQL dapat diakses dari container Airflow. Jika MySQL berjalan di luar container, alamat `localhost` dari dalam container tidak otomatis menunjuk ke komputer host. Gunakan alamat host yang sesuai dengan konfigurasi Docker, misalnya `host.docker.internal` jika didukung oleh lingkungan tersebut.

### 5. Instal Dependensi Python

Berkas `requirements.txt` yang tersedia mencantumkan:

```text
apache-airflow-providers-mysql
pandas
mysql-connector-python
requests
rasterio
streamlit
```

Notebook dan aplikasi juga mengimpor beberapa pustaka lain, termasuk NumPy, Matplotlib, Seaborn, Scikit-learn, dan Joblib. Pastikan dependensi tersebut tersedia di lingkungan yang digunakan.

Untuk lingkungan Python terpisah yang menjalankan notebook dan Streamlit, dependensi dapat dipasang dengan:

```bash
pip install pandas numpy mysql-connector-python requests
pip install matplotlib seaborn scikit-learn joblib streamlit jupyter
```

Untuk lingkungan Astro/Airflow, tambahkan dependensi yang diperlukan oleh DAG ke `requirements.txt`, lalu bangun ulang lingkungan Astro. Pastikan versi paket kompatibel dengan versi Python dan Astro Runtime yang digunakan.

## Menjalankan Apache Airflow

Pastikan folder proyek sudah menjadi proyek Astro yang valid dan memiliki berkas konfigurasi yang diperlukan, termasuk `Dockerfile` Astro. Berkas tersebut tidak terdapat dalam arsip repository yang diperiksa. Jika belum ada, inisialisasi proyek Astro sesuai dokumentasi Astro CLI, kemudian tempatkan berkas DAG pada direktori `dags/` proyek Astro.

Jalankan perintah berikut dari direktori proyek Astro:

```bash
astro dev start
```

Setelah seluruh layanan berjalan, buka Airflow UI:

```text
http://localhost:8080
```

Selanjutnya:

1. Pastikan koneksi `mysql-local` sudah dibuat.
2. Pastikan database dan skema tabel sudah disiapkan.
3. Aktifkan DAG `etl_air_quality_jakarta`, `etl_weather_jakarta`, dan `etl_city_profile_jakarta`.
4. Jalankan DAG secara manual untuk pengujian awal atau tunggu jadwalnya berjalan.
5. Periksa status setiap task dan log apabila terjadi kegagalan.

Untuk menghentikan lingkungan lokal:

```bash
astro dev stop
```

Untuk melihat status container:

```bash
docker ps
```

## Menjalankan Notebook Analisis

Notebook `analisis_kualitas_udara.ipynb` digunakan untuk mengambil data dari MySQL dan melakukan analisis.

Sebelum menjalankan notebook:

1. Pastikan MySQL aktif.
2. Pastikan database `airflow_db` sudah berisi tabel `air_quality`, `weather_data`, dan `city_profile`.
3. Pastikan dependensi notebook sudah terpasang.
4. Periksa kembali konfigurasi koneksi MySQL pada notebook.

Jalankan Jupyter Notebook:

```bash
jupyter notebook
```

Buka `analisis_kualitas_udara.ipynb`, kemudian jalankan setiap cell secara berurutan.

Tahapan analisis di dalam notebook meliputi:

1. **Gathering Data:** Membaca tiga tabel MySQL menggunakan Pandas.
2. **Assessing Data:** Memeriksa struktur, tipe data, statistik deskriptif, nilai kosong, dan duplikasi.
3. **Cleaning Data:** Memastikan kolom waktu memiliki tipe `datetime` dan memeriksa kembali kondisi data.
4. **Integrasi Data:** Menggabungkan `air_quality` dan `weather_data` berdasarkan waktu pengukuran.
5. **Exploratory Data Analysis (EDA):** Menganalisis pola PM2.5, hubungan faktor cuaca, dan hubungan polutan lain terhadap PM2.5.
6. **Machine Learning:** Membentuk kategori AQI, membagi data, melatih, membandingkan, dan mengevaluasi model.
7. **Visualisasi dan Explanatory Analysis:** Membuat histogram, grafik korelasi, heatmap, dan boxplot.

Notebook menyimpan model terbaik dalam berkas:

```text
best_model_aqi.joblib
```

Letakkan berkas tersebut di direktori yang sama dengan `app.py` agar dapat dibaca oleh aplikasi Streamlit.

## Menjalankan Dashboard Streamlit

Dashboard membaca tabel `air_quality`, `weather_data`, dan `city_profile` dari MySQL. Tabel kualitas udara dan cuaca digabungkan menggunakan kolom `time_id`. Data profil kota dibaca secara terpisah.

Pastikan:
- MySQL aktif dan dapat diakses.
- Tabel yang diperlukan tersedia.
- Konfigurasi koneksi pada `app.py` sesuai dengan MySQL lokal.
- Berkas `best_model_aqi.joblib` tersedia di direktori yang sama dengan `app.py`.

Jalankan aplikasi:

```bash
streamlit run app.py
```

Dashboard dapat dibuka melalui alamat lokal yang ditampilkan Streamlit, biasanya:

```text
http://localhost:8501
```

### Fitur Dashboard

Dashboard terdiri atas tiga tab utama:

**1. Overview**

Menampilkan ringkasan data berupa:
- Jumlah data.
- Rata-rata PM2.5.
- Rata-rata US AQI.
- Nilai PM2.5 tertinggi.
- Grafik rata-rata konsentrasi PM2.5 berdasarkan jam.
- Sepuluh data terbaru.

**2. Analisis**

Menampilkan:
- Distribusi konsentrasi PM2.5.
- Hubungan faktor cuaca dengan PM2.5.
- Hubungan polutan lain, yaitu NO2, SO2, CO, dan ozon, dengan PM2.5.

**3. Machine Learning**

Menampilkan:
- Median US AQI pada data yang sedang difilter.
- Jumlah data pada kategori Lebih Rendah dan Lebih Tinggi.
- Grafik distribusi kategori AQI.
- Algoritma terbaik dan akurasi yang tersimpan pada artefak model.

Dashboard juga menyediakan filter rentang tanggal untuk menyesuaikan data yang ditampilkan.

## Metode Machine Learning

### Pembentukan Target

Target klasifikasi dibuat dari nilai median US AQI pada dataset. Data dibagi menjadi dua kategori:

- **Lebih Rendah:** nilai US AQI lebih kecil dari median.
- **Lebih Tinggi:** nilai US AQI sama dengan atau lebih besar dari median.

### Fitur dan Target

Empat fitur yang digunakan sebagai masukan model (`X`):

| Fitur | Keterangan |
|---|---|
| `temperature_c` | Suhu udara dalam derajat Celsius. |
| `humidity_pct` | Kelembapan udara dalam persen. |
| `wind_speed_kmh` | Kecepatan angin dalam km/jam. |
| `precipitation_mm` | Curah hujan dalam milimeter. |

Target (`y`) adalah `aqi_category`, yaitu kategori AQI Lebih Rendah atau Lebih Tinggi.

### Pembagian Data

Data dibagi menggunakan `train_test_split` dengan konfigurasi:

- Data training: 80%.
- Data testing: 20%.
- `random_state=42`.
- `stratify=y` untuk mempertahankan proporsi kelas pada data training dan testing.

Pada hasil yang tercantum di notebook, terdapat 1.056 data, dengan 844 data training dan 212 data testing.

### Model yang Dibandingkan

Lima algoritma klasifikasi yang diuji:

1. K-Nearest Neighbors (KNN).
2. Random Forest.
3. Decision Tree.
4. Support Vector Machine (SVM).
5. Logistic Regression.

Model dibandingkan berdasarkan nilai *accuracy* pada data pengujian. Model dengan akurasi tertinggi dipilih sebagai model terbaik dan disimpan menggunakan Joblib.

## Hasil Analisis

Ringkasan hasil yang ditampilkan pada notebook dan materi presentasi:

### Hasil Integrasi Data

- Data kualitas udara: 1.056 baris.
- Data cuaca: 1.080 baris.
- Hasil integrasi: 1.056 baris yang cocok berdasarkan waktu.
- Terdapat 24 waktu pencatatan cuaca yang tidak memiliki pasangan data kualitas udara.

### Temuan EDA

- Rata-rata konsentrasi PM2.5 tertinggi berdasarkan jam pada materi presentasi terjadi sekitar pukul 06.00.
- Suhu memiliki korelasi negatif dengan PM2.5 sekitar `-0,699`.
- Kelembapan memiliki korelasi positif dengan PM2.5 sekitar `0,693`.
- Kecepatan angin memiliki korelasi negatif dengan PM2.5 sekitar `-0,513`.
- Curah hujan memiliki korelasi sangat lemah dengan PM2.5 sekitar `-0,055`.
- NO2 memiliki korelasi positif paling kuat dengan PM2.5 sekitar `0,80`.
- SO2 memiliki korelasi positif sekitar `0,49`, CO sekitar `0,34`, dan ozon memiliki korelasi negatif sekitar `-0,69`.

Nilai korelasi menunjukkan hubungan linear pada dataset yang dianalisis dan tidak membuktikan hubungan sebab-akibat.

### Hasil Perbandingan Model

| Model | Accuracy |
|---|---:|
| Logistic Regression | 59,91% |
| KNN | 58,96% |
| Random Forest | 57,55% |
| Decision Tree | 53,77% |
| SVM | 53,77% |

Logistic Regression memperoleh akurasi tertinggi, yaitu sekitar **59,91%**. Hasil ini menunjukkan bahwa empat variabel cuaca yang digunakan memiliki kemampuan klasifikasi yang masih terbatas.

## Catatan Teknis Penting

Bagian ini perlu diperhatikan sebelum menjalankan repository dari awal. Dokumentasi ini disusun berdasarkan berkas sumber yang tersedia. Ada beberapa hal yang perlu diselaraskan agar seluruh komponen dapat berjalan secara konsisten.

### 1. Tabel `dim_time` dan Kolom `time_id`

Kode `dag_air_quality.py` dan `dag_weather.py` melakukan proses insert ke tabel `dim_time` dan mengisi kolom `time_id` pada tabel masing-masing. Namun, definisi `CREATE TABLE` pada kedua DAG belum mencantumkan kolom `time_id`, dan repository yang diperiksa tidak menyertakan DAG atau SQL untuk membuat tabel `dim_time`.

Sebelum menjalankan kedua DAG, pastikan tabel referensi waktu dan kolom `time_id` sudah dibuat. Salah satu bentuk skema yang sesuai dengan operasi insert pada kode adalah:

```sql
CREATE TABLE IF NOT EXISTS dim_time (
    time_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    time_value DATETIME NOT NULL UNIQUE
);
```

Jika tabel `air_quality` dan `weather_data` sudah terlanjur dibuat tanpa `time_id`, kolom tersebut perlu ditambahkan sebelum task pemuatan data dijalankan:

```sql
ALTER TABLE air_quality
ADD COLUMN time_id BIGINT;

ALTER TABLE weather_data
ADD COLUMN time_id BIGINT;
```

Periksa struktur tabel terlebih dahulu agar perintah `ALTER TABLE` tidak dijalankan berulang kali pada kolom yang sudah tersedia. Jika tabel masih kosong dan belum dibuat, alternatifnya adalah memperbarui pernyataan `CREATE TABLE` di masing-masing DAG agar langsung menyertakan kolom `time_id`.

### 2. Cara Integrasi Waktu pada Notebook dan Dashboard

Terdapat perbedaan pendekatan integrasi pada berkas yang tersedia:

- Notebook menggabungkan `air_quality.measured_at` dengan `weather_data.recorded_at`.
- `app.py` menggabungkan kedua tabel menggunakan kolom `time_id`.

Karena itu, pastikan kedua kolom waktu dan `time_id` merujuk pada waktu pengukuran yang sama. Jika menggunakan `time_id`, pastikan pemetaan pada `dim_time` benar dan tidak ada nilai yang belum terisi.

### 3. Pembuatan Model

Repository ZIP yang diperiksa tidak menyertakan `best_model_aqi.joblib`, sedangkan `app.py` memerlukan berkas tersebut untuk menampilkan informasi model. Jalankan cell pemodelan pada notebook sampai proses penyimpanan model selesai. Setelah itu, pastikan berkas `best_model_aqi.joblib` berada di direktori yang sama dengan `app.py`.

### 4. Konfigurasi Kredensial

Kode notebook dan `app.py` memuat konfigurasi MySQL secara langsung. Sesuaikan host, port, username, password, dan nama database dengan lingkungan masing-masing. Untuk penggunaan bersama atau deployment, sebaiknya simpan kredensial dalam environment variable atau pengelola secret, bukan menuliskannya langsung di source code.

### 5. Konsistensi Kategori AQI pada Dashboard

Notebook membentuk kategori berdasarkan median US AQI dari keseluruhan data yang dianalisis. Sementara itu, `app.py` menghitung ulang median berdasarkan data yang sedang dipilih melalui filter tanggal. Akibatnya, kategori pada dashboard dapat berubah ketika rentang tanggal diubah. Selain itu, informasi akurasi model yang ditampilkan berasal dari artefak model yang dibuat saat pelatihan.

Jika kategori dashboard ingin dibandingkan secara konsisten dengan hasil pelatihan, gunakan batas median yang sama dengan yang digunakan saat membentuk target pada notebook.

### 6. Dependensi dan Berkas Astro

`requirements.txt` yang tersedia belum mencantumkan seluruh pustaka yang diimpor oleh notebook dan dashboard, seperti `numpy`, `matplotlib`, `seaborn`, `scikit-learn`, dan `joblib`. Tambahkan dependensi yang diperlukan pada lingkungan masing-masing. Arsip repository yang diperiksa juga belum menyertakan `Dockerfile` Astro, sehingga folder perlu dilengkapi atau ditempatkan pada proyek Astro yang valid sebelum menjalankan `astro dev start`.

## Pemecahan Masalah

| Kendala | Hal yang perlu diperiksa |
|---|---|
| Airflow tidak dapat terhubung ke MySQL | Periksa koneksi `mysql-local`, host, port, username, password, nama database, dan akses jaringan dari container. |
| DAG gagal pada proses insert `dim_time` | Pastikan tabel `dim_time` tersedia dan kolom `time_id` pada tabel tujuan sudah dibuat. |
| Error `Unknown column 'time_id'` | Periksa definisi tabel `air_quality` dan `weather_data`, lalu tambahkan kolom yang dibutuhkan. |
| Notebook gagal membaca tabel | Pastikan MySQL aktif, database `airflow_db` tersedia, dan ketiga tabel sudah terisi. |
| Notebook gagal mengimpor pustaka | Instal dependensi yang belum tersedia pada environment Python aktif. |
| Streamlit gagal terhubung ke database | Periksa konfigurasi koneksi MySQL pada `app.py` dan pastikan server MySQL berjalan. |
| Streamlit gagal memuat model | Pastikan `best_model_aqi.joblib` sudah dibuat dari notebook dan berada di direktori yang sama dengan `app.py`. |
| Data hasil integrasi lebih sedikit | Periksa kesesuaian waktu pengukuran, zona waktu, dan nilai `time_id` pada kedua tabel. |

## Penutup

Repository ini menggabungkan pipeline ETL, database relasional, analisis data, visualisasi, dan klasifikasi *machine learning* dalam satu proyek analisis kualitas udara Jakarta. Pengguna dapat mempelajari proses pengambilan data otomatis menggunakan Apache Airflow, pengelolaan data menggunakan MySQL, analisis menggunakan Python, serta penyajian informasi melalui dashboard Streamlit.

Sebelum menjalankan seluruh komponen, periksa kembali konfigurasi database, skema `dim_time` dan `time_id`, dependensi Python, serta ketersediaan artefak model agar pipeline, notebook, dan dashboard dapat bekerja secara konsisten.
