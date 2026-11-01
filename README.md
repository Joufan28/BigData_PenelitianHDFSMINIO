# Lakehouse Banjir: MinIO + Apache Iceberg vs HDFS + Parquet

Panduan ini berisi urutan menjalankan proyek dari awal pada Ubuntu menggunakan Docker Compose dan Fish shell.

Penelitian membandingkan:
- **Lakehouse:** MinIO + Apache Iceberg.
- **Baseline:** HDFS + Apache Parquet.
- **Query engine:** Apache Spark yang sama untuk kedua sistem.

Fokusnya adalah analitik batch data risiko banjir, operasi perubahan data, dan pengelolaan riwayat perubahan. Ini bukan sistem peringatan banjir real-time.

## 1. Persyaratan

- Ubuntu/Linux, Docker Engine, Docker Compose plugin.
- Fish shell.
- Dataset Kaggle `naiyakhalid/flood-prediction-dataset`.
- Disarankan menutup aplikasi berat ketika benchmark berjalan (target laptop RAM 8 GB).

Periksa Docker:

```fish
docker --version
docker compose version
docker info
```

## 2. Masuk ke direktori proyek

```fish
cd ~/Documents/lakehouse_banjir_project
mkdir -p data/raw results/runs scripts
```

Struktur penting:

```text
lakehouse_banjir_project/
├── data/raw/train.csv
├── results/runs/
├── scripts/benchmark_v2.py
├── .env
├── docker-compose.yml
└── Dockerfile.spark
```

Pastikan file script, Dockerfile, dan Compose tersedia sebelum melanjutkan.

## 3. Siapkan dataset

Gunakan **`train.csv`** sebagai dataset utama, bukan `test.csv`, `sample_submission.csv`, atau dataset lain seperti `flood.csv`.

Letakkan file di:

```text
data/raw/train.csv
```

Dataset yang digunakan dalam konfigurasi ini memiliki 1.117.957 record, kolom `id`, 20 faktor risiko, dan `FloodProbability`.

Periksa file:

```fish
ls -lh data/raw/train.csv
wc -l data/raw/train.csv
```

`wc -l` menghitung baris fisik termasuk header. Untuk CSV tanpa newline di dalam nilai, jumlah record biasanya hasil tersebut dikurangi satu.

## 4. Siapkan file .env

Jika `.env` belum ada, salin template:

```fish
cp .env.example .env
```

Jangan jalankan perintah `cp` jika `.env` sudah ada karena dapat menimpa konfigurasi. Isi kredensial sesuai konfigurasi Compose, contohnya:

```dotenv
MINIO_ROOT_USER=lakeadmin
MINIO_ROOT_PASSWORD=change-this-password
```

Gunakan password yang kuat dan jangan unggah `.env` berisi kredensial ke repositori publik.

## 5. Build image dan jalankan layanan

Pertama kali:

```fish
docker compose up -d --build
```

Perintah tersebut membangun image yang diperlukan lalu menjalankan container di background. Untuk sesi berikutnya, jika tidak ada perubahan image:

```fish
docker compose up -d
```

Build image saja:

```fish
docker compose build
```

Periksa status:

```fish
docker compose ps -a
```

Layanan utama:
- MinIO: object storage.
- `minio-init`: memastikan bucket tersedia, lalu biasanya selesai/exit.
- NameNode dan DataNode: HDFS.
- Spark: image untuk menjalankan job batch.

Container Spark yang berhenti setelah menjalankan `--version` dapat merupakan kondisi normal. Job Spark dijalankan terpisah pada langkah berikutnya.

Periksa log jika ada masalah:

```fish
docker compose logs --tail=100 minio
docker compose logs --tail=100 minio-init
docker compose logs --tail=100 namenode
docker compose logs --tail=100 datanode
```

**Penting:** perintah job di bawah menggunakan `--no-deps`, sehingga layanan MinIO/HDFS harus sudah dinyalakan lebih dahulu.

## 6. Periksa MinIO dan HDFS

MinIO Console:

```text
http://localhost:9001
```

Login menggunakan kredensial dari `.env`. Bucket yang digunakan adalah `lakehouse`, dengan warehouse Iceberg:

```text
s3a://lakehouse/warehouse
```

Dari dalam jaringan Docker, endpoint MinIO adalah `http://minio:9000` (bukan `localhost`).

Periksa HDFS:

```fish
docker compose exec namenode hdfs dfsadmin -report
docker compose exec namenode hdfs dfs -ls -h /bench/flood/current
```

Hitung file Parquet:

```fish
docker compose exec namenode sh -c 'hdfs dfs -ls /bench/flood/current | grep -c "\.parquet"'
```

Pastikan DataNode terdeteksi live. Direktori HDFS benchmark adalah `/bench/flood/current`.

## 7. Konfigurasi Spark untuk RAM 8 GB

Gunakan pengaturan konservatif berikut untuk setiap job:

```text
--master local[2]
--conf spark.driver.memory=1800m
--conf spark.executor.memory=1800m
--conf spark.sql.shuffle.partitions=4
```

Jangan menjalankan beberapa job Spark bersamaan.

## 8. Inisialisasi data (jalankan satu kali)

Mode `init` membaca `train.csv`, membuat/mengganti tabel Iceberg `lake.banjir.train`, dan menulis Parquet ke HDFS.

```fish
docker compose run --rm --no-deps spark \
  --master 'local[2]' \
  --conf spark.driver.memory=1800m \
  --conf spark.executor.memory=1800m \
  --conf spark.sql.shuffle.partitions=4 \
  /app/scripts/benchmark_v2.py init
```

**Peringatan:** `init` dapat menulis ulang tabel dan direktori data benchmark. Jangan jalankan ulang sebelum setiap benchmark. Jalankan ulang hanya jika ingin memuat ulang data atau mengembalikan kondisi awal.

## 9. Validasi data

Setelah `init` selesai:

```fish
docker compose run --rm --no-deps spark \
  --master 'local[2]' \
  --conf spark.driver.memory=1800m \
  --conf spark.executor.memory=1800m \
  --conf spark.sql.shuffle.partitions=4 \
  /app/scripts/benchmark_v2.py validate
```

Validasi membandingkan jumlah baris, jumlah `id` unik, jumlah total `FloodProbability`, dan rata-ratanya pada Iceberg dan Parquet. Lanjutkan jika hasil menyatakan `passed: true`.

Contoh hasil run validasi yang pernah diperoleh:

| Pemeriksaan | Iceberg | Parquet |
|---|---:|---:|
| Rows | 1.117.957 | 1.117.957 |
| Distinct IDs | 1.117.957 | 1.117.957 |
| Probability sum | 563987.3100000306 | 563987.3100000353 |
| Probability average | 0.5044803243774407 | 0.504480324377445 |

Perbedaan sangat kecil pada angka desimal dapat berasal dari presisi floating-point. Contoh di atas bukan jaminan hasil pada setiap komputer; gunakan output aktual.

## 10. Benchmark analitik

Jalankan setelah validasi berhasil:

```fish
docker compose run --rm --no-deps spark \
  --master 'local[2]' \
  --conf spark.driver.memory=1800m \
  --conf spark.executor.memory=1800m \
  --conf spark.sql.shuffle.partitions=4 \
  /app/scripts/benchmark_v2.py benchmark
```

Versi script ini menguji lima query:
1. `count_all`
2. `probability_avg`
3. `monsoon_group`
4. `feature_summary`
5. `correlation`

Script melakukan warm-up dan pengukuran berulang, lalu menyimpan ringkasan/median. Pertahankan dataset, konfigurasi Spark, dan query yang sama saat membandingkan kedua sistem.

## 11. Benchmark perubahan data

```fish
docker compose run --rm --no-deps spark \
  --master 'local[2]' \
  --conf spark.driver.memory=1800m \
  --conf spark.executor.memory=1800m \
  --conf spark.sql.shuffle.partitions=4 \
  /app/scripts/benchmark_v2.py changes
```

Mode ini menguji INSERT/append, UPDATE, dan DELETE. Implementasi Parquet pada script menggunakan staging lalu penggantian direktori untuk operasi perubahan agar proses baca/tulis tidak saling menimpa.

Operasi perubahan dapat mengubah kondisi data. Untuk mengulang eksperimen perubahan dari kondisi awal yang sama, jalankan `init` kembali terlebih dahulu dan simpan hasil sebelumnya.

## 12. Hasil pengujian

Hasil tiap run disimpan di:

```text
results/runs/
```

Periksa file:

```fish
ls -lh results/runs
```

Baca salah satu hasil (sesuaikan nama file):

```fish
cat results/runs/run_001.jsonl
```

Simpan hasil validasi, benchmark, dan changes. Jangan menghapus hasil lama sebelum salinannya aman.

## 13. Urutan singkat dari awal

1. Pasang dan periksa Docker serta Docker Compose.
2. Masuk ke direktori proyek.
3. Pastikan `data/raw/train.csv` tersedia.
4. Pastikan `.env` sudah dikonfigurasi.
5. Jalankan `docker compose up -d --build`.
6. Periksa `docker compose ps -a` dan kesiapan MinIO/HDFS.
7. Jalankan mode `init` satu kali.
8. Jalankan `validate`; lanjut jika `passed: true`.
9. Jalankan `benchmark`.
10. Jalankan `changes`.
11. Periksa dan amankan file di `results/runs/`.

Untuk sesi berikutnya saat data sudah ada:

```fish
cd ~/Documents/lakehouse_banjir_project
docker compose up -d
docker compose ps -a
```

Kemudian jalankan mode yang diperlukan. Jangan jalankan `init` kecuali memang ingin menginisialisasi ulang data.

## 14. Menghentikan layanan

Hentikan container tanpa menghapus volume:

```fish
docker compose down
```

Nyalakan kembali:

```fish
docker compose up -d
```

**Jangan gunakan `docker compose down -v` untuk penghentian biasa.** Opsi `-v` menghapus named volume yang dapat berisi data MinIO/HDFS.

## 15. Troubleshooting

### Container tidak berjalan

```fish
docker compose ps -a
docker compose logs --tail=150
```

### Bucket MinIO belum tersedia

```fish
docker compose logs --tail=150 minio-init
```

Pastikan kredensial service MinIO dan `minio-init` konsisten.

### Data HDFS tidak ditemukan

```fish
docker compose exec namenode hdfs dfs -ls -h /bench/flood
docker compose exec namenode hdfs dfs -ls -h /bench/flood/current
```

Jika belum pernah diinisialisasi, jalankan `init` setelah dataset dan layanan siap.

### Spark gagal mengakses MinIO/HDFS

Pastikan layanan berjalan dan endpoint konsisten:
- MinIO: `http://minio:9000`
- HDFS: `hdfs://namenode:8020`
- Iceberg warehouse: `s3a://lakehouse/warehouse`

Gunakan hostname service Compose (`minio`) dari container Spark, bukan `localhost`.

### Memori tidak cukup

- Tutup aplikasi berat.
- Jangan menjalankan job bersamaan.
- Mulai dari `local[2]`, memori 1800m, dan shuffle partitions 4.
- Dokumentasikan setiap perubahan konfigurasi; jangan mengubahnya di tengah rangkaian perbandingan tanpa catatan.

## 16. Ringkasan versi dan lokasi

| Komponen | Konfigurasi |
|---|---|
| Spark | 3.5.9 |
| Scala | 2.12 |
| Java | 17 |
| Iceberg runtime | 1.9.2 |
| Hadoop AWS | 3.3.4 |
| AWS SDK bundle | 1.12.262 |
| MinIO image | `dappros/minio:RELEASE.2025-09-07T16-13-09Z` |
| Bucket | `lakehouse` |
| Iceberg warehouse | `s3a://lakehouse/warehouse` |
| Iceberg table | `lake.banjir.train` |
| HDFS NameNode | `hdfs://namenode:8020` |
| HDFS Parquet path | `/bench/flood/current` |
| Dataset | `naiyakhalid/flood-prediction-dataset` (`train.csv`) |
| Record | 1.117.957 |

Untuk reproduksibilitas, catat tanggal/waktu, versi image, konfigurasi Spark, dataset, mode pengujian, file JSONL, dan error atau perubahan yang dilakukan.
