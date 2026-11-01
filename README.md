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
├── data/
│   └── raw/
│       └── train.csv
├── results/
│   ├── runs/
│   └── csv/
├── scripts/
│   ├── benchmark_v2.py
│   ├── benchmark_history.py
│   └── export_results_csv.py
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

## 12. Pengujian change history

Pengujian change history dilakukan untuk mengetahui kemampuan masing-masing sistem dalam mempertahankan dan membaca kondisi data sebelum dan sesudah perubahan.

Pengujian menggunakan dataset utama sebanyak 1.117.957 record. Satu baris data dipilih secara deterministik berdasarkan ID terkecil. Nilai `FloodProbability` pada data tersebut diubah dari nilai awal menjadi `0.500`.

### 12.1. Menjalankan pengujian

Pastikan layanan MinIO dan HDFS sudah aktif serta data utama sudah diinisialisasi.

Jalankan perintah berikut:

```fish
docker compose run --rm --no-deps spark \
  --master 'local[2]' \
  --conf spark.driver.memory=1800m \
  --conf spark.executor.memory=1800m \
  --conf spark.sql.shuffle.partitions=4 \
  /app/scripts/benchmark_history.py
```

### 12.2. Skenario pengujian

Pengujian dilakukan dengan tahapan berikut:

1. Membaca tabel utama Iceberg dan dataset Parquet pada HDFS.
2. Memeriksa kesesuaian jumlah record kedua sistem.
3. Membuat tabel uji terpisah `lake.banjir.history_test` pada Iceberg.
4. Mengambil ID snapshot awal sebelum perubahan.
5. Melakukan UPDATE terhadap satu record pada tabel uji Iceberg.
6. Mengambil snapshot terbaru dan membaca kembali snapshot awal menggunakan mekanisme time travel (`VERSION AS OF`).
7. Menyimpan versi awal Parquet pada direktori `version_0`.
8. Membuat versi setelah perubahan pada direktori `version_1`.
9. Membaca kembali kedua versi untuk memeriksa nilai sebelum dan sesudah perubahan.
10. Menyimpan hasil pengujian dalam file JSONL.

### 12.3. Lokasi penyimpanan versi

Iceberg menggunakan mekanisme snapshot bawaan tabel.

Parquet menggunakan pengelolaan versi eksternal melalui direktori HDFS:

```text
/bench/flood/history/parquet/
├── version_0/
└── version_1/
```

### 12.4. Metrik pengujian

Metrik yang dicatat meliputi:

- Jumlah snapshot atau versi data.
- ID snapshot awal dan snapshot terbaru pada Iceberg.
- Waktu operasi UPDATE atau penulisan versi baru.
- Waktu pembacaan metadata riwayat Iceberg.
- Waktu pembacaan versi lama.
- Nilai data sebelum dan sesudah perubahan.
- Status keberhasilan validasi.

### 12.5. Catatan interpretasi

Apache Iceberg menyediakan snapshot dan time travel sebagai fitur bawaan tabel. Sebaliknya, Apache Parquet tidak memiliki mekanisme snapshot tabel bawaan. Pada penelitian ini, riwayat Parquet dikelola secara eksternal melalui penyimpanan dataset pada direktori versi yang berbeda.

Waktu UPDATE Iceberg dan waktu penulisan versi Parquet tidak sepenuhnya setara karena keduanya menggunakan mekanisme perubahan data yang berbeda.

Pengujian ini mencakup pembacaan kondisi data lama, tetapi belum menguji rollback terhadap tabel aktif.

Tabel uji Iceberg menggunakan tabel terpisah sehingga tabel utama `lake.banjir.train` tidak menjadi target perubahan. Direktori versi Parquet disimpan pada HDFS dan dapat dibuat ulang ketika pengujian diulang.

## 13. Hasil pengujian

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


## 14. Ekspor hasil pengujian ke CSV

Hasil benchmark disimpan dalam format JSONL. Untuk memudahkan pemeriksaan dan pengolahan data menggunakan aplikasi spreadsheet, hasil tersebut dapat dikonversi ke format CSV.

Script yang digunakan:

```text
scripts/export_results_csv.py
```

Jalankan dari direktori utama proyek:

```fish
python scripts/export_results_csv.py
```

Script membaca seluruh file JSONL dalam direktori `results/runs/` dan mengelompokkan hasil berdasarkan jenis pengujian.

Hasil konversi disimpan dalam:

```text
results/csv/
├── all_runs.csv
├── benchmark.csv
├── changes.csv
├── history.csv
├── other.csv
└── export_summary.json
```

File `other.csv` hanya dibuat apabila ditemukan hasil pengujian yang tidak termasuk kategori benchmark, changes, atau history.

Keterangan:

- `all_runs.csv`: seluruh record hasil pengujian.
- `benchmark.csv`: hasil pengujian analitik.
- `changes.csv`: hasil operasi INSERT, UPDATE, dan DELETE.
- `history.csv`: hasil pengujian pengelolaan riwayat perubahan.
- `export_summary.json`: informasi jumlah record yang berhasil diekspor.

CSV merupakan hasil konversi struktur JSONL dan belum menjadi ringkasan statistik. Penghitungan median, rata-rata, dan perbandingan kinerja dilakukan pada tahap analisis data.

File JSONL asli harus tetap dipertahankan sebagai sumber data eksperimen.


## 15. Urutan singkat dari awal

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
11. Jalankan pengujian `change history`.
12. Konversi hasil JSONL ke CSV.
13. Periksa dan amankan hasil pada `results/runs/` dan `results/csv/`.


Untuk sesi berikutnya saat data sudah ada:

```fish
cd ~/Documents/lakehouse_banjir_project
docker compose up -d
docker compose ps -a
```

Kemudian jalankan mode yang diperlukan. Jangan jalankan `init` kecuali memang ingin menginisialisasi ulang data.

## 16. Menghentikan layanan

Hentikan container tanpa menghapus volume:

```fish
docker compose down
```

Nyalakan kembali:

```fish
docker compose up -d
```

**Jangan gunakan `docker compose down -v` untuk penghentian biasa.** Opsi `-v` menghapus named volume yang dapat berisi data MinIO/HDFS.

## 17. Troubleshooting

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

## 18. Ringkasan versi dan lokasi

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
