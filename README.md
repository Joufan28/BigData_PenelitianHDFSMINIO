## Struktur Proyek

- `docker-compose.yml`: konfigurasi layanan MinIO, HDFS, dan Spark.
- `Dockerfile.spark`: lingkungan Spark dan dependensi yang digunakan.
- `scripts/prepare_data.py`: persiapan dan normalisasi dataset.
- `scripts/benchmark_v2.py`: inisialisasi, validasi, benchmark analitik, dan pengujian perubahan data.
- `scripts/analyze_benchmark.py`: analisis hasil pengujian JSONL menjadi CSV.
- `scripts/create_benchmark_charts.py`: pembuatan grafik perbandingan.
- `results/runs/`: hasil pengujian dalam format JSONL, satu file untuk setiap eksekusi.
- `results/report/`: tabel hasil analisis dan grafik untuk laporan.

## Menjalankan Pengujian

Pastikan layanan Docker dan dataset yang diperlukan sudah tersedia sebelum menjalankan pengujian. Jalankan satu pengujian pada satu waktu.

### 1. Validasi dataset

```bash
docker compose run --rm --no-deps \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  --entrypoint /usr/bin/python3 \
  spark /app/scripts/benchmark_v2.py validate
```

### 2. Benchmark query analitik

```bash
docker compose run --rm --no-deps \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  --entrypoint /usr/bin/python3 \
  spark /app/scripts/benchmark_v2.py benchmark
```

### 3. Benchmark perubahan data

```bash
docker compose run --rm --no-deps \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  --entrypoint /usr/bin/python3 \
  spark /app/scripts/benchmark_v2.py changes
```

**Peringatan:** jalankan mode `init` hanya jika dataset atau tabel utama memang perlu dibuat ulang dan kamu siap mengganti data yang ada.

## Analisis Hasil

Setelah file JSONL tersedia di `results/runs/`, jalankan:

```bash
python3 scripts/analyze_benchmark.py
```

Hasil analisis disimpan di:

- `results/report/benchmark_raw.csv`
- `results/report/benchmark_summary.csv`
- `results/report/benchmark_comparison.csv`

Buat grafik perbandingan:

```bash
python3 scripts/create_benchmark_charts.py
```

Grafik disimpan di `results/report/figures/`.

Setiap eksekusi pengujian menghasilkan file JSONL tersendiri. Simpan file hasil asli agar hasil analisis dapat ditelusuri dan direproduksi.

## Ringkasan Hasil Benchmark

Pengujian dilakukan pada dua format penyimpanan, Apache Iceberg dan Parquet, menggunakan lima query analitik serta tiga operasi perubahan data.

### Metodologi

- Setiap query analitik dijalankan satu kali sebagai warm-up, tanpa memasukkan durasinya ke hasil pengukuran.
- Setiap query diukur sebanyak lima kali untuk masing-masing format.
- Durasi diukur menggunakan `time.perf_counter()`.
- Median dari lima pengukuran digunakan sebagai ringkasan waktu eksekusi.
- Hasil mentah disimpan dalam JSONL dan dianalisis menjadi CSV serta grafik perbandingan.

### Hasil Utama

- **Query analitik:** Parquet mencatat waktu median lebih rendah pada kelima query, dengan pengurangan waktu sekitar 33,8%–46,9% dibandingkan Iceberg.
- **INSERT:** Parquet lebih cepat, dengan waktu sekitar 0,222 detik dibandingkan 0,731 detik pada Iceberg.
- **UPDATE:** Iceberg lebih cepat, dengan waktu sekitar 1,806 detik dibandingkan 2,466 detik pada Parquet.
- **DELETE:** Iceberg lebih cepat, dengan waktu sekitar 1,193 detik dibandingkan 2,263 detik pada Parquet.

### Validasi dan Keterbatasan

Seluruh 67 record dari tiga eksekusi pengujian yang tersimpan berstatus berhasil (`passed=True`). Hasil tersebut menunjukkan bahwa pemeriksaan yang dicatat oleh skrip lolos; bukan bukti bahwa seluruh kemungkinan kondisi telah diuji.

Kesimpulan performa berlaku untuk dataset, konfigurasi, lingkungan, dan implementasi yang digunakan dalam eksperimen ini. Hasil tidak dapat langsung digeneralisasi ke seluruh ukuran dataset, konfigurasi cluster, atau pola beban kerja.

File hasil mentah dan hasil analisis perlu dipertahankan untuk mendukung penelusuran dan reproduksi eksperimen.