
import os
import json
import time
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F


# ============================================================
# KONFIGURASI
# ============================================================

ICEBERG_SOURCE = "lake.banjir.train"
ICEBERG_TEST = "lake.banjir.history_test"

HDFS_CURRENT = "hdfs://namenode:8020/bench/flood/current"
HDFS_HISTORY = "hdfs://namenode:8020/bench/flood/history"

RESULT_DIR = "/app/results/runs"


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark():
    return (
        SparkSession.builder
        .appName("LakehouseBanjirChangeHistory")
        .master("local[2]")
        .config("spark.driver.memory", "1800m")
        .config("spark.executor.memory", "1800m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "false")

        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lake",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config(
            "spark.sql.catalog.lake.warehouse",
            "s3a://lakehouse/warehouse",
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ROOT_USER", "lakeadmin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv(
                "MINIO_ROOT_PASSWORD",
                "change-this-password",
            ),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


# ============================================================
# PENYIMPANAN HASIL
# ============================================================

def save_result(result):
    os.makedirs(RESULT_DIR, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    result_file = os.path.join(
        RESULT_DIR,
        f"history_{timestamp}.jsonl",
    )

    result["mode"] = "history"
    result["timestamp_utc"] = datetime.now(
        timezone.utc
    ).isoformat()

    with open(result_file, "a", encoding="utf-8") as file:
        file.write(
            json.dumps(
                result,
                ensure_ascii=False,
                default=str,
            ) + "\n"
        )

    print(json.dumps(
        result,
        ensure_ascii=False,
        indent=2,
        default=str,
    ))

    print(f"Hasil disimpan: {result_file}", flush=True)


# ============================================================
# UTILITAS HDFS
# ============================================================

def delete_hdfs_path(spark, path_string):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()

    path = jvm.org.apache.hadoop.fs.Path(path_string)
    filesystem = path.getFileSystem(conf)

    if filesystem.exists(path):
        if not filesystem.delete(path, True):
            raise RuntimeError(
                f"Gagal menghapus direktori: {path_string}"
            )


# ============================================================
# UTILITAS SNAPSHOT ICEBERG
# ============================================================

def get_latest_snapshot_id(spark):
    history_table = f"{ICEBERG_TEST}.history"

    row = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {history_table}
        ORDER BY made_current_at DESC
        LIMIT 1
        """
    ).first()

    if row is None:
        raise RuntimeError(
            "Snapshot Iceberg tidak ditemukan."
        )

    return int(row["snapshot_id"])


# ============================================================
# PENGUJIAN RIWAYAT PERUBAHAN
# ============================================================

def run_history_test(spark):

    print(
        "Memulai pengujian riwayat perubahan data...",
        flush=True,
    )

    # --------------------------------------------------------
    # 1. MEMBACA DATA SUMBER
    # --------------------------------------------------------

    iceberg_source = spark.table(ICEBERG_SOURCE)
    parquet_source = spark.read.parquet(HDFS_CURRENT)

    iceberg_rows = iceberg_source.count()
    parquet_rows = parquet_source.count()

    if iceberg_rows != parquet_rows:
        raise RuntimeError(
            "Jumlah record sumber berbeda: "
            f"Iceberg={iceberg_rows}, "
            f"Parquet={parquet_rows}"
        )

    # Memilih ID terkecil agar pemilihan record deterministik.
    target = (
        iceberg_source
        .agg(F.min("id").alias("test_id"))
        .first()
    )

    test_id = int(target["test_id"])

    original_row = (
        iceberg_source
        .where(F.col("id") == test_id)
        .select("FloodProbability")
        .first()
    )

    original_value = float(original_row[0])

    updated_value = (
        0.501
        if abs(original_value - 0.5) < 1e-12
        else 0.5
    )

    print(f"ID pengujian: {test_id}", flush=True)
    print(f"Nilai awal: {original_value}", flush=True)
    print(f"Nilai baru: {updated_value}", flush=True)

    # --------------------------------------------------------
    # 2. ICEBERG: MEMBUAT TABEL UJI DAN SNAPSHOT AWAL
    # --------------------------------------------------------

    spark.sql(
        f"DROP TABLE IF EXISTS {ICEBERG_TEST}"
    )

    (
        iceberg_source.writeTo(ICEBERG_TEST)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .create()
    )

    initial_snapshot_id = get_latest_snapshot_id(spark)

    print(
        f"Snapshot awal Iceberg: {initial_snapshot_id}",
        flush=True,
    )

    # --------------------------------------------------------
    # 3. ICEBERG: MELAKUKAN UPDATE
    # --------------------------------------------------------

    start = time.perf_counter()

    spark.sql(
        f"""
        UPDATE {ICEBERG_TEST}
        SET FloodProbability = {updated_value}
        WHERE id = {test_id}
        """
    )

    iceberg_update_seconds = (
        time.perf_counter() - start
    )

    updated_snapshot_id = get_latest_snapshot_id(spark)

    # --------------------------------------------------------
    # 4. ICEBERG: MEMERIKSA RIWAYAT SNAPSHOT
    # --------------------------------------------------------

    history_table = f"{ICEBERG_TEST}.history"

    start = time.perf_counter()

    history_rows = spark.sql(
        f"""
        SELECT *
        FROM {history_table}
        """
    ).collect()

    iceberg_history_seconds = (
        time.perf_counter() - start
    )

    snapshot_count = len(history_rows)

    if initial_snapshot_id == updated_snapshot_id:
        raise RuntimeError(
            "Snapshot tidak berubah setelah UPDATE."
        )

    # --------------------------------------------------------
    # 5. ICEBERG: MEMBACA SNAPSHOT LAMA
    # --------------------------------------------------------

    start = time.perf_counter()

    old_iceberg_row = spark.sql(
        f"""
        SELECT FloodProbability
        FROM {ICEBERG_TEST}
        VERSION AS OF {initial_snapshot_id}
        WHERE id = {test_id}
        """
    ).first()

    iceberg_old_read_seconds = (
        time.perf_counter() - start
    )

    current_iceberg_row = (
        spark.table(ICEBERG_TEST)
        .where(F.col("id") == test_id)
        .select("FloodProbability")
        .first()
    )

    if old_iceberg_row is None:
        raise RuntimeError(
            "Snapshot lama Iceberg tidak dapat dibaca."
        )

    if current_iceberg_row is None:
        raise RuntimeError(
            "Data terbaru Iceberg tidak ditemukan."
        )

    iceberg_old_value = float(old_iceberg_row[0])
    iceberg_current_value = float(current_iceberg_row[0])

    # --------------------------------------------------------
    # 6. PARQUET: MEMBUAT VERSI AWAL DAN VERSI BARU
    # --------------------------------------------------------

    parquet_base = f"{HDFS_HISTORY}/parquet"

    parquet_v0 = f"{parquet_base}/version_0"
    parquet_v1 = f"{parquet_base}/version_1"

    # Menghapus hasil pengujian Parquet sebelumnya.
    delete_hdfs_path(spark, parquet_base)

    # Versi awal disimpan sebelum timer UPDATE.
    parquet_source.write.mode("overwrite").parquet(
        parquet_v0
    )

    # --------------------------------------------------------
    # 7. PARQUET: UPDATE DENGAN MENULIS VERSI BARU
    # --------------------------------------------------------

    start = time.perf_counter()

    parquet_updated = parquet_source.withColumn(
        "FloodProbability",
        F.when(
            F.col("id") == test_id,
            F.lit(updated_value),
        ).otherwise(
            F.col("FloodProbability")
        ),
    )

    parquet_updated.write.mode("overwrite").parquet(
        parquet_v1
    )

    parquet_update_seconds = (
        time.perf_counter() - start
    )

    # --------------------------------------------------------
    # 8. PARQUET: MEMBACA VERSI LAMA
    # --------------------------------------------------------

    start = time.perf_counter()

    old_parquet_row = (
        spark.read.parquet(parquet_v0)
        .where(F.col("id") == test_id)
        .select("FloodProbability")
        .first()
    )

    parquet_old_read_seconds = (
        time.perf_counter() - start
    )

    current_parquet_row = (
        spark.read.parquet(parquet_v1)
        .where(F.col("id") == test_id)
        .select("FloodProbability")
        .first()
    )

    if old_parquet_row is None:
        raise RuntimeError(
            "Versi lama Parquet tidak dapat dibaca."
        )

    if current_parquet_row is None:
        raise RuntimeError(
            "Versi baru Parquet tidak ditemukan."
        )

    parquet_old_value = float(old_parquet_row[0])
    parquet_current_value = float(current_parquet_row[0])

    # --------------------------------------------------------
    # 9. VALIDASI HASIL
    # --------------------------------------------------------

    tolerance = 1e-12

    passed = (
        iceberg_rows == parquet_rows
        and snapshot_count >= 2
        and initial_snapshot_id != updated_snapshot_id

        and abs(iceberg_old_value - original_value)
        <= tolerance

        and abs(iceberg_current_value - updated_value)
        <= tolerance

        and abs(parquet_old_value - original_value)
        <= tolerance

        and abs(parquet_current_value - updated_value)
        <= tolerance
    )

    # --------------------------------------------------------
    # 10. MENYIMPAN HASIL
    # --------------------------------------------------------

    result = {
        "operation": "change_history",
        "dataset_rows": iceberg_rows,
        "test_id": test_id,
        "original_value": original_value,
        "updated_value": updated_value,

        "iceberg": {
            "initial_snapshot_id": initial_snapshot_id,
            "updated_snapshot_id": updated_snapshot_id,
            "snapshot_count": snapshot_count,
            "update_seconds": iceberg_update_seconds,
            "history_read_seconds": iceberg_history_seconds,
            "old_snapshot_read_seconds": iceberg_old_read_seconds,
            "old_value": iceberg_old_value,
            "current_value": iceberg_current_value,
        },

        "parquet": {
            "version_count": 2,
            "version_0_path": parquet_v0,
            "version_1_path": parquet_v1,
            "update_and_write_version_seconds": parquet_update_seconds,
            "old_version_read_seconds": parquet_old_read_seconds,
            "old_value": parquet_old_value,
            "current_value": parquet_current_value,
            "note": (
                "Versi dikelola melalui direktori HDFS terpisah. "
                "Parquet tidak memiliki snapshot tabel bawaan."
            ),
        },

        "passed": passed,

        "scope_note": (
            "Pengujian mencakup pembacaan kondisi lama. "
            "Rollback tabel aktif belum diuji."
        ),
    }

    save_result(result)

    if not passed:
        raise RuntimeError(
            "Validasi pengujian riwayat perubahan gagal."
        )

    # Menghapus tabel uji Iceberg, bukan tabel utama.
    spark.sql(
        f"DROP TABLE IF EXISTS {ICEBERG_TEST}"
    )

    print(
        "PENGUJIAN RIWAYAT PERUBAHAN BERHASIL.",
        flush=True,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    spark = create_spark()

    try:
        run_history_test(spark)

    except Exception:
        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
