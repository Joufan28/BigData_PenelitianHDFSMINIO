import os
import sys
import json
import time
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    DoubleType,
)

# ============================================================
# KONFIGURASI
# ============================================================

INPUT = "/app/data/raw/train.csv"

ICEBERG_TABLE = "lake.banjir.train"
HDFS_CURRENT = "hdfs://namenode:8020/bench/flood/current"
HDFS_CHANGES = "hdfs://namenode:8020/bench/flood/changes"

# Setiap eksekusi mendapatkan file hasil tersendiri.
RESULT_DIR = "/app/results/runs"
RUN_ID = None
RUN_MODE = None
RESULT_FILE = None

EXPECTED_ROWS = 1_117_957

FEATURES = [
    "MonsoonIntensity",
    "TopographyDrainage",
    "RiverManagement",
    "Deforestation",
    "Urbanization",
    "ClimateChange",
    "DamsQuality",
    "Siltation",
    "AgriculturalPractices",
    "Encroachments",
    "IneffectiveDisasterPreparedness",
    "DrainageSystems",
    "CoastalVulnerability",
    "Landslides",
    "Watersheds",
    "DeterioratingInfrastructure",
    "PopulationScore",
    "WetlandLoss",
    "InadequatePlanning",
    "PoliticalFactors",
]

SCHEMA = StructType(
    [StructField("id", LongType(), False)]
    + [StructField(name, IntegerType(), True) for name in FEATURES]
    + [StructField("FloodProbability", DoubleType(), True)]
)


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark():
    return (
        SparkSession.builder
        .appName("LakehouseBanjirBenchmark")
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
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


# ============================================================
# UTILITAS HASIL BENCHMARK
# ============================================================

def initialize_run(mode):
    """
    Membuat ID dan file baru untuk setiap eksekusi.

    Contoh:
        results/runs/run_001.jsonl
        results/runs/run_002.jsonl
    """
    global RUN_ID, RUN_MODE, RESULT_FILE

    os.makedirs(RESULT_DIR, exist_ok=True)

    existing_runs = []

    for filename in os.listdir(RESULT_DIR):
        if filename.startswith("run_") and filename.endswith(".jsonl"):
            number = filename[4:-6]

            if number.isdigit():
                existing_runs.append(int(number))

    next_run = max(existing_runs, default=0) + 1

    RUN_ID = f"run_{next_run:03d}"
    RUN_MODE = mode

    RESULT_FILE = os.path.join(
        RESULT_DIR,
        f"{RUN_ID}.jsonl",
    )

    print("=" * 60, flush=True)
    print(f"ID eksekusi : {RUN_ID}", flush=True)
    print(f"Mode        : {RUN_MODE}", flush=True)
    print(f"File hasil  : {RESULT_FILE}", flush=True)
    print("=" * 60, flush=True)


def save_result(record):
    if RESULT_FILE is None or RUN_ID is None:
        raise RuntimeError(
            "Run belum diinisialisasi. "
            "Panggil initialize_run() terlebih dahulu."
        )

    record["run_id"] = RUN_ID
    record["mode"] = RUN_MODE
    record["timestamp_utc"] = datetime.now(
        timezone.utc
    ).isoformat()

    with open(RESULT_FILE, "a", encoding="utf-8") as file:
        file.write(
            json.dumps(
                record,
                ensure_ascii=False,
                default=str,
            ) + "\n"
        )

    print(
        json.dumps(
            record,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        flush=True,
    )


def read_source(spark):
    if not os.path.isfile(INPUT):
        raise FileNotFoundError(
            f"Dataset tidak ditemukan: {INPUT}"
        )

    return (
        spark.read
        .option("header", True)
        .option("mode", "FAILFAST")
        .schema(SCHEMA)
        .csv("file://" + INPUT)
    )


def read_iceberg(spark, table=ICEBERG_TABLE):
    return spark.table(table)


def read_parquet(spark, path=HDFS_CURRENT):
    return spark.read.parquet(path)


# ============================================================
# MODE INIT
# ============================================================

def init_data(spark):
    source = read_source(spark)
    rows = source.count()

    if rows != EXPECTED_ROWS:
        raise RuntimeError(
            f"Jumlah baris sumber salah: {rows}; "
            f"ekspektasi {EXPECTED_ROWS}"
        )

    (
        source.writeTo(ICEBERG_TABLE)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    source.write.mode("overwrite").parquet(HDFS_CURRENT)

    iceberg_rows = read_iceberg(spark).count()
    parquet_rows = read_parquet(spark).count()

    if (
        iceberg_rows != EXPECTED_ROWS
        or parquet_rows != EXPECTED_ROWS
    ):
        raise RuntimeError(
            "Validasi init gagal: "
            f"Iceberg={iceberg_rows}, "
            f"Parquet={parquet_rows}"
        )

    save_result({
        "operation": "init",
        "expected_rows": EXPECTED_ROWS,
        "iceberg_rows": iceberg_rows,
        "parquet_rows": parquet_rows,
        "passed": True,
    })

    print(
        f"INIT SELESAI: {rows:,} baris ditulis ke kedua sistem.",
        flush=True,
    )


# ============================================================
# MODE VALIDATE
# ============================================================

def validate_data(spark):
    systems = {}

    for name, df in [
        ("iceberg", read_iceberg(spark)),
        ("parquet", read_parquet(spark)),
    ]:
        stats = df.agg(
            F.count("*").alias("rows"),
            F.countDistinct("id").alias("distinct_ids"),
            F.sum("FloodProbability").alias("probability_sum"),
            F.avg("FloodProbability").alias("probability_avg"),
        ).first().asDict()

        systems[name] = {
            "rows": int(stats["rows"]),
            "distinct_ids": int(stats["distinct_ids"]),
            "probability_sum": float(stats["probability_sum"]),
            "probability_avg": float(stats["probability_avg"]),
        }

    iceberg = systems["iceberg"]
    parquet = systems["parquet"]

    passed = (
        iceberg["rows"] == EXPECTED_ROWS
        and parquet["rows"] == EXPECTED_ROWS
        and iceberg["distinct_ids"] == EXPECTED_ROWS
        and parquet["distinct_ids"] == EXPECTED_ROWS
        and abs(
            iceberg["probability_sum"]
            - parquet["probability_sum"]
        ) < 1e-6
        and abs(
            iceberg["probability_avg"]
            - parquet["probability_avg"]
        ) < 1e-9
    )

    save_result({
        "operation": "validation",
        "systems": systems,
        "passed": passed,
    })

    if not passed:
        raise RuntimeError("Validasi kedua sistem gagal.")

    print("VALIDASI LULUS.", flush=True)


# ============================================================
# MODE BENCHMARK ANALITIK
# ============================================================

def analytics_queries(df):
    return {
        "count_all": lambda: df.count(),

        "probability_avg": lambda: (
            df.agg(F.avg("FloodProbability")).first()[0]
        ),

        "monsoon_group": lambda: (
            df.groupBy("MonsoonIntensity")
            .agg(
                F.count("*").alias("n"),
                F.avg("FloodProbability").alias(
                    "avg_probability"
                ),
            )
            .orderBy("MonsoonIntensity")
            .collect()
        ),

        "feature_summary": lambda: (
            df.agg(
                F.avg("MonsoonIntensity").alias("monsoon_avg"),
                F.avg("Urbanization").alias("urbanization_avg"),
                F.avg("Deforestation").alias("deforestation_avg"),
                F.avg("FloodProbability").alias("probability_avg"),
                F.min("FloodProbability").alias("probability_min"),
                F.max("FloodProbability").alias("probability_max"),
            ).first()
        ),

        "correlation": lambda: (
            df.stat.corr(
                "MonsoonIntensity",
                "FloodProbability",
            )
        ),
    }


def benchmark_analytics(spark):
    for system in ("iceberg", "parquet"):
        df = (
            read_iceberg(spark)
            if system == "iceberg"
            else read_parquet(spark)
        )

        queries = analytics_queries(df)

        for query_name, query_fn in queries.items():
            print(
                f"Benchmark {system}/{query_name}",
                flush=True,
            )

            # Warm-up tidak masuk hasil pengukuran.
            query_fn()

            samples = []

            for repeat in range(1, 6):
                start = time.perf_counter()
                query_fn()
                elapsed = time.perf_counter() - start

                samples.append(elapsed)

                save_result({
                    "operation": "analytics",
                    "system": system,
                    "query": query_name,
                    "repeat": repeat,
                    "seconds": elapsed,
                    "passed": True,
                })

            ordered = sorted(samples)
            median = ordered[len(ordered) // 2]

            save_result({
                "operation": "analytics_summary",
                "system": system,
                "query": query_name,
                "samples_seconds": samples,
                "median_seconds": median,
                "passed": True,
            })


# ============================================================
# MODE PERUBAHAN DATA
# ============================================================

def change_test(spark, system, operation):
    """
    Uji INSERT, UPDATE, DELETE.

    UPDATE dan DELETE Parquet ditulis ke staging directory.
    Setelah penulisan berhasil, direktori sumber diganti.
    """

    if operation not in ("insert", "update", "delete"):
        raise ValueError(
            f"Operasi tidak dikenal: {operation}"
        )

    suffix = f"{system}_{operation}"
    table = f"lake.banjir.test_{suffix}"
    path = f"{HDFS_CHANGES}/{suffix}"
    temp_path = f"{path}_tmp"

    source = read_source(spark)

    # Siapkan dataset terisolasi untuk setiap operasi.
    if system == "iceberg":
        (
            source.writeTo(table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        df = spark.table(table)

    elif system == "parquet":
        source.write.mode("overwrite").parquet(path)
        df = spark.read.parquet(path)

    else:
        raise ValueError(
            f"Sistem tidak dikenal: {system}"
        )

    before = df.count()

    first = (
        df.select("id", "FloodProbability")
        .orderBy("id")
        .first()
    )

    max_id = df.agg(F.max("id")).first()[0]

    if first is None or max_id is None:
        raise RuntimeError("Dataset uji kosong.")

    test_id = int(first["id"])
    new_id = int(max_id) + 1
    original_probability = float(first["FloodProbability"])

    update_value = (
        0.501
        if abs(original_probability - 0.5) < 1e-12
        else 0.5
    )

    start = time.perf_counter()

    if system == "iceberg":
        if operation == "insert":
            (
                df.where(F.col("id") == test_id)
                .withColumn(
                    "id",
                    F.lit(new_id).cast("long"),
                )
                .writeTo(table)
                .append()
            )

        elif operation == "update":
            spark.sql(
                f"UPDATE {table} "
                f"SET FloodProbability = {update_value} "
                f"WHERE id = {test_id}"
            )

        elif operation == "delete":
            spark.sql(
                f"DELETE FROM {table} "
                f"WHERE id = {test_id}"
            )

    elif system == "parquet":
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()

        original = jvm.org.apache.hadoop.fs.Path(path)
        temp = jvm.org.apache.hadoop.fs.Path(temp_path)
        fs = original.getFileSystem(conf)

        # Bersihkan staging yang tersisa.
        if fs.exists(temp) and not fs.delete(temp, True):
            raise RuntimeError(
                f"Gagal membersihkan staging: {temp_path}"
            )

        if operation == "insert":
            (
                df.where(F.col("id") == test_id)
                .withColumn(
                    "id",
                    F.lit(new_id).cast("long"),
                )
                .write.mode("append")
                .parquet(path)
            )

        else:
            if operation == "update":
                changed_df = df.withColumn(
                    "FloodProbability",
                    F.when(
                        F.col("id") == test_id,
                        F.lit(update_value),
                    ).otherwise(F.col("FloodProbability")),
                )
            else:
                changed_df = df.where(
                    F.col("id") != test_id
                )

            # Tulis ke lokasi berbeda untuk menghindari
            # SparkFileNotFoundException.
            changed_df.write.mode("overwrite").parquet(
                temp_path
            )

            if not fs.exists(temp):
                raise RuntimeError(
                    f"Hasil staging tidak ditemukan: {temp_path}"
                )

            if fs.exists(original):
                if not fs.delete(original, True):
                    raise RuntimeError(
                        f"Gagal menghapus direktori lama: {path}"
                    )

            if not fs.rename(temp, original):
                raise RuntimeError(
                    f"Gagal memindahkan staging ke: {path}"
                )

    elapsed = time.perf_counter() - start

    # Validasi dilakukan setelah timer berhenti.
    if system == "iceberg":
        result_df = spark.table(table)
    else:
        result_df = spark.read.parquet(path)

    after = result_df.count()

    if operation == "insert":
        expected_rows = before + 1
    elif operation == "delete":
        expected_rows = before - 1
    else:
        expected_rows = before

    if after != expected_rows:
        raise RuntimeError(
            f"{system}/{operation}: jumlah baris salah; "
            f"sebelum={before}, sesudah={after}, "
            f"seharusnya={expected_rows}"
        )

    changed = result_df.where(
        F.col("id") == test_id
    ).first()

    inserted_id_count = None
    final_probability = None

    if operation == "insert":
        inserted_id_count = (
            result_df.where(F.col("id") == new_id).count()
        )

        if inserted_id_count != 1:
            raise RuntimeError(
                f"Validasi INSERT gagal: ID {new_id} "
                f"ditemukan {inserted_id_count} kali."
            )

    elif operation == "update":
        if (
            changed is None
            or abs(
                float(changed["FloodProbability"])
                - update_value
            ) > 1e-12
            or abs(
                original_probability - update_value
            ) <= 1e-12
        ):
            raise RuntimeError("Validasi UPDATE gagal.")

        final_probability = float(
            changed["FloodProbability"]
        )

    elif operation == "delete":
        if changed is not None:
            raise RuntimeError("Validasi DELETE gagal.")

    save_result({
        "operation": operation,
        "system": system,
        "seconds": elapsed,
        "rows_before": before,
        "rows_after": after,
        "test_id": test_id,
        "inserted_id": (
            new_id if operation == "insert" else None
        ),
        "inserted_id_count": inserted_id_count,
        "original_probability": (
            original_probability
            if operation == "update"
            else None
        ),
        "updated_probability": final_probability,
        "passed": True,
        "note": (
            "Validasi di luar timer. UPDATE/DELETE Parquet "
            "menggunakan staging directory."
        ),
    })


def run_changes(spark):
    for system in ("iceberg", "parquet"):
        for operation in ("insert", "update", "delete"):
            print(
                f"Perubahan data: {system}/{operation}",
                flush=True,
            )
            change_test(spark, system, operation)


# ============================================================
# MAIN
# ============================================================

def main():
    valid_modes = (
        "init",
        "validate",
        "benchmark",
        "changes",
    )

    if len(sys.argv) != 2 or sys.argv[1] not in valid_modes:
        print(
            "Penggunaan: benchmark_v2.py "
            "{init|validate|benchmark|changes}",
            file=sys.stderr,
        )
        sys.exit(2)

    mode = sys.argv[1]

    initialize_run(mode)

    spark = create_spark()

    try:
        if mode == "init":
            init_data(spark)
        elif mode == "validate":
            validate_data(spark)
        elif mode == "benchmark":
            benchmark_analytics(spark)
        elif mode == "changes":
            run_changes(spark)

    except Exception:
        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
