#!/usr/bin/env python3
import argparse, csv, json, os, statistics, time
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F, types as T

ICEBERG_TABLE = "lake.banjir.events"
HDFS_CURRENT = "hdfs://namenode:8020/bench/banjir/current"
HDFS_HISTORY = "hdfs://namenode:8020/bench/banjir/history"
INPUT = "/app/data/processed/banjir.csv"
OUT = "/app/results"

def make_spark():
    b = (SparkSession.builder.appName("LakehouseBanjirBenchmark")
         .master("local[2]")
         .config("spark.driver.memory", "1800m")
         .config("spark.executor.memory", "1800m")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.default.parallelism", "2")
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.lake.type", "hadoop")
         .config("spark.sql.catalog.lake.warehouse", "s3a://lakehouse/warehouse")
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "lakeadmin"))
         .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "change-this-password"))
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
         .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO"))
    return b.getOrCreate()

def read_data(spark):
    if not os.path.exists(INPUT):
        raise FileNotFoundError(f"{INPUT} tidak ada. Jalankan scripts/prepare_data.py di host.")
    schema = T.StructType([
        T.StructField("record_id", T.StringType(), False),
        T.StructField("observed_at", T.StringType(), True),
        T.StructField("province", T.StringType(), True),
        T.StructField("district", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("affected_people", T.StringType(), True),
        T.StructField("houses_damaged", T.StringType(), True),
        T.StructField("rainfall_mm", T.StringType(), True),
        T.StructField("water_level_cm", T.StringType(), True),
        T.StructField("risk_level", T.StringType(), True),
    ])
    df = spark.read.option("header", True).option("mode", "FAILFAST").schema(schema).csv(INPUT)
    return (df.withColumn("observed_at", F.to_timestamp("observed_at"))
        .withColumn("affected_people", F.col("affected_people").cast("long"))
        .withColumn("houses_damaged", F.col("houses_damaged").cast("long"))
        .withColumn("rainfall_mm", F.col("rainfall_mm").cast("double"))
        .withColumn("water_level_cm", F.col("water_level_cm").cast("double"))
        .withColumn("province", F.coalesce("province", F.lit("")))
        .withColumn("district", F.coalesce("district", F.lit("")))
        .withColumn("event_type", F.coalesce("event_type", F.lit("tidak_diketahui")))
        .withColumn("risk_level", F.coalesce("risk_level", F.lit("tidak_diketahui"))))

def ensure_namespace(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.banjir")

def init(spark):
    df = read_data(spark).cache()
    n = df.count()
    if n == 0: raise ValueError("Dataset kosong setelah normalisasi.")
    ensure_namespace(spark)
    df.writeTo(ICEBERG_TABLE).using("iceberg").createOrReplace()
    df.write.mode("overwrite").parquet(HDFS_CURRENT)
    print(f"INIT OK: {n:,} rows written to Iceberg and HDFS Parquet.")
    print("Pastikan jumlah baris dan agregat cocok sebelum benchmark.")
    df.unpersist()

def time_action(label, fn, repeats=5):
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn()
        if result is not None:
            result  # force evaluation by caller action
        times.append(time.perf_counter() - t0)
    return {"operation": label, "repeats_seconds": times, "median_seconds": statistics.median(times)}

def save_result(obj):
    os.makedirs(OUT, exist_ok=True)
    obj["timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    path = os.path.join(OUT, "benchmark_results.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    print(json.dumps(obj, ensure_ascii=False, indent=2))

def run_query_set(spark, system):
    if system == "iceberg":
        df = spark.table(ICEBERG_TABLE)
    else:
        df = spark.read.parquet(HDFS_CURRENT)
    df.createOrReplaceTempView("events")
    queries = {
      "count_all": lambda: spark.sql("SELECT COUNT(*) AS n FROM events").collect(),
      "by_region": lambda: spark.sql("SELECT province, COUNT(*) AS n, SUM(affected_people) AS affected FROM events GROUP BY province ORDER BY n DESC").collect(),
      "by_year": lambda: spark.sql("SELECT YEAR(observed_at) AS yr, COUNT(*) AS n FROM events WHERE observed_at IS NOT NULL GROUP BY YEAR(observed_at) ORDER BY yr").collect(),
      "risk_summary": lambda: spark.sql("SELECT risk_level, COUNT(*) AS n FROM events GROUP BY risk_level ORDER BY n DESC").collect(),
    }
    for name, fn in queries.items():
        result = time_action(f"{system}.{name}", fn, 5)
        result["system"] = system
        save_result(result)

def update_test(spark, system):
    # Baseline uses an explicit pre-update Parquet copy as its version-history mechanism.
    if system == "parquet":
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
        src = jvm.org.apache.hadoop.fs.Path(HDFS_CURRENT)
        dst = jvm.org.apache.hadoop.fs.Path(HDFS_HISTORY + "/before_update_" + str(int(time.time())))
        if fs.exists(dst): fs.delete(dst, True)
        if not fs.exists(src): raise RuntimeError("Baseline belum diinisialisasi.")
        fs.mkdirs(dst)
        fs.copyToLocalFile(False, src, dst) if False else None
        # Recursive HDFS copy is done via FileUtil.
        jvm.org.apache.hadoop.fs.FileUtil.copy(fs, src, fs, dst, False, conf)
        before = spark.read.parquet(HDFS_CURRENT)
        first = before.select("record_id").where(F.col("record_id").isNotNull()).first()
        if not first: raise RuntimeError("Tidak ada record untuk update.")
        rid = first["record_id"]
        updated = before.withColumn("risk_level", F.when(F.col("record_id") == rid, F.lit("benchmark_updated")).otherwise(F.col("risk_level")))
        t0 = time.perf_counter()
        updated.write.mode("overwrite").parquet(HDFS_CURRENT)
        elapsed = time.perf_counter() - t0
        save_result({"system":"parquet","operation":"rewrite_for_update","record_id":rid,"seconds":elapsed})
        # Verify saved historical copy can be read.
        hist_path = dst.toString()
        t0 = time.perf_counter(); historical_count = spark.read.parquet(hist_path).count(); hist_s = time.perf_counter()-t0
        save_result({"system":"parquet","operation":"read_history_copy","historical_rows":historical_count,"seconds":hist_s,"path":hist_path})
    else:
        first = spark.table(ICEBERG_TABLE).select("record_id").where(F.col("record_id").isNotNull()).first()
        if not first: raise RuntimeError("Tidak ada record untuk update.")
        rid = first["record_id"]
        t0 = time.perf_counter()
        spark.sql(f"UPDATE {ICEBERG_TABLE} SET risk_level = 'benchmark_updated' WHERE record_id = {sql_string(rid)}")
        elapsed = time.perf_counter() - t0
        save_result({"system":"iceberg","operation":"update_one_record","record_id":rid,"seconds":elapsed})
        # Query Iceberg snapshot metadata and measure time-travel read.
        snaps = spark.sql(f"SELECT committed_at, snapshot_id, operation FROM {ICEBERG_TABLE}.snapshots ORDER BY committed_at")
        snap_rows = snaps.collect()
        save_result({"system":"iceberg","operation":"snapshot_count","count":len(snap_rows)})
        if len(snap_rows) >= 2:
            old_id = snap_rows[-2]["snapshot_id"]
            t0 = time.perf_counter()
            n = spark.sql(f"SELECT COUNT(*) AS n FROM {ICEBERG_TABLE} VERSION AS OF {old_id}").first()["n"]
            save_result({"system":"iceberg","operation":"time_travel_read","snapshot_id":int(old_id),"historical_rows":int(n),"seconds":time.perf_counter()-t0})

def sql_string(s):
    return "'" + s.replace("'", "''") + "'"

def validate(spark):
    a = spark.table(ICEBERG_TABLE)
    b = spark.read.parquet(HDFS_CURRENT)
    ca, cb = a.count(), b.count()
    agg_a = a.agg(F.sum("affected_people").alias("affected"), F.countDistinct("record_id").alias("ids")).first()
    agg_b = b.agg(F.sum("affected_people").alias("affected"), F.countDistinct("record_id").alias("ids")).first()
    out = {"operation":"validation","iceberg_rows":ca,"parquet_rows":cb,
           "iceberg_distinct_ids":agg_a["ids"],"parquet_distinct_ids":agg_b["ids"],
           "iceberg_affected_sum":agg_a["affected"],"parquet_affected_sum":agg_b["affected"],
           "counts_match":ca==cb,"distinct_ids_match":agg_a["ids"]==agg_b["ids"],
           "affected_sum_match":agg_a["affected"]==agg_b["affected"]}
    save_result(out)
    if not (out["counts_match"] and out["distinct_ids_match"] and out["affected_sum_match"]):
        raise RuntimeError("Validasi gagal: jangan jalankan benchmark sebelum kedua sistem konsisten.")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["init","validate","benchmark","update-iceberg","update-parquet"])
    args = p.parse_args()
    spark = make_spark()
    try:
        if args.mode == "init": init(spark)
        elif args.mode == "validate": validate(spark)
        elif args.mode == "benchmark":
            validate(spark)
            run_query_set(spark, "iceberg")
            run_query_set(spark, "parquet")
        elif args.mode == "update-iceberg": update_test(spark, "iceberg")
        elif args.mode == "update-parquet": update_test(spark, "parquet")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
