from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("LakehouseBanjir-Parquet-SmokeTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.createDataFrame(
        [
            (1, "Uji HDFS Parquet"),
            (2, "Data risiko banjir"),
        ],
        ["id", "message"],
    )

    path = "hdfs://namenode:8020/benchmark/smoke_test/parquet"

    df.write.mode("overwrite").parquet(path)

    result = spark.read.parquet(path)
    result.show(truncate=False)

    count = result.count()
    assert count == 2, f"Jumlah baris tidak sesuai: {count}"

    print("BERHASIL: Parquet dapat ditulis dan dibaca dari HDFS.")
finally:
    spark.stop()
