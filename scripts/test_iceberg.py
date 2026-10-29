import os
from pyspark.sql import SparkSession

user = os.environ.get("MINIO_ROOT_USER", "lakeadmin")
password = os.environ.get(
    "MINIO_ROOT_PASSWORD", "change-this-password"
)

spark = (
    SparkSession.builder
    .appName("LakehouseBanjir-Iceberg-SmokeTest")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://lakehouse/warehouse",
    )
    .config(
        "spark.sql.catalog.lakehouse.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", user)
    .config("spark.hadoop.fs.s3a.secret.key", password)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.benchmark")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.benchmark.smoke_test (
            id BIGINT,
            message STRING
        ) USING iceberg
        """
    )

    spark.sql(
        """
        INSERT INTO lakehouse.benchmark.smoke_test
        VALUES (1, 'Iceberg tersambung ke MinIO')
        """
    )

    spark.sql("SELECT * FROM lakehouse.benchmark.smoke_test").show(
        truncate=False
    )

    print("BERHASIL: tabel Iceberg dapat ditulis dan dibaca.")
finally:
    spark.stop()
