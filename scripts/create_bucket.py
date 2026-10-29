import os
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key=os.getenv("MINIO_ROOT_USER", "lakeadmin"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "change-this-password"),
    secure=False,
)

bucket = "lakehouse"

if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f"Berhasil membuat bucket: {bucket}")
else:
    print(f"Bucket sudah tersedia: {bucket}")