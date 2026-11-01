
#!/usr/bin/env python3

import os
import time

from minio import Minio
from minio.error import S3Error


ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "lakeadmin")
SECRET_KEY = os.getenv(
    "MINIO_ROOT_PASSWORD",
    "change-this-password",
)

BUCKET_NAME = "lakehouse"


def main():
    client = Minio(
        ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False,
    )

    for attempt in range(10):
        try:
            if not client.bucket_exists(BUCKET_NAME):
                client.make_bucket(BUCKET_NAME)
                print(
                    f"Bucket {BUCKET_NAME} berhasil dibuat.",
                    flush=True,
                )
            else:
                print(
                    f"Bucket {BUCKET_NAME} sudah tersedia.",
                    flush=True,
                )

            return

        except Exception as error:
            print(
                f"Percobaan {attempt + 1}/10 gagal: {error}",
                flush=True,
            )
            if attempt == 9:
                raise

            time.sleep(5)


if __name__ == "__main__":
    main()
