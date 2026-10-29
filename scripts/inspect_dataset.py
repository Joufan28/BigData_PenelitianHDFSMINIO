import csv
import hashlib
from pathlib import Path

data_dir = Path("data/raw")

for path in sorted(data_dir.glob("*.csv")):
    print("\n" + "=" * 65)
    print(f"FILE: {path.name}")
    print(f"UKURAN: {path.stat().st_size / (1024**2):.2f} MB")

    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)

    print(f"SHA-256: {digest.hexdigest()}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            print("File kosong.")
            continue
        print(f"JUMLAH KOLOM: {len(header)}")
        print(f"NAMA KOLOM: {header}")
        print("CONTOH 3 BARIS:")
        for i, row in enumerate(reader):
            if i >= 3:
                break
            print(row)
