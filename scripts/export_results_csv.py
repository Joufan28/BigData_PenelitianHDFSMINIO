
import csv
import json
from pathlib import Path
from datetime import datetime


# Lokasi folder hasil pengujian
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "results" / "runs"
OUTPUT_DIR = BASE_DIR / "results" / "csv"


def flatten(data, parent_key=""):
    """Mengubah dictionary bertingkat menjadi kolom CSV."""
    result = {}

    if not isinstance(data, dict):
        return {parent_key or "value": data}

    for key, value in data.items():
        new_key = f"{parent_key}_{key}" if parent_key else key

        if isinstance(value, dict):
            result.update(flatten(value, new_key))
        elif isinstance(value, list):
            result[new_key] = json.dumps(
                value, ensure_ascii=False
            )
        else:
            result[new_key] = value

    return result


def detect_type(record, filename):
    """Mengidentifikasi jenis pengujian."""
    mode = str(record.get("mode", "")).lower()
    operation = str(record.get("operation", "")).lower()
    name = filename.lower()

    if "history" in mode or "history" in operation or "history" in name:
        return "history"

    if "change" in mode or "change" in operation or "change" in name:
        return "changes"

    if "benchmark" in mode or "benchmark" in name:
        return "benchmark"

    return "other"


def read_jsonl():
    records = []

    if not INPUT_DIR.exists():
        print(f"Folder tidak ditemukan: {INPUT_DIR}")
        return records

    for file_path in sorted(INPUT_DIR.glob("*.jsonl")):
        print(f"Membaca: {file_path.name}")

        with file_path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.strip()

                if not line:
                    continue

                try:
                    record = json.loads(line)

                    if not isinstance(record, dict):
                        print(
                            f"  Lewati baris {line_number}: "
                            "bukan JSON object"
                        )
                        continue

                    record["source_file"] = file_path.name
                    record["source_line"] = line_number
                    record["test_type"] = detect_type(
                        record, file_path.name
                    )

                    records.append(record)

                except json.JSONDecodeError as error:
                    print(
                        f"  JSON error {file_path.name}, "
                        f"baris {line_number}: {error}"
                    )

    return records


def write_csv(records, output_path):
    """Menyimpan kumpulan record ke CSV."""
    if not records:
        print(f"Tidak ada data untuk: {output_path.name}")
        return

    flattened = [flatten(record) for record in records]

    # Gabungkan seluruh nama kolom
    columns = sorted({
        key
        for row in flattened
        for key in row.keys()
    })

    with output_path.open(
        "w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=columns,
            extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(flattened)

    print(
        f"Berhasil: {output_path.name} "
        f"({len(flattened)} baris)"
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    records = read_jsonl()

    if not records:
        print("Tidak ditemukan data JSONL.")
        return

    # CSV gabungan seluruh hasil pengujian
    write_csv(
        records,
        OUTPUT_DIR / "all_runs.csv"
    )

    # CSV terpisah berdasarkan jenis pengujian
    for test_type in ["benchmark", "changes", "history", "other"]:
        selected = [
            record for record in records
            if record["test_type"] == test_type
        ]

        if selected:
            write_csv(
                selected,
                OUTPUT_DIR / f"{test_type}.csv"
            )

    # Catatan proses ekspor
    summary = {
        "exported_at": datetime.now().isoformat(),
        "source_directory": str(INPUT_DIR),
        "total_records": len(records),
        "output_directory": str(OUTPUT_DIR),
        "types": {
            test_type: sum(
                1 for record in records
                if record["test_type"] == test_type
            )
            for test_type in [
                "benchmark", "changes", "history", "other"
            ]
        }
    }

    with (OUTPUT_DIR / "export_summary.json").open(
        "w", encoding="utf-8"
    ) as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    print("\n=== EKSPOR SELESAI ===")
    print(f"Total record: {len(records)}")
    print(f"Lokasi hasil: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
