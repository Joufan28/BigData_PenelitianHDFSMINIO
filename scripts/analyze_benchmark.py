#!/usr/bin/env python3
"""
Analisis hasil benchmark Lakehouse Banjir.

Membaca seluruh file *.jsonl dalam folder results/runs dan membuat:
  - benchmark_summary.csv : ringkasan median analitik dan waktu operasi perubahan
  - benchmark_raw.csv     : seluruh record JSONL yang berhasil dibaca
  - benchmark_comparison.csv : perbandingan Iceberg vs Parquet untuk metrik yang sepadan

Penggunaan:
    python3 scripts/analyze_benchmark.py
    python3 scripts/analyze_benchmark.py --input results/runs --output results/report
"""

import argparse
import csv
import json
import statistics
from pathlib import Path


def load_records(input_dir: Path):
    records = []
    errors = []

    if not input_dir.exists():
        raise FileNotFoundError(f"Folder hasil tidak ditemukan: {input_dir}")

    files = sorted(input_dir.glob("run_*.jsonl"))
    if not files:
        raise FileNotFoundError(f"Tidak ada file run_*.jsonl di: {input_dir}")

    for file_path in files:
        with file_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    record.setdefault("source_file", file_path.name)
                    records.append(record)
                except json.JSONDecodeError as exc:
                    errors.append(
                        f"{file_path.name}:{line_no}: JSON tidak valid ({exc})"
                    )

    return records, files, errors


def write_csv(path: Path, rows, fieldnames=None):
    path.parent.mkdir(parents=True, exist_ok=True)

    if fieldnames is None:
        keys = set()
        for row in rows:
            keys.update(row.keys())
        fieldnames = sorted(keys)

    if not fieldnames:
        fieldnames = ["message"]

    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def median(values):
    clean = []
    for value in values:
        try:
            if value is not None:
                clean.append(float(value))
        except (TypeError, ValueError):
            pass
    return statistics.median(clean) if clean else None


def summarize_records(records):
    summary = []

    # Analitik: gunakan median dari lima pengukuran individual jika tersedia.
    analytic_groups = {}
    for record in records:
        if record.get("operation") == "analytics":
            key = (
                record.get("run_id"),
                record.get("system"),
                record.get("query"),
            )
            analytic_groups.setdefault(key, []).append(record)

    for (run_id, system, query), group in sorted(
        analytic_groups.items(),
        key=lambda item: tuple(str(x or "") for x in item[0]),
    ):
        seconds = [r.get("seconds") for r in group]
        summary.append({
            "run_id": run_id,
            "mode": "benchmark",
            "category": "analytics",
            "system": system,
            "operation": query,
            "measurement_count": len(seconds),
            "median_seconds": median(seconds),
            "min_seconds": min(
                [float(x) for x in seconds if x is not None],
                default=None,
            ),
            "max_seconds": max(
                [float(x) for x in seconds if x is not None],
                default=None,
            ),
            "rows_before": None,
            "rows_after": None,
            "passed": all(r.get("passed") is True for r in group),
            "source_file": group[0].get("source_file"),
        })

    # Perubahan data: waktu operasi berasal dari masing-masing record.
    for record in records:
        operation = record.get("operation")
        system = record.get("system")
        if operation in ("insert", "update", "delete") and system in (
            "iceberg",
            "parquet",
        ):
            summary.append({
                "run_id": record.get("run_id"),
                "mode": record.get("mode"),
                "category": "change",
                "system": system,
                "operation": operation,
                "measurement_count": 1,
                "median_seconds": record.get("seconds"),
                "min_seconds": record.get("seconds"),
                "max_seconds": record.get("seconds"),
                "rows_before": record.get("rows_before"),
                "rows_after": record.get("rows_after"),
                "passed": record.get("passed"),
                "source_file": record.get("source_file"),
            })

        elif operation == "validation":
            systems = record.get("systems", {})
            for validation_system, metrics in systems.items():
                summary.append({
                    "run_id": record.get("run_id"),
                    "mode": record.get("mode"),
                    "category": "validation",
                    "system": validation_system,
                    "operation": "validation",
                    "measurement_count": None,
                    "median_seconds": None,
                    "min_seconds": None,
                    "max_seconds": None,
                    "rows_before": metrics.get("rows"),
                    "rows_after": metrics.get("rows"),
                    "distinct_ids": metrics.get("distinct_ids"),
                    "probability_sum": metrics.get("probability_sum"),
                    "probability_avg": metrics.get("probability_avg"),
                    "passed": record.get("passed"),
                    "source_file": record.get("source_file"),
                })

        elif operation == "init":
            summary.append({
                "run_id": record.get("run_id"),
                "mode": record.get("mode"),
                "category": "init",
                "system": "both",
                "operation": "init",
                "measurement_count": None,
                "median_seconds": None,
                "min_seconds": None,
                "max_seconds": None,
                "rows_before": record.get("expected_rows"),
                "rows_after": None,
                "passed": record.get("passed"),
                "source_file": record.get("source_file"),
            })

    return summary


def build_comparison(summary):
    """
    Membandingkan median waktu antar-sistem dalam run yang sama.
    Speedup > 1 berarti Parquet lebih cepat daripada Iceberg.
    """
    indexed = {}

    for row in summary:
        if row.get("category") not in ("analytics", "change"):
            continue
        if row.get("system") not in ("iceberg", "parquet"):
            continue
        key = (
            row.get("run_id"),
            row.get("category"),
            row.get("operation"),
            row.get("system"),
        )
        indexed[key] = row

    comparisons = []
    base_keys = {
        (run_id, category, operation)
        for run_id, category, operation, system in indexed
    }

    for run_id, category, operation in sorted(
        base_keys,
        key=lambda x: tuple(str(v or "") for v in x),
    ):
        iceberg = indexed.get((run_id, category, operation, "iceberg"))
        parquet = indexed.get((run_id, category, operation, "parquet"))

        if not iceberg or not parquet:
            continue

        ice_time = iceberg.get("median_seconds")
        par_time = parquet.get("median_seconds")

        if ice_time is None or par_time is None:
            faster = ""
            speedup = None
            percent = None
        else:
            ice_time = float(ice_time)
            par_time = float(par_time)
            faster = "tie" if ice_time == par_time else (
                "parquet" if par_time < ice_time else "iceberg"
            )
            speedup = (
                ice_time / par_time if par_time > 0 else None
            )
            percent = (
                (ice_time - par_time) / ice_time * 100
                if ice_time > 0 else None
            )

        comparisons.append({
            "run_id": run_id,
            "category": category,
            "operation": operation,
            "iceberg_seconds": ice_time,
            "parquet_seconds": par_time,
            "faster_system": faster,
            "iceberg_over_parquet_ratio": speedup,
            "parquet_time_reduction_percent": percent,
            "iceberg_passed": iceberg.get("passed"),
            "parquet_passed": parquet.get("passed"),
        })

    return comparisons


def main():
    parser = argparse.ArgumentParser(
        description="Gabungkan hasil benchmark JSONL menjadi laporan CSV."
    )
    parser.add_argument(
        "--input",
        default="results/runs",
        help="Folder yang berisi run_*.jsonl (default: results/runs)",
    )
    parser.add_argument(
        "--output",
        default="results/report",
        help="Folder keluaran CSV (default: results/report)",
    )
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)

    records, files, errors = load_records(input_dir)
    summary = summarize_records(records)
    comparison = build_comparison(summary)

    write_csv(output_dir / "benchmark_raw.csv", records)
    write_csv(output_dir / "benchmark_summary.csv", summary)
    write_csv(output_dir / "benchmark_comparison.csv", comparison)

    print(f"File run dibaca : {len(files)}")
    for file_path in files:
        print(f"  - {file_path}")
    print(f"Jumlah record   : {len(records)}")
    print(f"Baris ringkasan : {len(summary)}")
    print(f"Perbandingan    : {len(comparison)}")
    print(f"Folder laporan  : {output_dir.resolve()}")

    if errors:
        print("\nPERINGATAN: beberapa baris JSONL tidak terbaca:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("Semua baris JSONL berhasil dibaca.")


if __name__ == "__main__":
    main()
