
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

BASE = Path(__file__).resolve().parent.parent
INPUT = BASE / "results" / "report" / "benchmark_comparison.csv"
OUTPUT = BASE / "results" / "report" / "figures"
OUTPUT.mkdir(parents=True, exist_ok=True)

with INPUT.open(encoding="utf-8-sig", newline="") as f:
    rows = list(csv.DictReader(f))


def make_chart(category, filename, title, xlabel):
    data = [r for r in rows if r["category"] == category]

    if not data:
        raise ValueError(f"Tidak ada data kategori: {category}")

    labels = [r["operation"] for r in data]
    iceberg = [float(r["iceberg_seconds"]) for r in data]
    parquet = [float(r["parquet_seconds"]) for r in data]

    y = np.arange(len(labels))
    height = 0.36

    fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.9)))
    ax.barh(y + height / 2, iceberg, height, label="Iceberg")
    ax.barh(y - height / 2, parquet, height, label="Parquet")

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel(xlabel)
    ax.set_title(title)
    ax.legend()
    ax.grid(axis="x", linestyle="--", alpha=0.35)

    for i, value in enumerate(iceberg):
        ax.text(value, i + height / 2, f" {value:.3f}s", va="center")

    for i, value in enumerate(parquet):
        ax.text(value, i - height / 2, f" {value:.3f}s", va="center")

    fig.tight_layout()
    fig.savefig(OUTPUT / filename, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Grafik dibuat: {OUTPUT / filename}")


make_chart(
    "analytics",
    "analytics_comparison.png",
    "Analytical Query Performance",
    "Waktu eksekusi (detik)",
)

make_chart(
    "change",
    "data_changes_comparison.png",
    "Data Change Operation Performance",
    "Waktu eksekusi (detik)",
)
