
#!/usr/bin/env python3
import csv
import math
import os

PATH = "data/raw/train.csv"

EXPECTED_COLUMNS = [
    "id",
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
    "FloodProbability",
]

def main():
    if not os.path.isfile(PATH):
        raise FileNotFoundError(f"Dataset tidak ditemukan: {PATH}")

    count = 0
    unique_ids = set()
    duplicate_ids = 0
    empty_cells = 0
    invalid_numbers = 0
    min_probability = math.inf
    max_probability = -math.inf
    sum_probability = 0.0

    with open(PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(
                f"Header tidak sesuai.\n"
                f"Aktual: {reader.fieldnames}"
            )

        for line_number, row in enumerate(reader, start=2):
            count += 1

            if any(
                value is None or not value.strip()
                for value in row.values()
            ):
                empty_cells += 1
                continue

            record_id = row["id"]
            if record_id in unique_ids:
                duplicate_ids += 1
            else:
                unique_ids.add(record_id)

            try:
                probability = float(row["FloodProbability"])
                if not math.isfinite(probability):
                    raise ValueError("Nilai bukan angka finite")
                sum_probability += probability
                min_probability = min(min_probability, probability)
                max_probability = max(max_probability, probability)

                for column in EXPECTED_COLUMNS[1:-1]:
                    value = float(row[column])
                    if not math.isfinite(value):
                        raise ValueError(f"Nilai tidak valid: {column}")
            except ValueError:
                invalid_numbers += 1

    print("=== HASIL VALIDASI DATASET ===")
    print(f"File: {PATH}")
    print(f"Jumlah baris: {count}")
    print(f"Jumlah kolom: {len(EXPECTED_COLUMNS)}")
    print(f"ID unik: {len(unique_ids)}")
    print(f"ID duplikat: {duplicate_ids}")
    print(f"Baris dengan sel kosong: {empty_cells}")
    print(f"Baris dengan nilai numerik tidak valid: {invalid_numbers}")

    if count and invalid_numbers == 0 and empty_cells == 0:
        print(f"FloodProbability minimum: {min_probability}")
        print(f"FloodProbability maksimum: {max_probability}")
        print(f"FloodProbability rata-rata: {sum_probability / count}")

    if (
        count == 0
        or duplicate_ids > 0
        or empty_cells > 0
        or invalid_numbers > 0
    ):
        raise SystemExit("VALIDASI GAGAL: periksa dataset sebelum benchmark.")

    print("VALIDASI LULUS")

if __name__ == "__main__":
    main()
