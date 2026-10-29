import sys
import subprocess
from pathlib import Path
import shutil
import kagglehub

# ----------------------------------------------------------------------
# Dataset identifier
# ----------------------------------------------------------------------
DATASET = "naiyakhalid/flood-prediction-dataset"

print(f"Mengunduh dataset: {DATASET}")

# ----------------------------------------------------------------------
# Download with kagglehub, fall back to kaggle CLI if needed
# ----------------------------------------------------------------------
try:
    download_dir = Path(kagglehub.dataset_download(DATASET))
except Exception as e:
    print(f"kagglehub download failed: {e}")
    print("Mencoba mengunduh menggunakan kaggle CLI...")
    fallback_path = Path("data/raw")
    fallback_path.mkdir(parents=True, exist_ok=True)
    cmd = [
        "kaggle",
        "datasets",
        "download",
        "-d",
        DATASET,
        "-p",
        str(fallback_path),
        "--unzip",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print("Fallback download gagal:")
        print(result.stderr)
        sys.exit(1)
    download_dir = fallback_path

# ----------------------------------------------------------------------
# Prepare destination for CSV files
# ----------------------------------------------------------------------
destination = Path("data/raw")
destination.mkdir(parents=True, exist_ok=True)

print(f"\nLokasi unduhan: {download_dir}")
print("\nBerkas yang ditemukan:")

for file in sorted(download_dir.rglob("*")):
    if file.is_file():
        print(f"  {file.name} ({file.stat().st_size / (1024**2):.2f} MB)")
        if file.suffix.lower() == ".csv":
            target = destination / file.name
            if not target.exists():
                shutil.copy2(file, target)
                print(f"    -> Disalin ke {target}")

print("\nUnduhan dan penyalinan selesai.")

# ----------------------------------------------------------------------
# OPTIONAL: Load a CSV via kagglehub's Pandas adapter
#
