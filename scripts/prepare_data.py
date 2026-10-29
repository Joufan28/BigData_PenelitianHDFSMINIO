#!/usr/bin/env python3
"""
Normalisasi CSV sumber pemerintah ke skema benchmark.
Script tidak menggandakan baris untuk mencapai target 100.000.
Unduh/ekspor dataset resmi terlebih dahulu ke data/raw/government.csv.
"""
import csv, os, re, sys
from datetime import datetime

INPUT = sys.argv[1] if len(sys.argv) > 1 else "data/raw/government.csv"
OUTPUT = sys.argv[2] if len(sys.argv) > 2 else "data/processed/banjir.csv"

ALIASES = {
    "record_id": ["record_id", "id", "kode", "no", "nomor"],
    "observed_at": ["observed_at", "tanggal", "tanggal_kejadian", "date", "tahun", "waktu"],
    "province": ["province", "provinsi"],
    "district": ["district", "kabupaten", "kabupaten_kota", "kota", "wilayah"],
    "event_type": ["event_type", "jenis_bencana", "jenis_bencana_alam", "bencana", "jenis"],
    "affected_people": ["affected_people", "jumlah_terdampak", "korban_terdampak", "terdampak", "jumlah_jiwa"],
    "houses_damaged": ["houses_damaged", "rumah_rusak", "jumlah_rumah_rusak", "kerusakan_rumah"],
    "rainfall_mm": ["rainfall_mm", "curah_hujan", "curah_hujan_mm"],
    "water_level_cm": ["water_level_cm", "tinggi_muka_air", "tinggi_muka_air_cm", "tma"],
    "risk_level": ["risk_level", "tingkat_risiko", "risiko"],
}
def norm(s):
    return re.sub(r"[^a-z0-9]+", "_", (s or "").strip().lower()).strip("_")
def clean_num(x):
    if x is None: return ""
    x = str(x).strip().replace(".", "").replace(",", ".") if "," in str(x) else str(x).strip()
    return x if x and x.lower() not in {"nan", "null", "none", "-"} else ""
def clean_date(x):
    x = (x or "").strip()
    if not x: return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y", "%d/%m/%y"):
        try:
            d = datetime.strptime(x[:10] if fmt == "%Y-%m-%d" else x, fmt)
            return d.strftime("%Y-%m-%d 00:00:00")
        except ValueError:
            pass
    # Leave ISO datetime recognizable; Spark will parse it.
    return x

if not os.path.exists(INPUT):
    sys.exit(f"File sumber tidak ditemukan: {INPUT}\nUnduh data resmi, ekspor sebagai CSV, lalu simpan ke lokasi tersebut.")
os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)
with open(INPUT, newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    if not reader.fieldnames:
        sys.exit("CSV tidak memiliki header.")
    original = {norm(c): c for c in reader.fieldnames}
    mapping = {}
    for target, names in ALIASES.items():
        mapping[target] = next((original[norm(n)] for n in names if norm(n) in original), None)
    if not mapping["observed_at"] or not mapping["event_type"]:
        sys.exit("Kolom tanggal/tahun dan jenis bencana tidak terdeteksi. Edit ALIASES pada script agar cocok dengan header CSV sumber.")
    rows, seen = [], set()
    for idx, row in enumerate(reader, start=1):
        event = (row.get(mapping["event_type"], "") or "").strip()
        if not event:
            continue
        # Untuk penelitian risiko banjir, pilih kejadian yang berlabel banjir.
        if "banjir" not in event.lower():
            continue
        rid = (row.get(mapping["record_id"], "") or "").strip() if mapping["record_id"] else ""
        if not rid:
            rid = f"BNPB-{idx:09d}"
        if rid in seen:
            rid = f"{rid}-{idx}"
        seen.add(rid)
        date = clean_date(row.get(mapping["observed_at"], "") if mapping["observed_at"] else "")
        province = (row.get(mapping["province"], "") or "").strip() if mapping["province"] else ""
        district = (row.get(mapping["district"], "") or "").strip() if mapping["district"] else ""
        risk = (row.get(mapping["risk_level"], "") or "").strip().lower() if mapping["risk_level"] else ""
        if risk not in {"rendah", "sedang", "tinggi", "sangat tinggi"}:
            risk = "tidak_diketahui"
        rows.append({
            "record_id": rid, "observed_at": date, "province": province,
            "district": district, "event_type": event,
            "affected_people": clean_num(row.get(mapping["affected_people"], "")) if mapping["affected_people"] else "",
            "houses_damaged": clean_num(row.get(mapping["houses_damaged"], "")) if mapping["houses_damaged"] else "",
            "rainfall_mm": clean_num(row.get(mapping["rainfall_mm"], "")) if mapping["rainfall_mm"] else "",
            "water_level_cm": clean_num(row.get(mapping["water_level_cm"], "")) if mapping["water_level_cm"] else "",
            "risk_level": risk,
        })
headers = ["record_id","observed_at","province","district","event_type","affected_people","houses_damaged","rainfall_mm","water_level_cm","risk_level"]
with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=headers)
    w.writeheader()
    w.writerows(rows)
print(f"Output: {OUTPUT}")
print(f"Jumlah baris banjir: {len(rows):,}")
if len(rows) < 100_000:
    print("PERINGATAN: sumber yang dipilih menghasilkan kurang dari 100.000 baris.")
    print("Jangan menggandakan baris. Cari sumber granular tambahan atau ubah target benchmark dan laporkan ukuran aktual.")
else:
    print("Target awal 100.000 baris terpenuhi.")
print("Catatan: kolom yang tidak tersedia pada sumber asli dibiarkan kosong, bukan diisi dengan nilai buatan.")
