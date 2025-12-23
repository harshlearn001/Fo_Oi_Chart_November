#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FINAL FO + SECTOR EXCEL LAYOUT

Output format:
SECTOR | SYMBOL | prices...
(blank row after each sector)

Excel-friendly | Scheduler-safe | No emojis
"""

from pathlib import Path
import pandas as pd

# =================================================
# PATHS
# =================================================
BASE = Path(r"H:\Fo_Oi_Chart_November")

FO_FILE = BASE / "data" / "processed" / "merged" / "final_fo_oi_rollover_standard.csv"
SECTOR_FILE = BASE / "data" / "Sector" / "nse_sectorwise_500plus_symbols.csv"

OUT_FILE = BASE / "data" / "processed" / "merged" / "final_fo_sector_excel_layout.csv"
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# =================================================
# LOAD FILES
# =================================================
df = pd.read_csv(FO_FILE)
sector = pd.read_csv(SECTOR_FILE)

# =================================================
# NORMALIZE
# =================================================
df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()

sector["SYMBOL"] = sector["SYMBOL"].astype(str).str.strip().str.upper()
sector["SECTOR"] = sector["SECTOR"].astype(str).str.strip().str.upper()

sector = sector.drop_duplicates(subset=["SYMBOL"])

# =================================================
# MERGE SECTOR
# =================================================
df = df.merge(sector, on="SYMBOL", how="left")
df["SECTOR"] = df["SECTOR"].fillna("UNMAPPED")

# =================================================
# FINAL COLUMN ORDER
# =================================================
cols = [
    "SECTOR",
    "SYMBOL",
    "SPOT_CLOSE",
    "PREV_SPOT_CLOSE",
    "FUT_NEXT_PRICE",
    "ROLL_COST_PCT_M",
    "ROLL_OI_PCT_M",
    "ROLL_OI_PCT_6M",
    "ROLL_COST_PCT_6M",
]

df = df[cols]

# =================================================
# BUILD EXCEL-STYLE OUTPUT (WITH BLANK ROWS)
# =================================================
rows = []

for sector_name in sorted(df["SECTOR"].unique()):
    block = df[df["SECTOR"] == sector_name].sort_values("SYMBOL")

    for _, r in block.iterrows():
        rows.append(r.to_dict())

    # blank row after each sector
    rows.append({c: "" for c in cols})

# =================================================
# SAVE
# =================================================
out = pd.DataFrame(rows, columns=cols)
out.to_csv(OUT_FILE, index=False)

print("✔ FINAL FO SECTOR EXCEL LAYOUT GENERATED")
print("Output :", OUT_FILE)
print("Sectors:", df["SECTOR"].nunique())
print("Stocks :", df["SYMBOL"].nunique())
print("Rows   :", len(out))
