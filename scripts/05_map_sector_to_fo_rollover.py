#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel-style sector layout with blank row after each sector

AUTO
ASHOKLEY ...
BAJAJ-AUTO ...

(blank row)

BANKING
HDFCBANK ...
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\Fo_Oi_Chart_November")

FO_FILE = BASE / "data" / "processed" / "merged" / "final_fo_oi_rollover_standard.csv"
SECTOR_FILE = BASE / "data" / "Sector" / "nse_sectorwise_500plus_symbols.csv"

OUT_FILE = BASE / "data" / "processed" / "merged" / "final_fo_sector_excel_layout.csv"

# --------------------------------------------------
# LOAD
# --------------------------------------------------
df = pd.read_csv(FO_FILE)
sector = pd.read_csv(SECTOR_FILE)

# --------------------------------------------------
# NORMALIZE
# --------------------------------------------------
df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
sector["SYMBOL"] = sector["SYMBOL"].astype(str).str.strip().str.upper()
sector = sector.drop_duplicates("SYMBOL")

# --------------------------------------------------
# MERGE SECTOR
# --------------------------------------------------
df = df.merge(sector, on="SYMBOL", how="left")
df["SECTOR"] = df["SECTOR"].fillna("UNMAPPED")

# --------------------------------------------------
# STRICT COLUMN ORDER
# --------------------------------------------------
cols = [
    "SYMBOL",
    "SPOT_CLOSE",
    "FUT_CURR_PRICE",
    "FUT_NEXT_PRICE",
    "ROLL_COST_PCT_M",
    "ROLL_OI_PCT_M",
    "ROLL_OI_PCT_6M",
    "ROLL_COST_PCT_6M",
]

df = df[cols + ["SECTOR"]]

# --------------------------------------------------
# BUILD OUTPUT WITH BLANK ROWS
# --------------------------------------------------
rows = []

for sector_name in sorted(df["SECTOR"].unique()):
    # sector header row
    header = {c: "" for c in cols}
    header["SYMBOL"] = sector_name
    rows.append(header)

    block = df[df["SECTOR"] == sector_name].sort_values("SYMBOL")

    for _, r in block.iterrows():
        rows.append({c: r[c] for c in cols})

    # 👉 BLANK ROW AFTER EACH SECTOR
    rows.append({c: "" for c in cols})

# --------------------------------------------------
# SAVE
# --------------------------------------------------
out = pd.DataFrame(rows, columns=cols)
out.to_csv(OUT_FILE, index=False)

print("✔ DONE")
print("Output:", OUT_FILE)
print("Total rows:", len(out))
print("Stocks:", df["SYMBOL"].nunique())
