from pathlib import Path
import pandas as pd

# =================================================
# PATHS
# =================================================
BASE = Path(__file__).resolve().parents[1]

FO_M = BASE / "data/processed/latest_month_avg/monthly_rollover_standard.csv"
FO_6M = BASE / "data/processed/last_sixmonth_avg/six_month_rollover_standard.csv"
EQ = BASE / "data/processed/latest_month_cm/eq_spot_standard.csv"

OUT_DIR = BASE / "data/processed/merged"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "final_fo_oi_rollover_standard.csv"

# =================================================
# LOAD FILES
# =================================================
fo_m = pd.read_csv(FO_M)
fo_6m = pd.read_csv(FO_6M)
eq = pd.read_csv(EQ)

# =================================================
# NORMALIZE SYMBOL
# =================================================
for df in (fo_m, fo_6m, eq):
    df["SYMBOL"] = (
        df["SYMBOL"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

# =================================================
# MERGE
# =================================================
final = (
    fo_m
    .merge(fo_6m, on="SYMBOL", how="left")
    .merge(eq[["SYMBOL", "SPOT_CLOSE"]], on="SYMBOL", how="left")
)

# =================================================
# FINAL COLUMN ORDER (NO FAR PRICE)
# =================================================
final = final[[
    "SYMBOL",
    "SPOT_CLOSE",
    "FUT_CURR_PRICE",
    "FUT_NEXT_PRICE",
    "ROLL_COST_PCT_M",
    "ROLL_OI_PCT_M",
    "ROLL_OI_PCT_6M",
    "ROLL_COST_PCT_6M",
]]

# =================================================
# SAVE
# =================================================
final.to_csv(OUT_FILE, index=False)

print("✅ FINAL MERGE COMPLETED (FUT_FAR_PRICE REMOVED)")
print("📁 Output saved to:", OUT_FILE)
print(final.head())
