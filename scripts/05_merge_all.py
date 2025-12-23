from pathlib import Path
import pandas as pd

# =================================================
# PATHS
# =================================================
BASE = Path(__file__).resolve().parents[1]

FO_M = BASE / "data/processed/latest_month_avg/monthly_rollover_standard.csv"
FO_6M = BASE / "data/processed/last_sixmonth_avg/six_month_rollover_standard.csv"

EQ_CURR = BASE / "data/processed/latest_month_cm/eq_spot_standard.csv"
EQ_PREV = BASE / "data/processed/previous_month_cm/eq_spot_previous_standard.csv"

OUT_DIR = BASE / "data/processed/merged"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "final_fo_oi_rollover_standard.csv"

# =================================================
# LOAD
# =================================================
fo_m = pd.read_csv(FO_M)
fo_6m = pd.read_csv(FO_6M)
eq_curr = pd.read_csv(EQ_CURR)
eq_prev = pd.read_csv(EQ_PREV)

# =================================================
# NORMALIZE SYMBOL
# =================================================
for df in (fo_m, fo_6m, eq_curr, eq_prev):
    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()

# =================================================
# DETECT SPOT CLOSE COLUMNS
# =================================================
def detect_column(df, candidates, label):
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"{label} not found. Available columns: {list(df.columns)}")

# -------------------------------------------------
# CURRENT & PREVIOUS SPOT
# -------------------------------------------------
curr_spot_col = detect_column(
    eq_curr,
    ["SPOT_CLOSE", "CLOSE", "CLOSE_PRICE", "LAST"],
    "Current spot close"
)

prev_spot_col = detect_column(
    eq_prev,
    ["PREV_SPOT_CLOSE", "PRE_SPOT_CLOSE", "pre_SPOT_CLOSE", "CLOSE"],
    "Previous spot close"
)

eq_curr = eq_curr[["SYMBOL", curr_spot_col]].rename(
    columns={curr_spot_col: "SPOT_CLOSE"}
)

eq_prev = eq_prev[["SYMBOL", prev_spot_col]].rename(
    columns={prev_spot_col: "PREV_SPOT_CLOSE"}
)

# =================================================
# MONTHLY FUTURES (ONLY WHAT EXISTS)
# =================================================
# monthly file has: FUT_NEXT_PRICE, ROLL_COST_PCT_M, ROLL_OI_PCT_M

required_monthly = {
    "SYMBOL",
    "FUT_NEXT_PRICE",
    "ROLL_COST_PCT_M",
    "ROLL_OI_PCT_M"
}

missing = required_monthly - set(fo_m.columns)
if missing:
    raise KeyError(f"Missing in monthly rollover file: {missing}")

# =================================================
# MERGE ALL
# =================================================
final = (
    fo_m
    .merge(fo_6m, on="SYMBOL", how="left")
    .merge(eq_curr, on="SYMBOL", how="left")
    .merge(eq_prev, on="SYMBOL", how="left")
)

# =================================================
# FINAL COLUMN ORDER (SAFE)
# =================================================
final = final[[
    "SYMBOL",
    "SPOT_CLOSE",
    "PREV_SPOT_CLOSE",
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

print("FINAL MERGE COMPLETED")
print("Output saved to:", OUT_FILE)
print("Rows   :", len(final))
print("Symbols:", final["SYMBOL"].nunique())
print(final.head())
