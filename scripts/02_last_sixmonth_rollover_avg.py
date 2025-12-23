from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parents[1]
RAW_DIR = BASE / "data/raw/fo_last_sixmonth"
OUT_DIR = BASE / "data/processed/last_sixmonth_avg"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "six_month_rollover_standard.csv"

files = sorted(f for f in RAW_DIR.glob("fo*.csv") if f.stem[2:].isdigit())
print(f"📂 Found {len(files)} FO files (6M)")

rows = []

for f in files:
    trade_dt = pd.to_datetime(f.stem.replace("fo", ""), format="%d%m%Y")
    ym = trade_dt.to_period("M").to_timestamp()

    df = pd.read_csv(f)
    df.columns = df.columns.str.strip().str.upper().str.replace("*", "", regex=False)
    df = df.rename(columns={"EXPIRY_DT": "EXP_DATE", "EXPIRY_DATE": "EXP_DATE"})

    req = {"SYMBOL", "EXP_DATE", "OPEN_INT", "CLOSE_PRICE"}
    if not req.issubset(df.columns):
        continue

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], dayfirst=True, errors="coerce")
    df["OPEN_INT"] = pd.to_numeric(df["OPEN_INT"], errors="coerce")
    df["CLOSE_PRICE"] = pd.to_numeric(df["CLOSE_PRICE"], errors="coerce")

    for sym, g in df.groupby("SYMBOL"):
        exp = (
            g.groupby("EXP_DATE", as_index=False)
             .agg(OI=("OPEN_INT", "sum"), PRICE=("CLOSE_PRICE", "mean"))
             .sort_values("EXP_DATE")
        )

        if len(exp) < 3:
            continue

        OI1, OI2, OI3 = exp["OI"].iloc[:3]
        CP1, CP2 = exp["PRICE"].iloc[:2]

        roll_oi = np.nan
        roll_cost = np.nan

        if pd.notna(OI1) and pd.notna(OI2) and pd.notna(OI3):
            tot = OI1 + OI2 + OI3
            if tot != 0:
                roll_oi = ((OI2 + OI3) / tot) * 100

        if pd.notna(CP1) and pd.notna(CP2) and CP1 != 0:
            roll_cost = ((CP2 - CP1) / CP1) * 100

        rows.append({
            "YEAR_MONTH": ym,
            "SYMBOL": sym,
            "ROLL_OI": roll_oi,
            "ROLL_COST": roll_cost
        })

dfd = pd.DataFrame(rows)
latest = dfd["YEAR_MONTH"].max()
cutoff = latest - pd.DateOffset(months=6)

sixm = dfd[dfd["YEAR_MONTH"] >= cutoff]

six_avg = (
    sixm.groupby("SYMBOL", as_index=False)
        .agg(
            ROLL_OI_PCT_6M=("ROLL_OI", "mean"),
            ROLL_COST_PCT_6M=("ROLL_COST", "mean")
        )
        .round(4)
)

six_avg.to_csv(OUT_FILE, index=False)
print("✅ 6-MONTH FUTURES ROLLOVER GENERATED")
print(six_avg.head())
