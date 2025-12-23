from pathlib import Path
import pandas as pd
import numpy as np

# =================================================
# PATHS
# =================================================
BASE = Path(__file__).resolve().parents[1]
RAW_DIR = BASE / "data" / "raw" / "fo_latest_month"
OUT_DIR = BASE / "data" / "processed" / "latest_month_avg"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "monthly_rollover_standard.csv"

# =================================================
# DISCOVER FILES
# =================================================
files = sorted(
    f for f in RAW_DIR.glob("fo*.csv")
    if f.stem[2:].isdigit()
)

print(f"📂 Found {len(files)} FO files (latest month)")

rows = []

# =================================================
# PROCESS FILES
# =================================================
for csv_file in files:
    print(f"▶ Processing {csv_file.name}")

    df = pd.read_csv(csv_file)

    # Normalize columns
    df.columns = (
        df.columns
          .str.strip()
          .str.upper()
          .str.replace("*", "", regex=False)
    )

    df = df.rename(columns={
        "EXPIRY_DT": "EXP_DATE",
        "EXPIRY_DATE": "EXP_DATE"
    })

    required = {"SYMBOL", "EXP_DATE", "OPEN_INT", "CLOSE_PRICE"}
    if not required.issubset(df.columns):
        continue

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], dayfirst=True, errors="coerce")
    df["OPEN_INT"] = pd.to_numeric(df["OPEN_INT"], errors="coerce")
    df["CLOSE_PRICE"] = pd.to_numeric(df["CLOSE_PRICE"], errors="coerce")

    # =================================================
    # PER SYMBOL → EXPIRY AGGREGATION
    # =================================================
    for sym, g in df.groupby("SYMBOL"):

        exp = (
            g.groupby("EXP_DATE", as_index=False)
             .agg(
                 OI=("OPEN_INT", "sum"),
                 PRICE=("CLOSE_PRICE", "mean")
             )
             .sort_values("EXP_DATE")
        )

        if len(exp) < 3:
            continue

        # Prices
        CP1 = exp["PRICE"].iloc[0]   # current / near
        CP2 = exp["PRICE"].iloc[1]   # next / mid

        # OI
        OI1, OI2, OI3 = exp["OI"].iloc[:3]

        roll_oi = np.nan
        roll_cost = np.nan

        # -----------------------------
        # ROLLOVER OI %
        # -----------------------------
        if pd.notna(OI1) and pd.notna(OI2) and pd.notna(OI3):
            total_oi = OI1 + OI2 + OI3
            if total_oi != 0:
                roll_oi = ((OI2 + OI3) / total_oi) * 100

        # -----------------------------
        # ROLL COST %
        # -----------------------------
        if pd.notna(CP1) and pd.notna(CP2) and CP1 != 0:
            roll_cost = ((CP2 - CP1) / CP1) * 100

        rows.append({
            "SYMBOL": sym,
            "FUT_CURR_PRICE": CP1,
            "FUT_NEXT_PRICE": CP2,
            "ROLL_COST_PCT_M": roll_cost,
            "ROLL_OI_PCT_M": roll_oi
        })

# =================================================
# MONTHLY AVERAGE (NaN SKIPPED)
# =================================================
dfm = pd.DataFrame(rows)

monthly = (
    dfm
    .groupby("SYMBOL", as_index=False)
    .mean(numeric_only=True)
    .round(4)
)

monthly.to_csv(OUT_FILE, index=False)

print("✅ MONTHLY FUTURES ROLLOVER GENERATED (FAR PRICE REMOVED)")
print("📁 Output:", OUT_FILE)
print(monthly.head())
