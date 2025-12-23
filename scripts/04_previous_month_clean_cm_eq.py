from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
RAW_DIR = BASE / "data/raw/cm_previous_month"
OUT_DIR = BASE / "data/processed/previous_month_cm"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "eq_spot_previous_standard.csv"

files = list(RAW_DIR.glob("BhavCopy_NSE_CM_*.csv"))
if not files:
    raise RuntimeError("No CM bhavcopy found")

df = pd.read_csv(files[0])

df = df[(df["Sgmt"] == "CM") & (df["SctySrs"] == "EQ")].copy()
df = df[["TradDt", "TckrSymb", "ClsPric"]]

df = df.rename(columns={
    "TckrSymb": "SYMBOL",
    "ClsPric": "pre_SPOT_CLOSE"
})

df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
df["TradDt"] = pd.to_datetime(df["TradDt"], dayfirst=True, errors="coerce")
df["pre_SPOT_CLOSE"] = pd.to_numeric(df["pre_SPOT_CLOSE"], errors="coerce")

df.to_csv(OUT_FILE, index=False)
print("✅ EQ SPOT CLEANED")
print(df.head())
