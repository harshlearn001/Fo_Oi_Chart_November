#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
STEP 06 | FINAL FO → SECTOR EXCEL LAYOUT (INBUILT NSE SECTORS)

✔ No external sector CSV
✔ NSE symbols preserved exactly
✔ Deterministic & auditable
✔ INDEX symbols handled separately
✔ Blank row after each sector
✔ Production safe
"""

from pathlib import Path
import pandas as pd

# =================================================
# PATHS
# =================================================
BASE = Path(__file__).resolve().parents[1]

FO_FILE = BASE / "data/processed/merged/final_fo_oi_rollover_standard.csv"
OUT_FILE = BASE / "data/processed/merged/final_fo_sector_excel_layout.csv"
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# =================================================
# LOAD
# =================================================
if not FO_FILE.exists():
    raise FileNotFoundError(f"FO file missing: {FO_FILE}")

fo = pd.read_csv(FO_FILE)

# =================================================
# SYMBOL NORMALIZATION
# =================================================
RENAME_MAP = {
    "PHOENIXL": "PHOENIXLTD",
    "UNO MINDA": "UNOMINDA",
    "LTI": "LTIM",
    "MCDOWELL": "MCDOWELL-N",
}

fo["SYMBOL"] = (
    fo["SYMBOL"]
      .astype(str)
      .str.strip()
      .str.upper()
      .replace(RENAME_MAP)
)

# =================================================
# 🔑 INBUILT NSE SECTOR MASTER (TRADING-GRADE)
# =================================================
SECTOR_MAP = {

    # ---------------- BANKING ----------------
    "BANKING": {
        "HDFCBANK","ICICIBANK","AXISBANK","SBIN","KOTAKBANK","BANKBARODA",
        "PNB","IDFCFIRSTB","FEDERALBNK","YESBANK","RBLBANK","INDUSINDBK",
        "AUBANK","BANDHANBNK","CANBK","UNIONBANK","INDIANB","BANKINDIA",
        "UCOBANK","CENTRALBK","MAHABANK","CSBBANK","DCBBANK","SOUTHBANK",
        "IDBI","KARURVYSYA","TMB"
    },

    # ---------------- FINANCE / NBFC ----------------
    "FINANCE": {
        "ABCAPITAL","ANGELONE","JIOFIN","LICI","PNBHOUSING",
        "BAJFINANCE","BAJAJFINSV","CHOLAFIN","SHRIRAMFIN","LTF",
        "LICHSGFIN","MUTHOOTFIN","MANAPPURAM","PFC","RECLTD",
        "IRFC","IREDA","IIFL","KFINTECH","MFSL","SAMMAANCAP",
        "360ONE","ICICIGI","ICICIPRULI","SBICARD","SBILIFE",
        "HDFCAMC","NUVAMA","PAYTM","POLICYBZR","HDFCLIFE",
    "MAXHEALTH"
    },

    # ---------------- IT ----------------
    "IT": {
        "TCS","INFY","HCLTECH","WIPRO","LTIM","TECHM","COFORGE","MPHASIS",
        "KPITTECH","PERSISTENT","OFSS","CYIENT","SONATSOFTW","ZENSARTECH",
        "BIRLASOFT","DATAMATICS","INTELLECT","NEWGEN","RATEGAIN","QUESS",
        "TATAELXSI","TATATECH"
    },

    # ---------------- ENERGY ----------------
    "ENERGY": {
        "RELIANCE","ONGC","BPCL","IOC","GAIL","NTPC","POWERGRID","NHPC",
        "OIL","ADANIPOWER","ADANIGREEN","ADANIENSOL",
        "TATAPOWER","TORNTPOWER","JSWENERGY","SJVN","NLCINDIA",
        "PETRONET","CESC","RPOWER","INOXWIND","SUZLON","HINDPETRO",
        "POWERINDIA","ADANIENT",
    "IEX"
    },

    # ---------------- AUTO ----------------
    "AUTO": {
        "MARUTI","M&M","TATAMOTORS","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT",
        "TVSMOTOR","ASHOKLEY","UNOMINDA","SONACOMS","MOTHERSON",
        "ENDURANCE","EXIDEIND","BOSCHLTD","BALKRISIND","CEAT","JKTYRE","MRF","TMPV"
    },

    # ---------------- FMCG ----------------
    "FMCG": {
        "ITC","HINDUNILVR","NESTLEIND","BRITANNIA","DABUR","GODREJCP",
        "MARICO","VBL","TATACONSUM","COLPAL","EMAMILTD","PATANJALI",
        "RADICO","UBL","BIKAJI","AWL","VARUN","KRBL","LTFOODS",
        "ASIANPAINT","ASTRAL","UNITDSPR","JUBLFOOD","KALYANKJIL"
    },

    # ---------------- PHARMA ----------------
    "PHARMA": {
        "SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","ALKEM","AUROPHARMA",
        "LUPIN","TORNTPHARM","ZYDUSLIFE","BIOCON","GLENMARK","LAURUSLABS",
        "NATCOPHARM","IPCALAB","AJANTPHARM","JBPHARMA","PFIZER","SYNGENE",
        "APOLLOHOSP","FORTIS","MANKIND","PPLPHARMA"
    },

    # ---------------- METAL ----------------
    "METAL": {
        "TATASTEEL","JSWSTEEL","HINDALCO","NMDC","COALINDIA","VEDL","SAIL",
        "NATIONALUM","HINDZINC","APLAPOLLO","RATNAMANI","JINDALSTEL",
        "MOIL","IMFA","GPIL","ISGEC","RAMCOIND",
        "AMBUJACEM","DALBHARAT","GRASIM","SHREECEM","ULTRACEMCO"
    },

    # ---------------- CHEMICALS ----------------
    "CHEMICALS": {
        "PIDILITIND","SRF","SOLARINDS","UPL","PIIND"
    },

    # ---------------- INDUSTRIALS ----------------
    "INDUSTRIALS": {
        "BHARATFORG","CGPOWER","KEI","KAYNES","SUPREMEIND",
        "TIINDIA","PGEL"
    },

    # ---------------- DEFENCE ----------------
    "DEFENCE": {
        "HAL","BDL","MAZDOCK","BEL"
    },

    # ---------------- INFRA / LOGISTICS ----------------
    "INFRA": {
        "LT","ADANIPORTS","SIEMENS","BHEL","ABB","CUMMINSIND","THERMAX",
        "KEC","KNRCON","IRCON","RVNL","NBCC","RITES","HUDCO","GRINFRA",
        "ENGINERSIN","GMRINFRA","HGINFRA","ASHOKA","PNCINFRA","CONCOR","GMRAIRPORT"
    },

    # ---------------- CONSUMER ----------------
    "CONSUMER": {
        "TITAN","VOLTAS","HAVELLS","WHIRLPOOL","BLUESTARCO",
        "CROMPTON","DIXON","AMBER","KAJARIACER","CERA","VGUARD","VIPIND","BATAINDIA",
        "INDHOTEL","INDIGO",  "POLYCAB"
    },

    # ---------------- INDEX ----------------
    "INDEX": {
        "NIFTY","BANKNIFTY","NIFTYNXT50","MIDCPNIFTY"
    },

# ---------------- TELECOM ----------------
    "TELECOM": {
        "BHARTIARTL","IDEA","VI","TATACOMM","INDUSTOWER"
    },
# ---------------- REALTY ----------------
    "REALTY": {
        "DLF","LALPATHLAB","OBEROIRLTY","PRESTIGE",
        "MAHINDCIE","SUNTECK","CIPLAHOUSING","LODHA","PHOENIXLTD",
    "GODREJPROP","BRIGADE","SOBHA","ANANTRAJ","KOLTEPATIL","MAHLIFE","RUSTOMJEE"
    },
# ---------------- MARKET INFRA ----------------
    "MARKET INFRA": {
        "NSE","BSE","MCX","CDSL","CAMS"
    },
# ---------------- TRANSPORT / LOGISTICS ----------------
    "TRANSPORT": {
        "IRCTC","DELHIVERY","ALLCARGO","GATI","TCIEXP","VRLLOG"
    },

# ---------------- INTERNET / PLATFORM ----------------
    "INTERNET": {
        "NYKAA","NAUKRI","TRENT","DMART","PAGEIND","ETERNAL","ZOMATO"
    },
      
}

# =================================================
# MAP SECTOR
# =================================================
def map_sector(symbol: str) -> str:
    for sector, symbols in SECTOR_MAP.items():
        if symbol in symbols:
            return sector
    return "UNMAPPED"

fo["SECTOR"] = fo["SYMBOL"].apply(map_sector)

# =================================================
# FINAL OUTPUT
# =================================================
cols = [
    "SECTOR","SYMBOL",
    "SPOT_CLOSE","PREV_SPOT_CLOSE","FUT_NEXT_PRICE",
    "ROLL_COST_PCT_M","ROLL_OI_PCT_M",
    "ROLL_OI_PCT_6M","ROLL_COST_PCT_6M"
]
fo = fo[cols]

rows = []
last_sector = None

for _, r in fo.sort_values(["SECTOR","SYMBOL"]).iterrows():
    if last_sector and r["SECTOR"] != last_sector:
        rows.append({c: "" for c in cols})
    rows.append(r.to_dict())
    last_sector = r["SECTOR"]

pd.DataFrame(rows, columns=cols).to_csv(OUT_FILE, index=False)

# =================================================
# SUMMARY
# =================================================
print("✔ FINAL SECTOR LAYOUT GENERATED (INBUILT)")
print("Output   :", OUT_FILE)
print("Sectors  :", fo["SECTOR"].nunique())
print("Stocks   :", fo["SYMBOL"].nunique())
print("Unmapped :", (fo["SECTOR"] == "UNMAPPED").sum())
