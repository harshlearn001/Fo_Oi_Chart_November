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
    raise FileNotFoundError(f"❌ FO file missing: {FO_FILE}")

fo = pd.read_csv(FO_FILE)

# =================================================
# SYMBOL NORMALIZATION (NSE SAFE)
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
# 🔑 INBUILT NSE SECTOR MASTER
# =================================================
FULL_SECTORIAL_MAP = {

    # ---------------- BANKS ----------------
    "PRIVATE_BANK": {
        "HDFCBANK","ICICIBANK","AXISBANK","KOTAKBANK","INDUSINDBK",
        "IDFCFIRSTB","FEDERALBNK","RBLBANK","YESBANK","BANDHANBNK","AUBANK"
    },

    "PSU_BANK": {
        "SBIN","BANKBARODA","PNB","CANBK","UNIONBANK","BANKINDIA",
        "INDIANB","IOB","UCOBANK","CENTRALBK","MAHABANK"
    },

    # ---------------- FINANCIAL SERVICES ----------------
    "FINANCIAL_SERVICES": {
        "BAJFINANCE","BAJAJFINSV","SHRIRAMFIN","CHOLAFIN","LICHSGFIN",
        "MUTHOOTFIN","MANAPPURAM","PNBHOUSING","ABCAPITAL","LTF",
        "PFC","RECLTD","IRFC","IREDA","HUDCO","HDFCAMC","360ONE",
        "SBICARD","ICICIGI","ICICIPRULI","SBILIFE","HDFCLIFE",
        "PAYTM","POLICYBZR","ANGELONE","MFSL","KFINTECH","CAMS",
        "IIFL","JIOFIN","LICI","NUVAMA","SAMMAANCAP"
    },

    # ---------------- IT ----------------
    "IT": {
        "TCS","INFY","HCLTECH","WIPRO","TECHM","LTIM","MPHASIS","COFORGE",
        "PERSISTENT","OFSS","KPITTECH","CYIENT","ZENSARTECH",
        "TATAELXSI","TATATECH","SONATSOFTW","BSOFT","LTTS","INTELLECT",
        "NAUKRI"
    },

    # ---------------- TELECOM ----------------
    "TELECOM": {
        "BHARTIARTL","IDEA","VI","TATACOMM","INDUSTOWER","HFCL"
    },

    # ---------------- AUTO / AUTO ANC ----------------
    "AUTO": {
        "MARUTI","TATAMOTORS","M&M","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT",
        "TVSMOTOR","ASHOKLEY","SONACOMS","MOTHERSON","UNOMINDA",
        "ENDURANCE","BOSCHLTD","EXIDEIND","BALKRISIND",
        "MRF","CEAT","JKTYRE","TMPV",
        "BHARATFORG","TIINDIA"
    },

    # ---------------- FMCG ----------------
    "FMCG": {
        "ITC","HINDUNILVR","NESTLEIND","BRITANNIA","TATACONSUM",
        "DABUR","GODREJCP","MARICO","VBL","EMAMILTD",
        "PATANJALI","RADICO","UBL","UNITDSPR","COLPAL",
        "PAGEIND","JUBLFOOD"
    },

    # ---------------- CONSUMER DURABLES ----------------
    "CONSUMER_DURABLES": {
        "TITAN","VOLTAS","HAVELLS","DIXON","WHIRLPOOL","BLUESTARCO",
        "CROMPTON","VGUARD","AMBER","PGEL","BATAINDIA",
        "KAJARIACER","CERA","CENTURYPLY","POLYCAB","ASTRAL"
    },

    # ---------------- HEALTHCARE ----------------
    "HEALTHCARE": {
        "APOLLOHOSP","FORTIS","MEDANTA","KIMS","RAINBOW",
        "MAXHEALTH","LALPATHLAB","METROPOLIS","VIJAYA","INDGN","ASTERDM",
        "NH","ONESOURCE","ONESOURCE"
    },

#-------------------PHARMA--------------------
    "PHARMA": {
    "SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","ZYDUSLIFE",
    "TORNTPHARM","ALKEM","LUPIN","AUROPHARMA","BIOCON",
    "GLENMARK","IPCALAB","MANKIND","LAURUSLABS","PPLPHARMA",
    "ABBOTINDIA","SYNGENE","PFIZER","GLAXO",
    "AJANTPHARM","JBCHEPHARM","NEULANDLAB","NATCOPHARM",
    "GRANULES","WOCKPHARMA","ERIS","CAPLIPOINT","EMCURE",
    "CONCORDBIO","ASTRAZEN"
    },

    # ---------------- CHEMICALS / PAINT ----------------
    "CHEMICALS": {
        "PIDILITIND","SRF","SOLARINDS","UPL","PIIND","AARTIIND",
        "FLUOROCHEM","NAVINFLUOR","TATACHEM","COROMANDEL",
        "DEEPAKNTR","DEEPAKFERT","CHAMBLFERT","BAYERCROP",
        "PCBL","LINDEINDIA","SUMICHEM","SWANCORP",
        "ASIANPAINT"
    },

    # ---------------- METALS / CEMENT ----------------
    "METALS": {
        "TATASTEEL","JSWSTEEL","JINDALSTEL","SAIL","NMDC",
        "HINDALCO","NATIONALUM","APLAPOLLO","HINDZINC",
        "VEDL","WELCORP","LLOYDSME",
        "AMBUJACEM","DALBHARAT","GRASIM","SHREECEM","ULTRACEMCO",
        "COALINDIA"
    },

    # ---------------- ENERGY / POWER ----------------
    "ENERGY": {
        "RELIANCE","ONGC","BPCL","IOC","HINDPETRO","OIL",
        "GAIL","PETRONET","IGL","MGL","GUJGASLTD","GSPL",
        "NTPC","POWERGRID","NHPC","TATAPOWER","ADANIPOWER",
        "ADANIGREEN","JSWENERGY","SJVN","NLCINDIA","IEX",
        "ADANIENSOL","INOXWIND","SUZLON","TORNTPOWER","POWERINDIA"
    },

    # ---------------- INDUSTRIALS ----------------
    "INDUSTRIALS": {
        "ABB","CGPOWER","CUMMINSIND","KAYNES","KEI",
        "SIEMENS","SUPREMEIND"
    },

    # ---------------- DEFENCE ----------------
    "DEFENCE": {
        "HAL","BEL","BDL","MAZDOCK"
    },

    # ---------------- INFRA ----------------
    "INFRA": {
        "LT","ADANIPORTS","BHEL","KEC","KNRCON","IRCON",
        "RVNL","NBCC","RITES","ENGINERSIN","PNCINFRA",
        "ASHOKA","GMRINFRA","GMRAIRPORT","HGINFRA","CONCOR",
        "ADANIENT"
    },

    # ---------------- REALTY ----------------
    "REALTY": {
        "DLF","LODHA","GODREJPROP","OBEROIRLTY","PRESTIGE",
        "PHOENIXLTD","SOBHA","BRIGADE","ANANTRAJ","SIGNATURE"
    },

    # ---------------- MEDIA ----------------
    "MEDIA": {
        "ZEEL","SUNTV","PVRINOX","SAREGAMA","TIPSMUSIC",
        "NETWORK18","HATHWAY","DBCORP","NAZARA","PFOCUS"
    },

    # ---------------- RETAIL / PLATFORM ----------------
    "RETAIL": {
        "DMART","TRENT","KALYANKJIL","NYKAA","ETERNAL","INDHOTEL","INDIGO"
    },

    # ---------------- MARKET INFRA ----------------
    "MARKET_INFRA": {
        "NSE","BSE","MCX","CDSL","CAMS"
    },

    # ---------------- LOGISTICS ----------------
    "LOGISTICS": {
        "IRCTC","DELHIVERY","ALLCARGO","GATI","TCIEXP","VRLLOG"
    },

    # ---------------- INDEX ----------------
    "INDEX": {
        "NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY","NIFTYNXT50"
    }
}

# =================================================
# FAST SYMBOL → SECTOR LOOKUP
# =================================================
SYMBOL_TO_SECTOR = {
    sym: sector
    for sector, syms in FULL_SECTORIAL_MAP.items()
    for sym in syms
}


fo["SECTOR"] = fo["SYMBOL"].map(SYMBOL_TO_SECTOR).fillna("UNMAPPED")

# =================================================
# FINAL OUTPUT LAYOUT
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
print("✔ FINAL SECTOR LAYOUT GENERATED")
print("Output   :", OUT_FILE)
print("Sectors  :", fo['SECTOR'].nunique())
print("Stocks   :", fo['SYMBOL'].nunique())
print("Unmapped :", (fo['SECTOR'] == 'UNMAPPED').sum())
