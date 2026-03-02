import os
import psycopg2
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("InstrumentPopulator")

DB_CONF = {
    "host": os.getenv("POSTGRES_HOST", "postgres_metadata"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "password123"),
    "database": os.getenv("POSTGRES_DB", "quant_platform")
}

def populate():
    try:
        conn = psycopg2.connect(**DB_CONF)
        cur = conn.cursor()

        # 1. Create Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS instruments (
                instrument_token VARCHAR(255) PRIMARY KEY,
                exchange VARCHAR(50),
                segment VARCHAR(50),
                symbol VARCHAR(50)
            );
        """)

        # 2. Insert Dummy Data (Reliance, HDFC)
        # Upstox V3 uses ISIN format for equities: NSE_EQ|INE...
        instruments = [
            ("NSE_EQ|INE585B01010", "NSE_EQ", "EQUITY", "MARUTI"),
            ("NSE_EQ|INE917I01010", "NSE_EQ", "EQUITY", "BAJAJ-AUTO"),
            ("NSE_EQ|INE160A01022", "NSE_EQ", "EQUITY", "PNB"),
            ("NSE_EQ|INE814H01029", "NSE_EQ", "EQUITY", "ADANIPOWER"),
            ("NSE_EQ|INE102D01028", "NSE_EQ", "EQUITY", "GODREJCP"),
            ("NSE_EQ|INE134E01011", "NSE_EQ", "EQUITY", "PFC"),
            ("NSE_EQ|INE009A01021", "NSE_EQ", "EQUITY", "INFY"),
            ("NSE_EQ|INE237A01036", "NSE_EQ", "EQUITY", "KOTAKBANK"),
            ("NSE_EQ|INE361B01024", "NSE_EQ", "EQUITY", "DIVISLAB"),
            ("NSE_EQ|INE030A01027", "NSE_EQ", "EQUITY", "HINDUNILVR"),
            ("NSE_EQ|INE476A01022", "NSE_EQ", "EQUITY", "CANBK"),
            ("NSE_EQ|INE691A01018", "NSE_EQ", "EQUITY", "UCOBANK"),
            ("NSE_EQ|INE028A01039", "NSE_EQ", "EQUITY", "BANKBARODA"),
            ("NSE_EQ|INE670K01029", "NSE_EQ", "EQUITY", "LODHA"),
            ("NSE_EQ|INE158A01026", "NSE_EQ", "EQUITY", "HEROMOTOCO"),
            ("NSE_EQ|INE123W01016", "NSE_EQ", "EQUITY", "SBILIFE"),
            ("NSE_EQ|INE192A01025", "NSE_EQ", "EQUITY", "TATACONSUM"),
            ("NSE_EQ|INE094A01015", "NSE_EQ", "EQUITY", "HINDPETRO"),
            ("NSE_EQ|INE528G01035", "NSE_EQ", "EQUITY", "YESBANK"),
            ("NSE_EQ|INE849A01020", "NSE_EQ", "EQUITY", "TRENT"),
            ("NSE_EQ|INE669C01036", "NSE_EQ", "EQUITY", "TECHM"),
            ("NSE_EQ|INE216A01030", "NSE_EQ", "EQUITY", "BRITANNIA"),
            ("NSE_EQ|INE002S01010", "NSE_EQ", "EQUITY", "MGL"),
            ("NSE_EQ|INE062A01020", "NSE_EQ", "EQUITY", "SBIN"),
            ("NSE_EQ|INE081A01020", "NSE_EQ", "EQUITY", "TATASTEEL"),
            ("NSE_EQ|INE883A01011", "NSE_EQ", "EQUITY", "MRF"),
            ("NSE_EQ|INE075A01022", "NSE_EQ", "EQUITY", "WIPRO"),
            ("NSE_EQ|INE027H01010", "NSE_EQ", "EQUITY", "MAXHEALTH"),
            ("NSE_EQ|INE121A01024", "NSE_EQ", "EQUITY", "CHOLAFIN"),
            ("NSE_EQ|INE974X01010", "NSE_EQ", "EQUITY", "TIINDIA"),
            ("NSE_EQ|INE742F01042", "NSE_EQ", "EQUITY", "ADANIPORTS"),
            ("NSE_EQ|INE047A01021", "NSE_EQ", "EQUITY", "GRASIM"),
            ("NSE_EQ|INE213A01029", "NSE_EQ", "EQUITY", "ONGC"),
            ("NSE_EQ|INE053F01010", "NSE_EQ", "EQUITY", "IRFC"),
            ("NSE_EQ|INE021A01026", "NSE_EQ", "EQUITY", "ASIANPAINT"),
            ("NSE_EQ|INE733E01010", "NSE_EQ", "EQUITY", "NTPC"),
            ("NSE_EQ|INE565A01014", "NSE_EQ", "EQUITY", "IOB"),
            ("NSE_EQ|INE239A01024", "NSE_EQ", "EQUITY", "NESTLEIND"),
            ("NSE_EQ|INE437A01024", "NSE_EQ", "EQUITY", "APOLLOHOSP"),
            ("NSE_EQ|INE399L01023", "NSE_EQ", "EQUITY", "ATGL"),
            ("NSE_EQ|INE019A01038", "NSE_EQ", "EQUITY", "JSWSTEEL"),
            ("NSE_EQ|INE522F01014", "NSE_EQ", "EQUITY", "COALINDIA"),
            ("NSE_EQ|INE296A01032", "NSE_EQ", "EQUITY", "BAJFINANCE"),
            ("NSE_EQ|INE066F01020", "NSE_EQ", "EQUITY", "HAL"),
            ("NSE_EQ|INE002A01018", "NSE_EQ", "EQUITY", "RELIANCE"),
            ("NSE_EQ|INE203G01027", "NSE_EQ", "EQUITY", "IGL"),
            ("NSE_EQ|INE467B01029", "NSE_EQ", "EQUITY", "TCS"),
            ("NSE_EQ|INE040A01034", "NSE_EQ", "EQUITY", "HDFCBANK"),
            ("NSE_EQ|INE066A01021", "NSE_EQ", "EQUITY", "EICHERMOT"),
            ("NSE_EQ|INE844O01030", "NSE_EQ", "EQUITY", "GUJGASLTD"),
            ("NSE_EQ|INE752E01010", "NSE_EQ", "EQUITY", "POWERGRID"),
            ("NSE_EQ|INE271C01023", "NSE_EQ", "EQUITY", "DLF"),
            ("NSE_EQ|INE318A01026", "NSE_EQ", "EQUITY", "PIDILITIND"),
            ("NSE_EQ|INE042A01014", "NSE_EQ", "EQUITY", "ESCORTS"),
            ("NSE_EQ|INE918I01026", "NSE_EQ", "EQUITY", "BAJAJFINSV"),
            ("NSE_EQ|INE758E01017", "NSE_EQ", "EQUITY", "JIOFIN"),
            ("NSE_EQ|INE089A01031", "NSE_EQ", "EQUITY", "DRREDDY"),
            ("NSE_EQ|INE494B01023", "NSE_EQ", "EQUITY", "TVSMOTOR"),
            ("NSE_EQ|INE646L01027", "NSE_EQ", "EQUITY", "INDIGO"),
            ("NSE_EQ|INE397D01024", "NSE_EQ", "EQUITY", "BHARTIARTL"),
            ("NSE_EQ|INE775A08105", "NSE_EQ", "EQUITY", "MOTHERSON"),
            ("NSE_EQ|INE059A01026", "NSE_EQ", "EQUITY", "CIPLA"),
            ("NSE_EQ|INE949L01017", "NSE_EQ", "EQUITY", "AUBANK"),
            ("NSE_EQ|INE280A01028", "NSE_EQ", "EQUITY", "TITAN"),
            ("NSE_EQ|INE298A01020", "NSE_EQ", "EQUITY", "CUMMINSIND"),
            ("NSE_EQ|INE095A01012", "NSE_EQ", "EQUITY", "INDUSINDBK"),
            ("NSE_EQ|INE562A01011", "NSE_EQ", "EQUITY", "INDIANB"),
            ("NSE_EQ|INE364U01010", "NSE_EQ", "EQUITY", "ADANIGREEN"),
            ("NSE_EQ|INE238A01034", "NSE_EQ", "EQUITY", "AXISBANK"),
            ("NSE_EQ|INE044A01036", "NSE_EQ", "EQUITY", "SUNPHARMA"),
            ("NSE_EQ|INE038A01020", "NSE_EQ", "EQUITY", "HINDALCO"),
            ("NSE_EQ|INE242A01010", "NSE_EQ", "EQUITY", "IOC"),
            ("NSE_EQ|INE692A01016", "NSE_EQ", "EQUITY", "UNIONBANK"),
            ("NSE_EQ|INE263A01024", "NSE_EQ", "EQUITY", "BEL"),
            ("NSE_EQ|INE020B01018", "NSE_EQ", "EQUITY", "RECLTD"),
            ("NSE_EQ|INE860A01027", "NSE_EQ", "EQUITY", "HCLTECH"),
            ("NSE_EQ|INE457A01014", "NSE_EQ", "EQUITY", "MAHABANK"),
            ("NSE_EQ|INE171A01029", "NSE_EQ", "EQUITY", "FEDERALBNK"),
            ("NSE_EQ|INE323A01026", "NSE_EQ", "EQUITY", "BOSCHLTD"),
            ("NSE_EQ|INE176B01034", "NSE_EQ", "EQUITY", "HAVELLS"),
            ("NSE_EQ|INE545U01014", "NSE_EQ", "EQUITY", "BANDHANBNK"),
            ("NSE_EQ|INE154A01025", "NSE_EQ", "EQUITY", "ITC"),
            ("NSE_EQ|INE101A01026", "NSE_EQ", "EQUITY", "M&M"),
            ("NSE_EQ|INE208A01029", "NSE_EQ", "EQUITY", "ASHOKLEY"),
            ("NSE_EQ|INE303R01014", "NSE_EQ", "EQUITY", "KALYANKJIL"),
            ("NSE_EQ|INE090A01021", "NSE_EQ", "EQUITY", "ICICIBANK"),
            ("NSE_EQ|INE787D01026", "NSE_EQ", "EQUITY", "BALKRISIND"),
            ("NSE_EQ|INE018A01030", "NSE_EQ", "EQUITY", "LT"),
            ("NSE_EQ|INE092T01019", "NSE_EQ", "EQUITY", "IDFCFIRSTB"),
            ("NSE_EQ|INE347G01014", "NSE_EQ", "EQUITY", "PETRONET"),
            ("NSE_EQ|INE103A01014", "NSE_EQ", "EQUITY", "MRPL"),
            ("NSE_EQ|INE067A01029", "NSE_EQ", "EQUITY", "CGPOWER"),
            ("NSE_EQ|INE423A01024", "NSE_EQ", "EQUITY", "ADANIENT"),
            ("NSE_EQ|INE259A01022", "NSE_EQ", "EQUITY", "COLPAL"),
            ("NSE_EQ|INE257A01026", "NSE_EQ", "EQUITY", "BHEL"),
            ("NSE_EQ|INE699H01024", "NSE_EQ", "EQUITY", "AWL"),
            ("NSE_EQ|INE129A01019", "NSE_EQ", "EQUITY", "GAIL"),
            ("NSE_EQ|INE481G01011", "NSE_EQ", "EQUITY", "ULTRACEMCO"),
            ("NSE_EQ|INE003A01024", "NSE_EQ", "EQUITY", "SIEMENS"),
            ("NSE_EQ|INE029A01011", "NSE_EQ", "EQUITY", "BPCL"),
            ("NSE_EQ|INE200M01039", "NSE_EQ", "EQUITY", "VBL"),
        ]

        for token, exch, seg, sym in instruments:
            cur.execute("""
                INSERT INTO instruments (instrument_token, exchange, segment, symbol)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (instrument_token) DO NOTHING;
            """, (token, exch, seg, sym))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"✅ Populated {len(instruments)} instruments into Postgres.")

    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    populate()
