# test_connection.py
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# 1. Load the same .env credentials
load_dotenv()
user = os.getenv("MYSQL_USER", "root")
pw   = os.getenv("MYSQL_PW", "")
host = os.getenv("MYSQL_HOST", "localhost")
port = os.getenv("MYSQL_PORT", "3306")
db   = os.getenv("MYSQL_DB", "econ_panel")

# 2. Build the engine just like your loader does
url = f"mysql+mysqlconnector://{user}:{pw}@{host}:{port}/{db}"
engine = create_engine(url, connect_args={"allow_local_infile": True})

# 3. Open a connection and run a quick test
with engine.connect() as conn:
    # A. Test simple connectivity
    one = conn.execute(text("SELECT 1")).scalar_one()
    print("SELECT 1 →", one)

    # B. Check fact_macro row count
    count = conn.execute(text("SELECT COUNT(*) FROM fact_macro")).scalar_one()
    print("Rows in fact_macro →", count)
