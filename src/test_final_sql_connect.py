# src/test_connection.py
"""
test_connection.py
Quick sanity check that you can:
  1) connect to econ_panel,
  2) read dim_date and dim_series counts,
  3) see fact_macro (probably zero until you load it).
"""
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

USER = os.getenv("MYSQL_USER", "root")
PW   = os.getenv("MYSQL_PW", "")
HOST = os.getenv("MYSQL_HOST", "localhost")
PORT = os.getenv("MYSQL_PORT", "3306")
DB   = os.getenv("MYSQL_DB", "econ_panel")

url = f"mysql+mysqlconnector://{USER}:{PW}@{HOST}:{PORT}/{DB}"
engine = create_engine(url, connect_args={"allow_local_infile": True})

with engine.connect() as conn:
    one = conn.execute(text("SELECT 1")).scalar_one()
    print("SELECT 1 →", one)
    dcount = conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar_one()
    scount = conn.execute(text("SELECT COUNT(*) FROM dim_series")).scalar_one()
    fcount = conn.execute(text("SELECT COUNT(*) FROM fact_macro")).scalar_one()
    print(f"dim_date rows:   {dcount}")
    print(f"dim_series rows: {scount}")
    print(f"fact_macro rows: {fcount}")
