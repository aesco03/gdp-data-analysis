from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()

user = os.getenv("MYSQL_USER")
pw   = os.getenv("MYSQL_PW")
host = os.getenv("MYSQL_HOST", "localhost")
port = os.getenv("MYSQL_PORT", "3306")

url = f"mysql+mysqlconnector://{user}:{pw}@{host}:{port}"
engine = create_engine(url, connect_args={"allow_local_infile": True})

# --- v2 pattern -------------------------------------------------
with engine.connect() as conn:
    ver = conn.execute(text("SELECT VERSION();")).scalar_one()
    print("Connected!  MySQL version →", ver)
# ----------------------------------------------------------------
