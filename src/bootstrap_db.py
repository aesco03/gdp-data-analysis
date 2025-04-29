"""
bootstrap_db.py
Creates the econ_panel database AND the three core tables,
then prints the MySQL version to prove the connection works.
"""
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# ── 1. read credentials ──────────────────────────────────────────
load_dotenv()                                      # .env in same folder
user = os.getenv("MYSQL_USER", "root")
pw   = os.getenv("MYSQL_PW",   "")
host = os.getenv("MYSQL_HOST", "localhost")
port = os.getenv("MYSQL_PORT", "3306")
db   = os.getenv("MYSQL_DB",   "econ_panel")

# ── 2. connect to the *server*, no db name ───────────────────────
engine_root = create_engine(
    f"mysql+mysqlconnector://{user}:{pw}@{host}:{port}/",
    echo=False                        # turn to True if you want SQL logs
)

# ── 3. create the database if it doesn't exist ───────────────────
with engine_root.begin() as conn:
    conn.execute(text(
        f"CREATE DATABASE IF NOT EXISTS {db} "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    ))
print(f"✔  Database '{db}' ready.")

# ── 4. reconnect *into* the new db ───────────────────────────────
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pw}@{host}:{port}/{db}",
    connect_args={"allow_local_infile": True}
)

# ── 5. build tables (idempotent) ─────────────────────────────────
ddl = """
CREATE TABLE IF NOT EXISTS dim_date (
  date_id      INT PRIMARY KEY,
  date_actual  DATE NOT NULL,
  year_qtr     CHAR(7) NOT NULL,
  year         SMALLINT NOT NULL,
  quarter      TINYINT  NOT NULL,
  is_recession TINYINT(1)
);

CREATE TABLE IF NOT EXISTS dim_series (
  series_id    INT AUTO_INCREMENT PRIMARY KEY,
  fred_code    VARCHAR(25) UNIQUE,
  series_name  VARCHAR(120),
  units        VARCHAR(60),
  source       VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fact_macro (
  date_id    INT NOT NULL,
  series_id  INT NOT NULL,
  value_num  DECIMAL(18,4) NOT NULL,
  PRIMARY KEY (date_id, series_id),
  KEY idx_fact_date   (date_id),
  KEY idx_fact_series (series_id),
  FOREIGN KEY (date_id)   REFERENCES dim_date(date_id),
  FOREIGN KEY (series_id) REFERENCES dim_series(series_id)
);
"""
with engine.begin() as conn:
    for stmt in ddl.strip().split(";"):
        if stmt.strip():
            conn.execute(text(stmt))
print("✔  Tables created or verified.")

# ── 6. smoke-test query ──────────────────────────────────────────
with engine.connect() as conn:
    ver = conn.execute(text("SELECT VERSION();")).scalar_one()
    print("Connected! MySQL version →", ver)
