-- drop the old view first
DROP VIEW IF EXISTS vw_macro;

CREATE VIEW vw_macro AS
SELECT
  d.date_actual  AS date,
  d.date_id,
  MAX(CASE WHEN s.fred_code='GDPC1'            THEN f.value_num END) AS real_gdp,
  MAX(CASE WHEN s.fred_code='A679RC1Q027SBEA' THEN f.value_num END) AS ict_inv,
  -- fix this line to your actual code:
  MAX(CASE WHEN s.fred_code='Y006RC1Q027SBEA'  THEN f.value_num END) AS priv_rd,
  MAX(CASE WHEN s.fred_code='USREC'           THEN f.value_num END) AS recession_dummy,
  MAX(CASE WHEN s.fred_code='UNRATE'          THEN f.value_num END) AS unemp_rate,
  MAX(CASE WHEN s.fred_code='CPIAUCSL'        THEN f.value_num END) AS cpi,
  MAX(CASE WHEN s.fred_code = 'FEDFUNDS'        THEN f.value_num END) AS fedfunds,
  MAX(CASE WHEN s.fred_code = 'CIVPART'         THEN f.value_num END) AS lfpr
FROM fact_macro f
JOIN dim_series s USING(series_id)
JOIN dim_date   d USING(date_id)
GROUP BY d.date_actual, d.date_id
ORDER BY d.date_actual;
