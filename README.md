# gdp-data-analysis


pip install -r requirements.txt

Teammate A/B/C download CSVs 
        ↓
They save raw files in /data/raw/
        ↓
They write scripts in src/load_data_x.py to:
        - read those CSVs
        - clean them
        - upload them to the SQL database