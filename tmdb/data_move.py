import sqlite3
import pandas as pd
from google.oauth2 import service_account

# 1. Configuration
DB_FILE = "TMDB-a-4006.db"
PROJECT_ID = "majindogo-data-project" # Your GCP project ID
DATASET_ID = "tmdb"
KEY_PATH = "C:\\Users\\Kanyinsola\\Desktop\\Kay\\majindogo-data-project-fc16daa482cb.json" # Your JSON credentials

# 2. Authenticate
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

# 3. Connect to the .db file
conn = sqlite3.connect(DB_FILE)

# 4. Get all table names from the database
query = "SELECT name FROM sqlite_master WHERE type='table';"
tables = pd.read_sql(query, conn)['name'].tolist()

# 5. Upload each table to BigQuery
for table_name in tables:
    print(f"Uploading table: {table_name}...")
    
    # Read table into a DataFrame
    df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    
    # Upload to BigQuery
    # destination_table uses format 'dataset.table'
    df.to_gbq(
        destination_table=f"{DATASET_ID}.{table_name}",
        project_id=PROJECT_ID,
        credentials=credentials,
        if_exists='replace' # Options: 'fail', 'replace', 'append'
    )

print("All tables uploaded successfully!")
conn.close()