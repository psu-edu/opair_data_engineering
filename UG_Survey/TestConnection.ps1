$env:MSSQL_URL = "mssql+pyodbc://@localhost/whsestage?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&TrustServerCertificate=yes"

$code = @"
from sqlalchemy import create_engine, text
import os
engine = create_engine(rf"{os.environ['MSSQL_URL']}")
with engine.connect() as conn:
    print("Connected to:", conn.execute(text("SELECT DB_NAME()")).scalar())
"@
$path = "$env:TEMP\test_conn.py"
$code | Set-Content -Path $path -Encoding UTF8
python $path
