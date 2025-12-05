from sqlalchemy import create_engine
engine = create_engine("mssql+pyodbc://@localhost/whsestage?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&Encrypt=no")
with engine.connect() as conn:
    print(conn.execute("SELECT @@SERVERNAME, DB_NAME()").fetchone())

