from sqlalchemy import create_engine, text
from ug_survey.config import load_settings

cfg = load_settings("config/settings.yaml")
engine = create_engine(cfg["database"]["url"], fast_executemany=True)

with engine.connect() as conn:
    row = conn.execute(text("SELECT @@SERVERNAME AS server_name, DB_NAME() AS db_name")).one()
    print(row.server_name, row.db_name)
