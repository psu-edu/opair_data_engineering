import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import warnings
import pyodbc
import matplotlib.pyplot as plt



# --- Suppress SQLAlchemy warning from pandas
warnings.filterwarnings("ignore", category=UserWarning, module='pandas.io.sql')

# --- Step 1: Define SQL Server connection details
server = 's8-whse-lst-a01.ad.psu.edu'  # or your actual server name 's8-whse-lst-p01.ad.psu.edu'
database = 'IDR'
schema = 'etl'  # <-- Update this to match your actual schema if different (use SSMS to check)

# --- Step 2: Set up the ODBC connection
conn = pyodbc.connect(
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    "Trusted_Connection=yes;"
)

# --- Step 3: Create SQLAlchemy engine for pandas
driver = 'ODBC+Driver+17+for+SQL+Server'
connection_string = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
engine = create_engine(connection_string)

# --- Step 4: Query the data (make sure schema/table name is correct)
query = f"""
SELECT FileCreated_On, fileimported
FROM {schema}.starfish_appointments
WHERE fileimported IS NOT NULL
"""

df = pd.read_sql(query, engine)

# --- Step 5: Extract date from filename like 'appointments_20240903T013611.csv'
def extract_file_date(filename):
    try:
        date_str = filename.split('_')[1][:8]  # Extract '20240903'
        return datetime.strptime(date_str, "%Y%m%d").date()
    except Exception:
        return None

df['FileDate'] = df['fileimported'].apply(extract_file_date)

# --- Step 6: Drop rows where date extraction failed
df = df.dropna(subset=['FileDate'])

# --- Step 7: Generate full range of expected dates
min_date = df['FileDate'].min()
max_date = df['FileDate'].max()
all_dates = pd.date_range(start=min_date, end=max_date).date

# --- Step 8: Find missing dates
actual_dates = set(df['FileDate'])
missing_dates = sorted(set(all_dates) - actual_dates)

# --- Step 9: Output missing dates
print("🔍 Missing file dates:")
for d in missing_dates:
    print(d)

# --- Step 10: Save results to CSV
output_file = 'missing_file_dates.csv'
pd.DataFrame(missing_dates, columns=['MissingFileDate']).to_csv(output_file, index=False)

print(f"\n✅ Total missing dates: {len(missing_dates)}")
print(f"📁 Results saved to: {output_file}")

# --- Step 11: Create a DataFrame of all expected dates
full_df = pd.DataFrame({'FileDate': all_dates})
full_df['Status'] = full_df['FileDate'].apply(lambda x: 'Missing' if x in missing_dates else 'Present')

# --- Step 12: Plot
plt.figure(figsize=(15, 5))
color_map = {'Present': 'green', 'Missing': 'red'}
colors = full_df['Status'].map(color_map)

plt.bar(full_df['FileDate'], [1]*len(full_df), color=colors, width=1.0)
plt.xticks(rotation=45)
plt.xlabel("Date")
plt.ylabel("Status")
# plt.title("📊 File Import Presence by Date")
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.yticks([])  # hide y-axis since it's just a presence marker

# --- Step 13: Show plot
plt.show()
