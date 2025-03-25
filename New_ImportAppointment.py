import os
import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
import re
import fnmatch
import shutil
import sys  # Use sys.exit() instead of exit()

# 📂 Directories
csv_directory = r"E:\dat\Starfish"
archive_directory = os.path.join(csv_directory, "Starfish_Archive", "appointments")
os.makedirs(archive_directory, exist_ok=True)

# Database connection parameters
server = 'localhost'
database = 'saraka_sandbox'
table_name = 'appointments_stg'
error_log_table = 'error_log'  # Ensure error log table exists

# Create connection string
conn_str = f"""
DRIVER={{ODBC Driver 17 for SQL Server}};
SERVER={server};
DATABASE={database};
Trusted_Connection=yes;
"""
# Connect to the database
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connection successful!")
except Exception as e:
    print(f"Database connection failed: {e}")
    sys.exit()

# Define expected data types for the CSV columns
dtype_map = {
    'appointment_ext_id': 'str',
    'calendar_owner_integration_id': 'str',
    'calendar_owner_userid': 'str',
    'calendar_owner_first_name': 'str',
    'calendar_owner_last_name': 'str',
    'calendar_owner_institutional_email': 'str',
    'participant_integration_id': 'str',
    'participant_userid': 'str',
    'participant_first_name': 'str',
    'participant_last_name': 'str',
    'participant_institutional_email': 'str',
    'participant_alternate_email': 'str',
    'appointment_type_reason': 'str',
    'appointment_start_date': 'str',
    'appointment_end_date': 'str',
    'appointment_location': 'str',
    'appointment_description': 'str',
    'appointment_comment': 'str',
    'canceled_date': 'str',
    'course_section_id': 'str',
    'no_show': 'boolean',
    'appointment_type': 'str',
    'arrival_time': 'str',
    'wait_time': 'str',
    'actual_start_date': 'str',
    'actual_end_date': 'str',
    'actual_duration': 'str',
    'scheduled_duration': 'str',
    'scheduled': 'boolean',
    'waiting_room_status': 'str',
    'activities': 'str',
}

datetime_columns = [
    'appointment_start_date',
    'appointment_end_date',
    'canceled_date',
    'actual_start_date',
    'actual_end_date'
]

numeric_columns = ['wait_time', 'actual_duration', 'scheduled_duration']


# Preprocessing function
def preprocess_dataframe(df):
    datetime_format = "%Y-%m-%d %H:%M:%S"  # Adjust format based on your data

    for column, col_type in dtype_map.items():
        if column not in df.columns:
            continue

        if column in datetime_columns:
            df[column] = df[column].astype(str).str.replace(r'\s+[A-Z]{3,}$', '', regex=True)
            df[column] = pd.to_datetime(df[column], format=datetime_format, errors='coerce')

        elif col_type == 'str':
            df[column] = df[column].astype(str).replace({pd.NA: '', 'nan': '', None: ''})

        elif col_type == 'boolean':
            df[column] = df[column].astype(str).str.lower().map(
                {'true': True, '1': True, 'yes': True, 'false': False, '0': False, 'no': False}
            ).fillna(False).astype(bool)

    # Convert numeric columns properly
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert invalid numbers to NaN

    return df


# Get file creation date
def get_file_creation_date(file_path):
    try:
        # Extract the filename from the path
        file_name = os.path.basename(file_path)

        # Extract the part after the last underscore using regex
        match = re.search(r'_(\d{8}T\d{6})\b', file_name)
        if match:
            date_str = match.group(1)
            return datetime.strptime(date_str, "%Y%m%dT%H%M%S")

        return datetime.fromtimestamp(os.path.getmtime(file_path))

    except Exception as e:
        print(f"Error extracting creation date from {file_path}: {e}")
        return None

# Detect problematic column
def detect_problematic_column(row, error_message, dtype_map):
    """
    Detects the problematic column in a row based on the error message.
    """
    print(f"Debug: Detecting problematic column for row: {row.to_dict()} with error: {error_message}")

    # Check for float conversion errors
    if "could not convert string to float" in error_message.lower():
        for col in row.index:
            expected_type = dtype_map.get(col)
            if expected_type == 'float':
                try:
                    # Attempt to cast to float
                    print(f"Debug: Trying to convert column '{col}' with value '{row[col]}' to float.")
                    float(row[col])  # Test conversion
                except (ValueError, TypeError):
                    print(f"Debug: Problematic column detected: {col}")
                    return col  # Return problematic column
    print("Debug: No problematic column detected.")
    return None

# Ensure directory exists before processing
if not os.path.exists(csv_directory):
    print(f"Error: Directory '{csv_directory}' does not exist.")
    sys.exit()

# Main loop
for file in os.listdir(csv_directory):
    if fnmatch.fnmatch(file, "appoint*.csv"):
        file_path = os.path.join(csv_directory, file)
        print(f"Processing file: {file_path}")

        try:
            file_created_on = get_file_creation_date(file_path)  # Pass full path
            df = pd.read_csv(
                file_path,
                dtype={col: dtype_map[col] for col in dtype_map if dtype_map[col] != 'boolean'},
                header=0,
                low_memory=False
            )

            # Replace NaN values
            df.replace({np.nan: None}, inplace=True)

            # Preprocess DataFrame
            df = preprocess_dataframe(df)

            df['file_name'] = file
            df['imported_on'] = datetime.now()
            df['file_created_on'] = file_created_on

            # Truncate stage table
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            inserted_count = 0

            for index, row in df.iterrows():
                try:
                    insert_query = f"""
                        INSERT INTO {table_name} ({', '.join(df.columns)})
                        VALUES ({', '.join(['?' for _ in df.columns])})
                    """
                    cursor.execute(insert_query, [None if pd.isna(v) else v for v in row])
                    inserted_count += 1
                except Exception as e:
                    print(f"Error inserting row {index}: {e}")
                    print(f"Row causing the error: {row.to_dict()}")

            conn.commit()
            print(f"✅ Successfully inserted {inserted_count} rows into {table_name}.")

            # ✅ Move the file to the archive directory
            archive_path = os.path.join(archive_directory, file)
            shutil.move(file_path, archive_path)
            print(f"✅ File archived: {archive_path}")

        except Exception as e:
            print(f"❌ Error processing file {file}: {e}")

            # Ensure error log query is defined **before** execution
            error_log_insert = f"""
                INSERT INTO {error_log_table} (file_name, row_data, error_message, problematic_column)
                VALUES (?, ?, ?, ?);
            """

            # Extract problematic column dynamically if possible
            problematic_column = "Unknown"
            match = re.search(r"column '(\w+)'", str(e))
            if match:
                problematic_column = match.group(1)
            cursor.execute(error_log_insert, (file, "N/A", str(e), problematic_column))
            conn.commit()

# Close the connection
conn.close()
print("Data import completed.")
