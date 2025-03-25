import pandas as pd
from sqlalchemy import create_engine, text, BINARY
import numpy as np
import logging

# Configure logging
logging.basicConfig(filename="etl_process.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Database connection details
server = 'localhost'
database = 'saraka_sandbox'
source_table = '[dbo].[appointments_stg]'
final_table = '[dbo].[starfish_appointments]'
staging_table = '[dbo].[staging_appointments]'

# Create SQLAlchemy engine using Windows Authentication
engine = create_engine(
    f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')

try:
    with engine.connect() as conn:
        logging.info("✅ Database connection established.")

        # 🔥 **Step 1: Fetch Latest Appointments**
        query = text(f"""
            WITH LastAppointment AS (
                SELECT 
                    CONCAT(appointment_ext_id, '_', participant_integration_id) AS INTEGRATION_ID,
                    appointment_ext_id,
                    participant_integration_id,
                    calendar_owner_integration_id,
                    calendar_owner_userid,
                    calendar_owner_first_name,
                    calendar_owner_last_name,
                    calendar_owner_institutional_email,
                    participant_userid,
                    participant_first_name,
                    participant_last_name,
                    participant_institutional_email,
                    participant_alternate_email,
                    appointment_type_reason,
                    appointment_start_date,
                    appointment_end_date,
                    appointment_location,
                    LEFT(appointment_description, 2000) AS appointment_description,  -- Truncate if needed
                    LEFT(appointment_comment, 2000) AS appointment_comment,  -- Truncate to prevent error
                    canceled_date,
                    course_section_id,
                    no_show,
                    appointment_type,
                    arrival_time,
                    wait_time,
                    actual_start_date,
                    actual_end_date,
                    actual_duration,
                    scheduled_duration,
                    scheduled,
                    waiting_room_status,
                    activities,
                    file_name AS FileImported,
                    file_created_on AS File_Created_On,
                    'SF_Appointments' AS ETL_SOURCE,
                    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', 
                        CONCAT(appointment_ext_id, participant_integration_id, 
                               appointment_type, scheduled, appointment_start_date, 
                               appointment_end_date))) AS Hash_DW,
                    ROW_NUMBER() OVER (PARTITION BY appointment_ext_id, participant_integration_id 
                                       ORDER BY file_created_on DESC) AS row_num
                FROM {source_table}
                WHERE appointment_ext_id <> 'None'
            )
            SELECT 
                INTEGRATION_ID, appointment_ext_id, participant_integration_id,
                calendar_owner_integration_id, calendar_owner_userid, 
                calendar_owner_first_name, calendar_owner_last_name, 
                calendar_owner_institutional_email, participant_userid, 
                participant_first_name, participant_last_name, 
                participant_institutional_email, participant_alternate_email, 
                appointment_type_reason, appointment_start_date, 
                appointment_end_date, appointment_location, appointment_description, 
                appointment_comment, canceled_date, course_section_id, no_show, 
                appointment_type, arrival_time, wait_time, actual_start_date, 
                actual_end_date, actual_duration, scheduled_duration, 
                scheduled, waiting_room_status, activities, FileImported, 
                File_Created_On, ETL_SOURCE, Hash_DW
            FROM LastAppointment WHERE row_num = 1;
        """)

        df = pd.read_sql(query, conn)
        logging.info(f"📊 Fetched {len(df)} records from source table.")

        # 🔥 **Step 2: Handle Oversized Text Fields in DataFrame**
        def truncate_column(value, max_length):
            """Truncates string values to a max length, if needed."""
            if isinstance(value, str) and len(value) > max_length:
                logging.warning(f"Truncating text in column to {max_length} chars: {value[:50]}...")
                return value[:max_length]  # Truncate text
            return value

        df['appointment_description'] = df['appointment_description'].apply(lambda x: truncate_column(x, 2000))
        df['appointment_comment'] = df['appointment_comment'].apply(lambda x: truncate_column(x, 2000))

        # 🔥 **Step 3: Insert into Staging Table**
        df.to_sql(staging_table.replace('[dbo].[', '').replace(']', ''), con=engine, if_exists='append', index=False,
                  dtype={'Hash_DW': BINARY(32)})

        logging.info(f"✅ Loaded {len(df)} records into staging table.")

except Exception as e:
    logging.error(f"❌ Error in ETL process: {e}")
    raise

print("ETL process completed successfully.")
