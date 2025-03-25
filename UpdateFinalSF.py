import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

# Define database connection
server = 'localhost'
database = 'saraka_sandbox'
source_table = '[dbo].[appointments_stg]'
final_table = 'starfish_appointments'
staging_table = 'staging_appointments'  # Using a persistent staging table

# Create an engine using Windows Authentication
engine = create_engine(f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
# Step 1: Fetch the latest appointment per (appointment_ext_id, participant_integration_id)
query = f"""
WITH LastAppointment AS (
    SELECT 
            CONCAT(appointment_ext_id, '_', participant_integration_id) AS INTEGRATION_ID,
            appointment_ext_id,
            calendar_owner_integration_id,
            calendar_owner_userid,
            calendar_owner_first_name,
            calendar_owner_last_name,
            calendar_owner_institutional_email,
            participant_integration_id,
            participant_userid,
            participant_first_name,
            participant_last_name,
            participant_institutional_email,
            participant_alternate_email,
            appointment_type_reason,
            appointment_start_date,
            appointment_end_date,
            appointment_location,
            appointment_description,
            appointment_comment,
            canceled_date,
            course_section_id,

            -- Extract class details based on course_section_id length
            CASE 
                WHEN LEN(course_section_id) = 14 THEN SUBSTRING(course_section_id, 1, 4)
                WHEN LEN(course_section_id) = 13 THEN SUBSTRING(course_section_id, 1, 4)
                WHEN LEN(course_section_id) = 15 THEN SUBSTRING(course_section_id, 1, 4)
                WHEN LEN(course_section_id) = 16 THEN SUBSTRING(course_section_id, 1, 4)
                ELSE NULL 
            END AS class_term_code,

            CASE 
                WHEN LEN(course_section_id) = 14 THEN SUBSTRING(course_section_id, 5, 1)
                WHEN LEN(course_section_id) = 13 THEN SUBSTRING(course_section_id, 5, 1)
                WHEN LEN(course_section_id) = 15 THEN SUBSTRING(course_section_id, 5, 3)
                WHEN LEN(course_section_id) = 16 THEN SUBSTRING(course_section_id, 5, 3)
                ELSE NULL 
            END AS class_session_code,

            CASE 
                WHEN LEN(course_section_id) = 14 THEN SUBSTRING(course_section_id, 6, 4)
                WHEN LEN(course_section_id) = 13 THEN SUBSTRING(course_section_id, 6, 4)
                WHEN LEN(course_section_id) = 15 THEN SUBSTRING(course_section_id, 8, 4)
                WHEN LEN(course_section_id) = 16 THEN SUBSTRING(course_section_id, 8, 4)
                ELSE NULL 
            END AS crse_car_code,

            CASE 
                WHEN LEN(course_section_id) = 14 THEN SUBSTRING(course_section_id, 10, 5)
                WHEN LEN(course_section_id) = 13 THEN SUBSTRING(course_section_id, 10, 4)
                WHEN LEN(course_section_id) = 15 THEN SUBSTRING(course_section_id, 12, 4)
                WHEN LEN(course_section_id) = 16 THEN SUBSTRING(course_section_id, 12, 5)
                ELSE NULL 
            END AS class_nbr,

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

            -- Hash column for change detection
            HASHBYTES('SHA2_256', 
                      CONCAT(appointment_ext_id, participant_integration_id, 
                             appointment_type, scheduled, appointment_start_date, 
                             appointment_end_date)) AS Hash_DW,

            ROW_NUMBER() OVER (PARTITION BY appointment_ext_id, participant_integration_id ORDER BY file_created_on DESC) AS row_num
        
    FROM {source_table} where appointment_ext_id <> 'None'
)
SELECT * FROM LastAppointment WHERE row_num = 1;

"""
df = pd.read_sql(query, engine)

# Replace string 'None' with actual NULL (NaN)
#df.replace("None", np.nan, inplace=True)
pd.set_option('future.no_silent_downcasting', True)
df.replace("None", np.nan, inplace=True)

# Step 2: Create a Staging Table (if not exists)
with engine.connect() as conn:
    conn.execute(text(f"""
    IF OBJECT_ID('{staging_table}', 'U') IS NULL
    CREATE TABLE {staging_table} (
        INTEGRATION_ID VARCHAR(255) PRIMARY KEY,
        appointment_ext_id VARCHAR(255),
        participant_integration_id VARCHAR(255),
        calendar_owner_integration_id VARCHAR(255),
        calendar_owner_userid VARCHAR(255),
        calendar_owner_first_name VARCHAR(255),
        calendar_owner_last_name VARCHAR(255),
        calendar_owner_institutional_email VARCHAR(255),
        participant_userid VARCHAR(255),
        participant_first_name VARCHAR(255),
        participant_last_name VARCHAR(255),
        participant_institutional_email VARCHAR(255),
        participant_alternate_email VARCHAR(255),
        appointment_type_reason VARCHAR(255),
        appointment_start_date DATETIME,
        appointment_end_date DATETIME,
        appointment_location VARCHAR(255),
        appointment_description TEXT,
        appointment_comment TEXT,
        canceled_date DATETIME,
        course_section_id VARCHAR(255),
		class_term_code VARCHAR(10),
		class_session_code VARCHAR(10),
		crse_car_code VARCHAR(10),
		class_nbr VARCHAR(10),
		no_show BIT,
        appointment_type VARCHAR(255),
        arrival_time DATETIME,
        wait_time INT,
        actual_start_date DATETIME,
        actual_end_date DATETIME,
        actual_duration INT,
        scheduled_duration INT,
        scheduled BIT,
        waiting_room_status VARCHAR(255),
        activities TEXT,
        imported_on datetime,
        FileImported varchar(255),
        File_Created_On datetime,
        ETL_Source varchar(25),
        -- Hash column for change detection
        Hash_DW varchar(100)
    );
    """))
    conn.commit()


# Step 3: Load Data into Staging Table
df.to_sql(staging_table, con=engine, if_exists='replace', index=False)

# Step 4: Merge Staging Table with Final Table
merge_query = f"""
MERGE INTO {final_table} AS target
USING {staging_table} AS source
ON target.appointment_ext_id = source.appointment_ext_id 
AND target.participant_integration_id = source.participant_integration_id

WHEN MATCHED AND source.File_Created_On > target.File_Created_On THEN
    UPDATE SET 
        target.INTEGRATION_ID = source.INTEGRATION_ID,
      target.appointment_ext_id = source.appointment_ext_id,
      target.calendar_owner_integration_id = source.calendar_owner_integration_id,
      target.calendar_owner_userid = source.calendar_owner_userid,
      target.calendar_owner_first_name = source.calendar_owner_first_name,
      target.calendar_owner_last_name = source.calendar_owner_last_name,
      target.calendar_owner_institutional_email = source.calendar_owner_institutional_email,
      target.participant_integration_id = source.participant_integration_id,
      target.participant_userid = source.participant_userid,
      target.participant_first_name = source.participant_first_name,
      target.participant_last_name = source.participant_last_name,
      target.participant_institutional_email = source.participant_institutional_email,
      target.participant_alternate_email = source.participant_alternate_email,
      target.appointment_type_reason = source.appointment_type_reason,
      target.appointment_start_date = source.appointment_start_date,
      target.appointment_end_date = source.appointment_end_date,
      target.appointment_location = source.appointment_location,
      target.appointment_description = source.appointment_description,
      target.appointment_comment = source.appointment_comment,
      target.canceled_date = source.canceled_date,
      target.course_section_id = source.course_section_id,
      target.class_term_code = source.class_term_code,
      target.class_session_code = source.class_session_code,
      target.crse_car_code = source.crse_car_code,
      target.class_nbr = source.class_nbr,
      target.no_show = source.no_show,
      target.appointment_type = source.appointment_type,
      target.arrival_time = source.arrival_time,
      target.wait_time = source.wait_time,
      target.actual_start_date = source.actual_start_date,
      target.actual_end_date = source.actual_end_date,
      target.actual_duration = source.actual_duration,
      target.scheduled_duration = source.scheduled_duration,
      target.scheduled = source.scheduled,
      target.waiting_room_status = source.waiting_room_status,
      target.activities = source.activities,
      --target.DELETE_FLG = source.DELETE_FLG,
      target.File_Created_On = source.file_created_on,
      --target.UPDATE_DT = source.UPDATE_DT,
      target.ETL_SOURCE = source.ETL_SOURCE,
      target.FileImported = source.FileImported,
     target.Hash_DW = source.Hash_DW

WHEN NOT MATCHED THEN
    INSERT (
         INTEGRATION_ID
      ,appointment_ext_id
      ,calendar_owner_integration_id
      ,calendar_owner_userid
      ,calendar_owner_first_name
      ,calendar_owner_last_name
      ,calendar_owner_institutional_email
      ,participant_integration_id
      ,participant_userid
      ,participant_first_name
      ,participant_last_name
      ,participant_institutional_email
      ,participant_alternate_email
      ,appointment_type_reason
      ,appointment_start_date
      ,appointment_end_date
      ,appointment_location
      ,appointment_description
      ,appointment_comment
      ,canceled_date
      ,course_section_id
      ,class_term_code
      ,class_session_code
      ,crse_car_code
      ,class_nbr
      ,no_show
      ,appointment_type
      ,arrival_time
      ,wait_time
      ,actual_start_date
      ,actual_end_date
      ,actual_duration
      ,scheduled_duration
      ,scheduled
      ,waiting_room_status
      ,activities
      --,DELETE_FLG
      ,File_Created_On
      --,UPDATE_DT
      ,ETL_SOURCE
      ,FileImported
      --,FileSize
      ,Hash_DW
    )
    VALUES (
        source.INTEGRATION_ID,
        source.appointment_ext_id,
        source.calendar_owner_integration_id,
        source.calendar_owner_userid,
        source.calendar_owner_first_name,
        source.calendar_owner_last_name,
        source.calendar_owner_institutional_email,
        source.participant_integration_id,
        source.participant_userid,
        source.participant_first_name,
        source.participant_last_name,
        source.participant_institutional_email,
        source.participant_alternate_email,
        source.appointment_type_reason,
        source.appointment_start_date,
        source.appointment_end_date,
        source.appointment_location,
        source.appointment_description,
        source.appointment_comment,
        source.canceled_date,
        source.course_section_id,
        source.class_term_code,
        source.class_session_code,
        source.crse_car_code,
        source.class_nbr,
        source.no_show,
        source.appointment_type,
        source.arrival_time,
        source.wait_time,
        source.actual_start_date,
        source.actual_end_date,
        source.actual_duration,
        source.scheduled_duration,
        source.scheduled,
        source.waiting_room_status,
        source.activities,
        source.File_Created_On,
        source.ETL_SOURCE,
        source.FileImported,
        source.Hash_DW
        
        
    );
"""

# Step 5: Execute Merge Statement
with engine.connect() as conn:
    conn.execute(text(merge_query))
    conn.commit()

print("Final table updated successfully.")
