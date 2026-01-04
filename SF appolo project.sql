CREATE OR REPLACE SCHEMA APPOLO_HC.RAW_SCH;

select * from APPOLO_HC.RAW_SCH.APPOINTMENT_RAW;

SHOW STORAGE INTEGRATIONS;

DESC INTEGRATION STORAGE_S3_INTEGRATION;


CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL','null');

CREATE OR REPLACE STAGE s3_stage
URL = 's3://sf-project-manojkrishna/sftp/'
STORAGE_INTEGRATION = STORAGE_S3_INTEGRATION
FILE_FORMAT = my_csv_format;

LIST @s3_stage/Appointment/;


CREATE OR REPLACE TABLE APPOLO_HC.RAW_SCH.APPOINTMENT_RAW (
    appointment_id TEXT,
    patient_id TEXT,
    doctor_id TEXT,
    department_id TEXT,
    prescription_id TEXT,
    appointment_date TEXT,
    amount_billed TEXT,
    discount TEXT,
    final_amount TEXT,
    created_date TEXT,
    modified_date TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COPY INTO APPOLO_HC.RAW_SCH.APPOINTMENT_RAW
(
  appointment_id,
  patient_id,
  doctor_id,
  department_id,
  prescription_id,
  appointment_date,
  amount_billed,
  discount,
  final_amount,
  created_date,
  modified_date
)
FROM (
  SELECT
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10, $11
  FROM @s3_stage/Appointment/appointment_dim_part1.csv
)
FILE_FORMAT = my_csv_format
ON_ERROR = CONTINUE;

--count the number of rows loaded
SELECT
  COUNT(*) AS rows_loaded,
  MIN(loaded_at),
  MAX(loaded_at)
FROM APPOLO_HC.RAW_SCH.APPOINTMENT_RAW;

SELECT * FROM APPOLO_HC.RAW_SCH.APPOINTMENT_RAW;
--Check load history:
select *
from table (information_schema.copy_history(
table_name => 'appointment_raw',
start_time => dateadd('hour', -1, current_timestamp())
));

--to check header names
SELECT 
  $1, $2, $3, $4, $5, $6, $7, $8,
  $9, $10, $11, $12, $13, $14, $15      -- number of columns 
FROM @s3_stage/sftp/Department/department_dim.csv
(FILE_FORMAT => 'my_csv_format')   -- format should have skip_header = 0   
LIMIT 10;                             --  till how many rows u want to see

LIST @s3_stage/Department/;  --till how many rows u want to see

ALTER FILE FORMAT my_csv_format SET SKIP_HEADER = 1;

ALTER FILE FORMAT temp_csv_no_skip SET ENCODING = 'WINDOWS1252';

CREATE OR REPLACE TEMPORARY FILE FORMAT temp_csv_no_skip
  TYPE = 'CSV'
  SKIP_HEADER = 0
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = '\\'
  NULL_IF = ('\\N', 'NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE;
  

-- To check the header names
  SELECT
  $1  AS header_col1,
  $2  AS header_col2,
  $3  AS header_col3,
  $4  AS header_col4,
  $5  AS header_col5,
  $6  AS header_col6
  -- Add more if needed
FROM @s3_stage/Department/department_dim.csv
(FILE_FORMAT => 'temp_csv_no_skip')
WHERE METADATA$FILE_ROW_NUMBER = 1;

--  header names reasult
-- DepartmentID	 DepartmentName	  CreatedDate	ModifiedDate

CREATE OR REPLACE TABLE APPOLO_HC.RAW_SCH.DEPARTMENT_RAW (
    Department_id TEXT,
    Department_name TEXT,
    created_date TEXT,
    modified_date TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COPY INTO APPOLO_HC.RAW_SCH.DEPARTMENT_RAW
(
    Department_id,
    Department_name,
    created_date,
    modified_date
)
FROM (
  SELECT
    $1, $2, $3, $4
  FROM @s3_stage/Department/department_dim.csv
)
FILE_FORMAT = my_csv_format
ON_ERROR = CONTINUE;


SELECT * FROM APPOLO_HC.RAW_SCH.DEPARTMENT_RAW;

--TRUNCATE TABLE APPOLO_HC.RAW_SCH.DEPARTMENT_RAW;
--s3://sf-project-manojkrishna/sftp/Patient/patient_dim_part1.csv

-- PatientID	FullName	Age	Gender	ContactNumber	Address	CreatedDate	ModifiedDate

CREATE OR REPLACE TABLE APPOLO_HC.RAW_SCH.PATIENT_RAW (
    Patient_id TEXT,
    FullName TEXT,
    Age TEXT,
    Gender TEXT,
    ContactNumber TEXT,
    Address TEXT,
    created_date TEXT,
    modified_date TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


COPY INTO APPOLO_HC.RAW_SCH.PATIENT_RAW
(
    Patient_id,
    FullName,
    Age,
    Gender,
    ContactNumber,
    Address,
    created_date,
    modified_date
)
FROM (
  SELECT
    $1, $2, $3, $4, $5, $6, $7, $8
  FROM @s3_stage/Patient/patient_dim_part1.csv
)
FILE_FORMAT = my_csv_format
ON_ERROR = CONTINUE;

SELECT * FROM APPOLO_HC.RAW_SCH.PATIENT_RAW;

   
 DELETE FROM APPOLO_HC.RAW_SCH.PATIENT_RAW
WHERE PATIENT_ID = 'PatientID'
  AND FULLNAME   = 'FullName'
  AND AGE        = 'Age'
  AND GENDER     = 'Gender'
  AND ContactNumber = 'ContactNumber'
  AND Address = 'Address'
  AND created_date = 'CreatedDate'
  AND modified_date = 'ModifiedDate';

SELECT * FROM APPOLO_HC.RAW_SCH.PATIENT_RAW;
  

-- Quick row count + timestamps
SELECT 
    COUNT(*) AS total_rows,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS last_load
FROM APPOLO_HC.RAW_SCH.DEPARTMENT_RAW;

-- Look at the first 5-10 rows to confirm header is skipped and data looks clean
SELECT * 
FROM APPOLO_HC.RAW_SCH.DEPARTMENT_RAW 
ORDER BY loaded_at 
LIMIT 10;

-- Check if any obvious header snuck in (should return 0 rows)
SELECT * 
FROM APPOLO_HC.RAW_SCH.DEPARTMENT_RAW 
WHERE Department_id ILIKE '%DepartmentID%' 
   OR Department_name ILIKE '%DepartmentName%';


   select distinct a.patient_id
from APPOLO_HC.RAW_SCH.appointment_raw a
left join APPOLO_HC.RAW_SCH.patient_raw p
  on a.patient_id = p.patient_id
where p.patient_id is null
  and a.patient_id != 'AppointmentID';

DELETE
FROM APPOLO_HC.RAW_SCH.appointment_raw
WHERE patient_id = '10101';


-- In Snowflake
SELECT COUNT(*) AS remaining_orphans
FROM DBT_DATA.DBT_SCHEMA.STG_APPOINTMENT a
LEFT JOIN APPOLO_HC.RAW_SCH.PATIENT_RAW p
  ON a.patient_id = p.patient_id
WHERE p.patient_id IS NULL;

SELECT * FROM DBT_DATA.DBT_SCHEMA.STG_PATIENT;

CREATE SCHEMA IF NOT EXISTS APPOLO_HC.STAGING;
CREATE SCHEMA IF NOT EXISTS APPOLO_HC.MARTS;

-- Grant to ACCOUNTADMIN (your current role)
GRANT USAGE ON DATABASE APPOLO_HC TO ROLE ACCOUNTADMIN;
GRANT CREATE SCHEMA ON DATABASE APPOLO_HC TO ROLE ACCOUNTADMIN;

GRANT ALL PRIVILEGES ON SCHEMA APPOLO_HC.STAGING TO ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON SCHEMA APPOLO_HC.MARTS TO ROLE ACCOUNTADMIN;

-- Allow reading raw data
GRANT USAGE ON SCHEMA APPOLO_HC.RAW_SCH TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA APPOLO_HC.RAW_SCH TO ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA APPOLO_HC.RAW_SCH TO ROLE ACCOUNTADMIN;

SHOW TABLES IN APPOLO_HC.MARTS;
SHOW VIEWS IN APPOLO_HC.STAGING;

select * from APPOLO_HC.STAGING.STG_APPOINTMENT;

select * from APPOLO_HC.MARTS.FACT_APPOINTMENT;
