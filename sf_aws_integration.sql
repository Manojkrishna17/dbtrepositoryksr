
CREATE OR REPLACE STORAGE INTEGRATION STORAGE_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::628743726368:role/sf_s3_role',
  STORAGE_ALLOWED_LOCATIONS = ('s3://sf-project-manojkrishna/');

  DESCRIBE INTEGRATION STORAGE_S3_INTEGRATION;

  CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
  TYPE = CSV
    SKIP_HEADER = 1                          -- Skips header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'       -- Handles quoted values properly
    FIELD_DELIMITER = ','                    -- Comma-separated values
    TRIM_SPACE = TRUE                         -- Removes extra spaces around values
    COMPRESSION = 'AUTO';

CREATE OR REPLACE STAGE ext_stage_storage_int
  STORAGE_INTEGRATION = STORAGE_S3_INTEGRATION  -- Links to the storage integration
  URL = 's3://sf-project-manojkrishna/'  -- Specifies the S3 bucket location
  FILE_FORMAT = MY_CSV_FORMAT;    

list @ext_stage_storage_int;


CREATE OR REPLACE STORAGE INTEGRATION STORAGE_S3_INTEGRATION_multi
  TYPE = EXTERNAL_STAGE  
  STORAGE_PROVIDER = S3  
  ENABLED = TRUE  
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::628743726368:role/sf_multi_role'  
  STORAGE_ALLOWED_LOCATIONS = ('s3://sf-ksr-datasets/','s3://sf-ksr-snowpipe/');

  DESCRIBE INTEGRATION STORAGE_S3_INTEGRATION_multi;

  CREATE OR REPLACE STAGE ext_stage_storage_int_multi_s2
  STORAGE_INTEGRATION = STORAGE_S3_INTEGRATION_multi  -- Links to the storage integration
  URL = 's3://sf-ksr-snowpipe/'  -- Specifies the S3 bucket location
  FILE_FORMAT = MY_CSV_FORMAT;

  list @ext_stage_storage_int_multi_s2;

  SHOW INTEGRATIONS;
