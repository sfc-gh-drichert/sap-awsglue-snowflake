CREATE or REPLACE DATABASE SAP_SNOW COMMENT = 'For SAP data';
USE DATABASE SAP_SNOW; 
CREATE OR REPLACE SCHEMA SAP_RAW; 
USE SCHEMA  SAP_RAW;

CREATE OR REPLACE STORAGE INTEGRATION sapsnowflakeintegration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::503561411659:role/sapsnowflakerole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sapsnowflakebucket/');
  

GRANT CREATE STAGE ON SCHEMA public TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION sapsnowflakeintegration TO ROLE ACCOUNTADMIN;

DESC INTEGRATION sapsnowflakeintegration;

CREATE OR REPLACE FILE FORMAT sap_file_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null');
  
  CREATE OR REPLACE STAGE sap_snowflake_stage
  STORAGE_INTEGRATION = sapsnowflakeintegration
  URL = 's3://sapsnowflakebucket/';

  DESC STAGE SAP_SNOWFLAKE_STAGE;

  LIST @SAP_SNOWFLAKE_STAGE

  


SHOW STAGES IN SCHEMA SAP_SNOW.SAP_RAW;

GRANT USAGE ON DATABASE SAP_SNOW TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA SAP_SNOW.SAP_RAW TO ROLE ACCOUNTADMIN;
GRANT USAGE ON STAGE SAP_SNOW.SAP_RAW.SAP_SNOWFLAKE_STAGE TO ROLE ACCOUNTADMIN;


CREATE OR REPLACE TABLE sap_sales_order_data
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION => '@sap_snowflake_stage/sap_sales_orders.csv',
        FILE_FORMAT => 'sap_file_format'
      )
    )
  );
select * from sap_sales_order_data;

SELECT get_ddl('TABLE', 'sap_sales_order_data');


CREATE OR REPLACE PIPE sap_sales_order_data_pipe
AUTO_INGEST = TRUE
AS
 COPY INTO sap_sales_order_data from @SAP_SNOWFLAKE_STAGE
  FILE_FORMAT = sap_file_format
  ON_ERROR=CONTINUE; 


  SELECT SYSTEM$PIPE_STATUS ('sap_sales_order_data_pipe');

  select * from sap_sales_order_data
