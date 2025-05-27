/* SETUP STORAGE & STAGE
Author: David Richert
Date: 26 May 2025
Documentation: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
 */
USE ROLE ACCOUNTADMIN;
SET sapbase = 'SAP_RAW_AWS';
SET sapschema = $SAPBASE||'.FI';
SET sapextractor = 'Z_BW_ODATA_0FI_AR_4_1_SRV'; --use for pipe and table name
SET saprole = 'SAPROLE';  --assign grants to create and own the rest of the objects
SET sapintegration = $SAPBASE||'_INTEGRATION';
SET sapfile = $SAPBASE||'_PARQUET';
SET sapstage = $SAPBASE||'_STAGE';
SET sappipe = $SAPEXTRACTOR||'_PIPE';
SET saps3targeturl = 's3://sap-s3-raw/s4/fi/ar/';


--CREATE sap role and assign grants to create and own the rest of the objects
CREATE OR REPLACE ROLE IDENTIFIER($saprole);
--GRANTS
GRANT CREATE DATABASE ON ACCOUNT TO ROLE IDENTIFIER($saprole);
--Integration
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE IDENTIFIER($saprole);
--ASSIGN ROLE TO an existing role for your user
GRANT ROLE IDENTIFIER($saprole) to ROLE accountadmin;
USE ROLE IDENTIFIER($saprole);

--https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
CREATE OR REPLACE STORAGE INTEGRATION IDENTIFIER($sapintegration)
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::631484165566:role/sap_snowflake_s3_access_role' --obfuscate for quickstart
  STORAGE_AWS_EXTERNAL_ID = 'EU_DEMO47_SFCRole=3571_xQgnqahg8fymSujM2bXWqAWHwOM='
  STORAGE_ALLOWED_LOCATIONS = ('s3://sap-s3-raw/');
SHOW  STORAGE INTEGRATIONS;
DESC INTEGRATION IDENTIFIER($sapintegration);
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION ($SAPINTEGRATION, 's3://sap-s3-raw/', 'validate_all.txt', 'all');
CREATE or REPLACE DATABASE IDENTIFIER($sapbase) COMMENT = 'For S4 raw data';
USE DATABASE IDENTIFIER($sapbase);
CREATE OR REPLACE SCHEMA IDENTIFIER($sapschema);
USE SCHEMA IDENTIFIER($sapschema);
--Create file format
CREATE OR REPLACE FILE FORMAT IDENTIFIER($sapfile) TYPE = parquet COMMENT = 'Parquet file format for AWS Glue files landed in S3';
--create stage
CREATE OR REPLACE STAGE IDENTIFIER($SAPSTAGE)
    URL = $SAPS3TARGETURL
    STORAGE_INTEGRATION = $SAPINTEGRATION
    FILE_FORMAT = $SAPFILE;

  --Test integration
  --run the job from AWS glue to get files to work with
 SHOW stages;
 LIST @SAP_RAW_AWS_STAGE/Z_BW_ODATA_0FI_AR_4_1_SRV/;
SELECT t.$1 from @sap_raw_aws_stage/Z_BW_ODATA_0FI_AR_4_1_SRV/run-1748278330761-part-block-0-r-00000-snappy.parquet t;
--Create DDLs 

  CREATE OR REPLACE TABLE identifier($sapextractor)
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
      LOCATION=>'@sap_raw_aws_stage/Z_BW_ODATA_0FI_AR_4_1_SRV/'
      , FILE_FORMAT=>'SAP_RAW_AWS_PARQUET'
        )
      ));

SELECT get_ddl('TABLE', $sapextractor);

--now delete the files from S3

CREATE OR REPLACE PIPE Z_BW_ODATA_0FI_AR_4_1_SRV_PIPE
AUTO_INGEST = TRUE
AS
 COPY INTO Z_BW_ODATA_0FI_AR_4_1_SRV from @SAP_RAW_AWS_STAGE/Z_BW_ODATA_0FI_AR_4_1_SRV/
  FILE_FORMAT = SAP_RAW_AWS_PARQUET
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  ON_ERROR=CONTINUE; 

SHOW pipes; -- get the ARN and configure the SNS in the S3 bucket
/*arn:aws:sqs:us-west-2:227202484309:sf-snowpipe-AIDATJZSSZBKXUQTZVL4W-j9b6KZYM9aqg3IToJ1ngqQ */
  SELECT SYSTEM$PIPE_STATUS ('Z_BW_ODATA_0FI_AR_4_1_SRV_PIPE');
  --ALTER PIPE Z_BW_ODATA_0FI_AR_4_1_SRV_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.
  
  --run the job from AWS Glue again
  SELECT * FROM Z_BW_ODATA_0FI_AR_4_1_SRV;

  ALTER PIPE Z_BW_ODATA_0FI_AR_4_1_SRV_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.
