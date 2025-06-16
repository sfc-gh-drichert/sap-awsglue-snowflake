/* SETUP STORAGE & STAGE
Author: David Richert
Date: 26 May 2025
Documentation: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
 */
USE ROLE ACCOUNTADMIN;
SET saps3targeturl = 's3://sap-s3-raw/s4/fi/ar/'; --this is the directory where you will land the files from AWS glue (minus the name of the extractor)
--CREATE sap role and assign grants to create and own the rest of the objects
CREATE OR REPLACE ROLE SAP_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE SAP_ROLE; --grant so the role can creat the database.
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SAP_ROLE; --grant to create the integration between Snowflake and AWS S3.
GRANT ROLE SAP_ROLE to ROLE accountadmin; --ASSIGN ROLE TO an existing role that your user has.
USE ROLE SAP_ROLE;

CREATE OR REPLACE STORAGE INTEGRATION SAP_INTEGRATION --more infor here https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::631484165566:role/sap_snowflake_s3_access_role' --obfuscate for quickstart
  STORAGE_AWS_EXTERNAL_ID = 'EU_DEMO47_SFCRole=3571_xQgnqahg8fymSujM2bXWqAWHwOM=' --so you don't have to set it each time your recreate the storage integration.
  STORAGE_ALLOWED_LOCATIONS = ('s3://sap-s3-raw/');
SHOW  STORAGE INTEGRATIONS;
DESC INTEGRATION SAP_INTEGRATION;
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION ('SAP_INTEGRATION', 's3://sap-s3-raw/', 'validate_all.txt', 'all');
CREATE or REPLACE DATABASE SAP_RAW_AWS COMMENT = 'For S4 raw data';
USE DATABASE SAP_RAW_AWS; --change to SAP_LLM_ANALYST2
CREATE OR REPLACE SCHEMA SAP_RAW_AWS.FI; --change to SAP_RAW
USE SCHEMA SAP_RAW_AWS.FI;
--Create file format
CREATE OR REPLACE FILE FORMAT SAP_FILE TYPE = parquet COMMENT = 'Parquet file format for AWS Glue files landed in S3';
--create stage
CREATE OR REPLACE STAGE SAP_STAGE
    URL = $SAPS3TARGETURL
    STORAGE_INTEGRATION = SAP_INTEGRATION
    FILE_FORMAT = SAP_FILE;

  --Test integration
  --run the job from AWS glue to get files to work with
 SHOW stages;
LIST @SAP_STAGE/fi_ar_4/; --copy one of the file names from the result.
LIST @SAP_STAGE/customer;
LIST @SAP_STAGE/material/;


--Create DDLs 

  CREATE OR REPLACE TABLE FI_AR_4 --change to "0fi_ar_4"
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
      LOCATION=>'@sap_stage/fi_ar_4/'
      , FILE_FORMAT=>'SAP_FILE'
        )
      ));

SELECT get_ddl('TABLE', 'Z_FI_AR_4');

  CREATE OR REPLACE TABLE CUSTOMER
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
      LOCATION=>'@sap_stage/customer/'
      , FILE_FORMAT=>'SAP_FILE'
        )
      ));

SELECT get_ddl('TABLE', 'CUSTOMER');

CREATE OR REPLACE TABLE MATERIAL
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
      LOCATION=>'@sap_stage/material/'
      , FILE_FORMAT=>'SAP_FILE'
        )
      ));

SELECT get_ddl('TABLE', 'MATERIAL');


--!!now delete the files from S3!!
REMOVE @sap_stage/fi_ar_4/;
REMOVE @sap_stage/customer/;
REMOVE @sap_stage/material/;
--truncate tables
TRUNCATE TABLE FI_AR_4;
TRUNCATE TABLE CUSTOMER;
TRUNCATE TABLE MATERIAL;

CREATE OR REPLACE PIPE FI_AR_4_PIPE
AUTO_INGEST = TRUE
AS
 COPY INTO FI_AR_4 from @SAP_STAGE/fi_ar_4/
  FILE_FORMAT = SAP_FILE
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  ON_ERROR=CONTINUE; 

CREATE OR REPLACE PIPE CUSTOMER_PIPE
AUTO_INGEST = TRUE
AS
 COPY INTO CUSTOMER from @SAP_STAGE/customer/
  FILE_FORMAT = SAP_FILE
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  ON_ERROR=CONTINUE; 

CREATE OR REPLACE PIPE MATERIAL_PIPE
AUTO_INGEST = TRUE
AS
 COPY INTO MATERIAL from @SAP_STAGE/material/
  FILE_FORMAT = SAP_FILE
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  ON_ERROR=CONTINUE; 

SHOW pipes; -- get the ARN and configure the SNS for the S3 bucket from the AWS console
/*arn:aws:sqs:us-west-2:227202484309:sf-snowpipe-AIDATJZSSZBKXUQTZVL4W-j9b6KZYM9aqg3IToJ1ngqQ */
SELECT SYSTEM$PIPE_STATUS ('FI_AR_4_PIPE');
SELECT SYSTEM$PIPE_STATUS ('CUSTOMER_PIPE');
SELECT SYSTEM$PIPE_STATUS ('MATERIAL_PIPE');
  --ALTER PIPE Z_BW_ODATA_0FI_AR_4_1_SRV_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.
  
  --RUN  THE AWS GLUE JOB NOW!è
  --after its executed, but before the SNS has kicked in  (can take minutes) check that the files have landed in the S3 bucket:
LIST @SAP_STAGE/fi_ar_4/; --copy one of the file names from the result.
LIST @SAP_STAGE/customer/;
LIST @SAP_STAGE/material/;
SELECT * FROM FI_AR_4; --5201 rows
SELECT * FROM CUSTOMER; --201
SELECT *  FROM MATERIAL; --2701

---TO PAUSE!!!!!!!!
--ALTER PIPE FI_AR_4_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.
--ALTER PIPE CUSTOMER_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.
--ALTER PIPE MATERIAL_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.

  /* Clean up -- Deletes database 
Author: David Richert
Date: 9 May 2025
*/

--Tear down
--DROP DATABASE SAP_RAW_AWS;
