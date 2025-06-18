/* SETUP STORAGE & STAGE
Author: David Richert
Date: 26 May 2025
Documentation: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
 
 
 
 Use this code to set up the roles, S3 to Snowflake integration, database, schemas, table definitions, 
 and snowpipe pipelines to ingest data from AWS S3 buckets.
 
 Please run the AWS Glue job before executing this code. This will enable Snowflake to build the tables from the parquet files.

 */
USE ROLE ACCOUNTADMIN;
SET saps3targeturl = 's3://sap-s3-raw/s4/fi/ar/'; --this is the directory where you will land the files from AWS glue (minus the name of the extractor)

--Use the following to set up and validate integration between Snowflake and S3 buckets.

/*
The following step is specific to your deployment with AWS S3. It is important so the SNS protocol can alert Snowflake that new files have landed
in the S3 bucket, so Snowflake can pick them up automatically. 
Read the following documentation to set this up correctly.
https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration

 */

CREATE OR REPLACE STORAGE INTEGRATION SAP_INTEGRATION 
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::631484165566:role/sap_snowflake_s3_access_role' --obfuscate for quickstart
  STORAGE_AWS_EXTERNAL_ID = 'EU_DEMO47_SFCRole=3571_xQgnqahg8fymSujM2bXWqAWHwOM=' --so you don't have to set it each time your recreate the storage integration.
  STORAGE_ALLOWED_LOCATIONS = ('s3://sap-s3-raw/');

/*  Use the following commands to Validate the storage integration */
SHOW  STORAGE INTEGRATIONS;
DESC INTEGRATION SAP_INTEGRATION;
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION ('SAP_INTEGRATION', 's3://sap-s3-raw/', 'validate_all.txt', 'all');

/* Create the database and schema that will be used to land the raw data from SAP */
CREATE or REPLACE DATABASE SAP_SNOW COMMENT = 'For SAP data';
USE DATABASE SAP_SNOW; 
CREATE OR REPLACE SCHEMA SAP_RAW; 
USE SCHEMA  SAP_RAW;

-- Create file format so Snowflake knows that the SAP files are coming into the S3 bucket as parquet files.
CREATE OR REPLACE FILE FORMAT SAP_FILE TYPE = parquet COMMENT = 'Parquet file format for AWS Glue files landed in S3';

-- Create a Snowflake stage to point to the S3 bucket.
CREATE OR REPLACE STAGE SAP_STAGE
    URL = $SAPS3TARGETURL
    STORAGE_INTEGRATION = SAP_INTEGRATION
    FILE_FORMAT = SAP_FILE;

/*
If you have not already, run the job from AWS glue to land the files. Check out the cloud formation template to get this. 
Use the following command afterwards to verify the files landed in the bucket.
*/
SHOW stages; LIST @SAP_STAGE/fi_ar_4/; LIST @SAP_STAGE/customer; LIST @SAP_STAGE/material/;

/* 

Setting up the table definitions. 
Using infer_schema is an easy way to set up the table definitions. It sources from the parquet file definition to do this. 

 */

 CREATE OR REPLACE TABLE FI_AR_4
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
      LOCATION=>'@sap_stage/fi_ar_4/'
      , FILE_FORMAT=>'SAP_FILE'
        )
      ));

  CREATE OR REPLACE TABLE CUSTOMER
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
      LOCATION=>'@sap_stage/customer/'
      , FILE_FORMAT=>'SAP_FILE'
        )
      ));

CREATE OR REPLACE TABLE MATERIAL
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
      LOCATION=>'@sap_stage/material/'
      , FILE_FORMAT=>'SAP_FILE'
        )
      ));

/*Check out the table definitions */
SELECT get_ddl('TABLE', 'Z_FI_AR_4'); SELECT get_ddl('TABLE', 'CUSTOMER'); SELECT get_ddl('TABLE', 'MATERIAL');

/* 

Now that we have built the table definition automatically, let's delete the files from the S3 bucket, make sure our raw tables are empty, and then create the pipes to monitor the bucket.
Once the pipes are running, we will run the AWS Glue job again to populate the buckets, snowpipe will detect new files because of the AWS SNS integration, and will automatically load the data into Snowflake.

*/
REMOVE @sap_stage/fi_ar_4/; REMOVE @sap_stage/customer/; REMOVE @sap_stage/material/;
--truncate tables
TRUNCATE TABLE FI_AR_4; TRUNCATE TABLE CUSTOMER; TRUNCATE TABLE MATERIAL;

/* 
Ok, now let's create the pipes. They will start automatically.
 */
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

/* Check that the pipes were created and are running */
SHOW pipes; 
SELECT SYSTEM$PIPE_STATUS ('FI_AR_4_PIPE'); SELECT SYSTEM$PIPE_STATUS ('CUSTOMER_PIPE'); SELECT SYSTEM$PIPE_STATUS ('MATERIAL_PIPE');
  --ALTER PIPE Z_BW_ODATA_0FI_AR_4_1_SRV_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.
  
/*

Now, re-run  the AWS glue job from the AWS console! The pipes need to be in a running state.

*/


/* Once the AWS Glue job has completed you can check if the files landed in the S3 bucket by checking the stages */
LIST @SAP_STAGE/fi_ar_4/; LIST @SAP_STAGE/customer/; LIST @SAP_STAGE/material/;

/* After 1-2 minutes after the AWS job has run, check that the data has landed into the Snowflake tables */
SELECT * FROM FI_AR_4; SELECT * FROM CUSTOMER; SELECT *  FROM MATERIAL; --2701

/*  

Once the data has loaded into the tables, you can pause the pipes to reduce costs. Pause them by uncommenting the following lines.

*/

--ALTER PIPE FI_AR_4_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.
--ALTER PIPE CUSTOMER_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.
--ALTER PIPE MATERIAL_PIPE SET PIPE_EXECUTION_PAUSED = TRUE; --make sure to turn off pipe so you don't pay.

/* Congrats! You've now loaded the SAP data into the raw zone. Now time to build out the harmonization and reporting layer. Go to the Python worksheet */



