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

---- SOME CLEANUP

-- fill in the business unit BEGRU so that data is not empty

USE DATABASE SAP_RAW_AWS;
USE SCHEMA FI;

/*  didn't bring across the field, so can't cleanup

UPDATE "CUSTOMER"
SET BEGRU = 
    CASE MOD(ABS(RANDOM()), 5) 
        WHEN 0 THEN 'TECH'  
        WHEN 1 THEN 'FINC'  
        WHEN 2 THEN 'HLTH'  
        WHEN 3 THEN 'RETL'  
        WHEN 4 THEN 'ENRG'  
    END;
*/
--the following doesn't work either because of missing fields

select c.kunnr from customer c inner join fi_ar_4 f on c.kunnr=f.kunnr;

INSERT INTO CUSTOMER (
    MANDT, KUNNR, ADRNR, ANRED, AUFSD, BAHNE, BAHNS, BBBNR, BBSNR, BEGRU, BRSCH, BUBKZ, DATLT, ERDAT, ERNAM, EXABL, 
    FAKSD, FISKN, KNAZK, KNRZA, KONZS, KTOKD, KUKLA, LAND1, LIFNR, LIFSD, LOCCO, LOEVM, NAME1, NAME2, NAME3, NAME4, 
    NIELS, ORT01, ORT02, PFACH, PSTL2, PSTLZ, REGIO, COUNC, CITYC, RPMKR, SORTL, SPERR, SPRAS, STCD1, STCD2, STKZA, 
    STKZU, STRAS, TELBX, TELF1, TELF2, TELFX, TELTX, TELX1, LZONE, XCPDK, XZEMP, VBUND, STCEG, DEAR1, DEAR2, DEAR3, 
    DEAR4, DEAR5, DEAR6, GFORM, BRAN1, BRAN2, BRAN3, BRAN4, BRAN5, EKONT, UMSAT, UMJAH, UWAER, JMZAH, JMJAH, KATR1, 
    KATR2, KATR3, KATR4, KATR5, KATR6, KATR7, KATR8, KATR9, KATR10, STKZN, UMSA1, TXJCD, MCOD1, MCOD2, MCOD3, PERIV, 
    ABRVW, INSPBYDEBI, INSPATDEBI, KTOCD, PFORT, WERKS, DTAMS, DTAWS, DUEFL, HZUOR, SPERZ, ETIKG, CIVVE, MILVE, KDKG1, 
    KDKG2, KDKG3, KDKG4, KDKG5, XKNZA, FITYP, STCDT, STCD3, STCD4, XICMS, XXIPI, XSUBT, CFOPC, TXLW1, TXLW2, CCC01, 
    CCC02, CCC03, CCC04, CASSD, KNURL
)
SELECT 
   distinct 800 AS MANDT,
     A.kunnr AS KUNNR,	
    0000006658 AS ADRNR,
    NULL AS ANRED,
    NULL AS AUFSD,
    NULL AS BAHNE,
    NULL AS BAHNS,
    0000000 AS BBBNR,
    00000 AS BBSNR,	
    'HLTH' AS BEGRU, 
    NULL AS BRSCH,
    0 AS BUBKZ,
    NULL AS DATLT,
    '1995-03-23' AS ERDAT,
    NULL AS ERNAM,
    NULL AS EXABL,
    NULL AS FAKSD,
    NULL AS FISKN,
    NULL AS KNAZK,
    NULL AS KNRZA,
    NULL AS KONZS,
    0006 AS KTOKD,
    NULL AS KUKLA,
    'DE' AS LAND1,
    NULL AS LIFNR,
    NULL AS LIFSD,
    NULL AS LOCCO,
    NULL AS LOEVM,
    'Wett' AS NAME1,
    NULL AS NAME2,
    NULL AS NAME3,
    NULL AS NAME4,
    NULL AS NIELS,
    'Walldorf' AS ORT01,
    NULL AS ORT02,
    NULL AS PFACH,
    NULL AS PSTL2,
    69190 AS PSTLZ,
    08 AS REGIO,
    NULL AS COUNC,
    NULL AS CITYC,
    NULL AS RPMKR,
    'WETT' AS SORTL,
    NULL AS SPERR,
    'D' AS SPRAS,
    NULL AS STCD1,
    NULL AS STCD2,
    NULL AS STKZA,
    NULL AS STKZU,
    'Astorstrasse34' AS STRAS,
    NULL AS TELBX,
    NULL AS TELF1,
    NULL AS TELF2,
    NULL AS TELFX,
    NULL AS TELTX,
    NULL AS TELX1,
    NULL AS LZONE,
    NULL AS XCPDK,
    NULL AS XZEMP,
    NULL AS VBUND,
    NULL AS STCEG,
    'X' AS DEAR1,
    NULL AS DEAR2,
    NULL AS DEAR3,
    NULL AS DEAR4,
    NULL AS DEAR5,
    NULL AS DEAR6,
    NULL AS GFORM,
    NULL AS BRAN1,
    NULL AS BRAN2,
    NULL AS BRAN3,
    NULL AS BRAN4,
    NULL AS BRAN5,
    NULL AS EKONT,
    0 AS UMSAT,
    0000 AS UMJAH,
    NULL AS UWAER,
    000000 AS JMZAH,
    0000 AS JMJAH,
    NULL AS KATR1,
    NULL AS KATR2,
    NULL AS KATR3,
    NULL AS KATR4,
    NULL AS KATR5,
    NULL AS KATR6,
    NULL AS KATR7,
    NULL AS KATR8,
    NULL AS KATR9,
    NULL AS KATR10,
    NULL AS STKZN,
    0 AS UMSA1,
    NULL AS TXJCD,
    'WETT' AS MCOD1,
    NULL AS MCOD2,
    'WALLDORF' AS MCOD3,
    NULL AS PERIV,
    NULL AS ABRVW,
    NULL AS INSPBYDEBI,
    NULL AS INSPATDEBI,
    NULL AS KTOCD,
    NULL AS PFORT,
    NULL AS WERKS,
    NULL AS DTAMS,
    NULL AS DTAWS,
    'X' AS DUEFL,
    00 AS HZUOR,																			
    NULL AS SPERZ,
    NULL AS ETIKG,
    NULL AS CIVVE,
    NULL AS MILVE,
    NULL AS KDKG1,
    NULL AS KDKG2,
    NULL AS KDKG3,
    NULL AS KDKG4,
    NULL AS KDKG5,
    NULL AS XKNZA,
    NULL AS FITYP,
    NULL AS STCDT,
    NULL AS STCD3,
    NULL AS STCD4,
    NULL AS XICMS,
    NULL AS XXIPI,
    NULL AS XSUBT,
    NULL AS CFOPC,
    NULL AS TXLW1,
    NULL AS TXLW2,
    NULL AS CCC01,
    NULL AS CCC02,
    NULL AS CCC03,
    NULL AS CCC04,
    NULL AS CASSD,
    NULL AS KNURL
FROM FI_AR_4 A
LEFT JOIN CUSTOMER B ON A.kunnr = B.kunnr
WHERE B.kunnr IS NULL;
 