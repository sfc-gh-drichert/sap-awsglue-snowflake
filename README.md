![Guidance](/docs/img/guidance.png)
# SAP to Snowflake with AWS Glue
Use the following templates and code to build a demo environment where you can create dataflows from SAP S/4HANA fully activated appliance and copy data to your own Snowflake database, model the data to transform the data into a data mart, and use dynamic tables to refresh it. 

On the *extraction* part I've given examples of how to access ECC extractors in S/4HANA (0FI_AR_4 is for accounts receivable), how to create custom views on tables (KNA1 is the famous Customer table), and how to access ABAP CDS views (here I chose I_MATERIAL). 

On the *transformation* semantics, I use large language modules (LLMs) to scan the tables and build up the business language automatically. It even joins the tables! 

After that, just another step to trigger the transforms--Snowflake dynamic tables are a declarative way to do this--write your SQL transform statement, what lag refresh you want, say 5 minutes, and Snowflake handles the change data capture for you! 

After this, it's just a quick step to start using *Snowflake Cortex*, or AWS AI tools!  

Here is a quick overview of the main steps to set up the demo environment:

**Use AWS Cloud Formation**

Use the following AWS Cloud formation [template](docs/s4_glue_iam_s3_2025_Jul_3.yaml) in region us-west-2 (Oregon) to deploy the necessary parts on the AWS side (S/4HANA fully activated appliance 2023 SPS00, AWS job, S3). Go to [Snowflake.com](https://snowflake.com) to activate a snowflake account with free credits.

**Prepare (SAP S/4 HANA)**

Prepare data sources (CDS views, tables, BW extractors) for initial and delta consumption by the ODP ODATA API v2. 

**Extract (AWS Glue)**

Use the AWS glue SAP odata extractor to: 
 * Connect to SAP ODP API v2 as a source and extract data from BW extractors, CDS views, or expose any arbitrary table.
 * Run the AWS Glue job to extract the data into an S3 bucket, split into folders. 
 * Connect to SAP ODP API v2 as a source and extract data from BW extractors, CDS views, or expose any arbitrary table.
 * Run the AWS Glue job to extract the data into an S3 bucket, split into folders. 

**Create the RAW layer (Snowflake SQL)**

Use the Snowflake SQL worksheet (file setup.sql) to:

 * Activate a secure integration between AWS and Snowflake. 
 * Generate table definitions automatically through metadata. 
 * Pipe data from the S3 bucket into Snowflake automatically.


**Create Bronze & Gold layers (Snowflake Notebook)**

Use a Snowflake notebook (SAP_PREP_GOLD.ipynb) to automatically:

 * Build business semantics.
 * Build a reporting mart.
 * Optimze query performance.

**Automate data flow between layers (Snowflake SQL)**

Take the code from above to automate transformation orchestration.
Take the code from above to automate transformation orchestration.

## Prerequisites

* An SAP S/4HANA demo & test account
* An AWS account
* A Snowflake account

### SAP S/4HANA Test & Demo Account

There are three options to activate an S/4 HANA demo system, either through:

* using the following AWS Cloud formation [template](docs/s4_glue_iam_s3_2025_Jul_3.yaml) in region us-west-2 (Oregon) to deploy a stack that includes the S/4HANA instance, the AWS Glue jobs, the IAM roles, and the S3 bucket.
* [SAP Cloud Application Library](cal.sap.com) (1-2 hours, use for 30 days), or use the
* AWS Launch wizard ([Create deployment - SAP NetWeaver on SAP HANA system single instance deployment](https://github.com/awslabs/aws-sap-automation/tree/main/s4h_faa) 4-5 hours, use indefinitely). To order the appliance for download, check [SAP Note 2041140](https://me.sap.com/notes/2041140).

You also need to obtain the SAP Test and Demo User, starter package (TD_7016852) from the [SAP partner pricing app](https://partnersappartnerpricingapp.cfapps.eu10.hana.ondemand.com/index.html#/PlistDataCollection/US#PARPL#TD_7016852/false). If you do not install the licenses, either the CAL or on-premise system, your system will stop running after 30-90 days.

### An AWS Account

If you did not use the Cloud formation template, you will need to do the following steps.
If you did not use the Cloud formation template, you will need to do the following steps.
1. Create the above S/4HANA system (S4H) into your AWS account.
2. Create an S3 bucket, name it *sap-s3-raw*.
3. Connect AWS Glue to S4H. If you need this done securely check out this [blog](https://aws.amazon.com/blogs/awsforsap/share-sap-odata-services-securely-through-aws-privatelink-and-the-amazon-appflow-sap-connector/), and adapt it to AWS glue, not AppFlow. In the step, **Allow principals to access VPC Endpoint Services**, add the principal *glue.amazonaws.com* and ***not*** *appflow.amazonaws.com*.

### A Snowflake Account

Go to [Snowflake.com](https://www.snowflake.com/en/), select *Start for Free* and follow the steps to create your account. 

## Prepare (SAP S/4 HANA)
In SAP GUI, use the following three methods to activate the extraction of data through the ODP ODATA api:
* [BW extractor for Accounts Receivables](docs/BW_ODATA_0FI_AR_4.pdf) (0FI_AR_4)
* [View on Table for Customers](docs/Table_ODATA_KNA1.pdf) (KNA1)
* [CDS View for Material](docs/CDS_ODATA_I_MATERIAL.pdf) (I_Material)

## Extract (AWS Glue)

To use AWS Glue with SAP S/4HANA (S4H)
1. Create a connector to S4H.
2. Create an ETL job
    a. Create a source using SAP ODATA.
    b. Create a target using S3.
    c. Run the job.
    d. Check the files landed in the S3 bucket.

  ##  Create the RAW layer (Snowflake SQL)
 1. Open an SQL worksheet in Snowflake.
 2. Copy and paste the following [code](docs/setup.sql) into it. 
 3. Review the links in the code on creating an integration between Snowflake and AWS S3 to 
obtain your *STORAGE_AWS_ROLE_ARN* and *STORAGE_AWS_EXTERNAL_ID*.
4. Run the code, line by line to set up the integrations and environment.

## Create Bronze & Gold layers (Snowflake Notebook)
Open the [SAP_PREP_GOLD](docs/SAP_PREP_GOLD.ipynb) notebook and execute each cell. You can verify the output of each one.