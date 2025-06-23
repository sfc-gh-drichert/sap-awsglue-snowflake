# SAP to Snowflake with AWS Glue
Use the following templates and code to build a demo environment where you can create dataflows from SAP S/4HANA fully activated appliance to your own Snowflake account. Here is a quick overview of the main actions:

**Prepare (SAP S/4 HANA)**

Prepare data sources (CDS views, tables, BW extractors) for initial and delta consumption by the ODP ODATA API v2. 

**Extract (AWS Glue)**

Use the AWS glue SAP odata extractor to: 

    a. Connect to SAP ODP API v2 as a source and extract data from BW extractors, CDS views, or expose any arbitrary table.
    b. Run the AWS Glue job to extract the data into an S3 bucket, split into folders. 
**Create the RAW layer (Snowflake SQL)**

Use the Snowflake SQL worksheet (file setup.sql) to:

    a. Activate a secure integration between AWS and Snowflake. 
    b. Generate table definitions automatically through metadata. 
    c. Pipe data from the S3 bucket into Snowflake automatically.
**Create Bronze & Gold layers (Snowflake Notebook)**

Use a Snowflake notebook (SAP_PREP_GOLD.ipynb) to automatically:

    a. Build business semantics.
    b. Build a reporting mart.
    c. Optimze query performance.
**Automate data flow between layers (Snowflake SQL)**

    Take the code from above to automate transformation orchestration.

## Prerequisites
* An SAP S/4HANA demo & test account
* An AWS account
* A Snowflake account

### SAP S/4HANA Test & Demo Account

There are two options to activate an S/4 HANA demo system, either through:
* [SAP Cloud Application Library](cal.sap.com) (1-2 hours, use for 30 days), or use the
* AWS Launch wizard ([Create deployment - SAP NetWeaver on SAP HANA system single instance deployment](https://github.com/awslabs/aws-sap-automation/tree/main/s4h_faa) 4-5 hours, use indefinitely). To order the appliance for download, check [SAP Note 2041140](https://me.sap.com/notes/2041140).

You also need to obtain the SAP Test and Demo User, starter package (TD_7016852) from the [SAP partner pricing app](https://partnersappartnerpricingapp.cfapps.eu10.hana.ondemand.com/index.html#/PlistDataCollection/US#PARPL#TD_7016852/false). If you do not install the licenses, either the CAL or on-premise system, your system will stop running after 30-90 days.

### An AWS Account
1. Create the above S/4HANA system (S4H) into your AWS account.
2. Create an S3 bucket, name it *sap-s3-raw*.
3. Connect AWS Glue to S4H. If you need this done securely check out this [blog](https://aws.amazon.com/blogs/awsforsap/share-sap-odata-services-securely-through-aws-privatelink-and-the-amazon-appflow-sap-connector/), and adapt it to AWS glue, not AppFlow. In the step, **Allow principals to access VPC Endpoint Services**, add the principal *glue.amazonaws.com* and ***not*** *appflow.amazonaws.com*.

### A Snowflake Account

Go to [Snowflake.com](https://www.snowflake.com/en/), select *Start for Free* and follow the steps to create your account. 

## Prepare (SAP S/4 HANA)
In SAP GUI, use the following three methods to activate the extraction of data through the ODP ODATA api:
* [BW extractor for Accounts Receivables](docs/BW_ODATA.md) (0fi_ar_4)
* [View on Table for Customers](docs/Table_ODATA.md) (kna1)
* [CDS View for Material](docs/CDS_ODATA.md) (I_Material)

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