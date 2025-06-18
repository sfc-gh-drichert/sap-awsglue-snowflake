# SAP to AWS Glue to Snowflake flows, harmonization and reporting layer generation.

## Raw Layer
1. Use the AWS glue odata extractor to: 
    a. Connect to SAP ODP API v2 as a source and extract data from BW extractors, CDS views, or create your own view on a table .
    b. Run the job to extract the data into an S3 bucket, split into folders. 
2. Use the Snowflake SQL worksheet (file setup.sql) to:
    a. Create an SNS integration with S3 to detect files as they land in S3.
    b. Generate the table definitions automatically from the files, and create the tables.
    c. Create snowpipes to copy the data into Snowflake automatically.
## Bronze & Gold  Layers
3. Use the Snowflake Python worksheet (SAP_PREP_GOLD.ipynb) to:
    a. Use your choice of LLM to generate and build the harmonization (bronze) layer, while mainting the security perimeter (you data and metadata always stay in Snowflake).
    b. Use your choice of LLM to generate and build the reporting (gold) layers
    c. Optimze query performance by activating Cortex Search.
    d. Take the transformation code from above and put it in a declarative Dynamic Table to automate change data capture between layers.
