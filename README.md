# AWSDataLakeServerless
AWS End to End Serverless Solution from RDBMS to Data Lake(s3) using Daily Batch ETL processes (Glue->S3)

Source Data = RDBMS
Extract - Glue (Daily Ingestion) + Glue Connection
Look up Table/ Tables Metadata - Dynamodb
Transform - Glue
Load - Glue

Governance
Logging - AMazon CloudWatch
Notification - Amazon SNS
Metadata - AWS Glue Data Catalog

Consume Layer
Database - Data Lake (Amazon S3)
Data Visualization - AWS QuickSight
