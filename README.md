# End-to-End AWS Data Engineering Pipeline

## Overview
This project demonstrates how to build a scalable analytics pipeline using AWS.

## Architecture
https://github.com/akhil993/data-engineering-etl-project/tree/main/Architecture#:~:text=Pipeline_diagram.png

## Tech Stack
Python
AWS S3
AWS Athena
Power BI
Parquet
SQL

## Data Pipeline Flow
1. Raw CSV data ingested to S3 Bronze layer
2. Data cleaned and transformed into Silver layer
3. Aggregated datasets created in Gold layer
4. Athena used for SQL analytics
5. Power BI used for reporting

## Key Features
Incremental data loading
Partitioned datasets
Serverless analytics
Scalable architecture
