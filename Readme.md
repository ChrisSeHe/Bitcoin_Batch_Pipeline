# Bitcoin Batch Pipeline

In this project a batch processing pipeline for the 1-minute Bitcoin price data is implemented. It can be used as backbone for a machine learning application. 

## Five Microservice Architecture
1. Ingestion of historic 1-minute BTC price data via CSV file
2. Storage in PostgreSQL / MinIO
3. Preprocessing with Pandas or Spark
4. Aggregation in weekly / quarterly summaries
5. Delivery via Flask API for ML access

## Status Update
Currently working on first Microservice

## Data
The dataset from Kaggle is stored locally 

## Author
ChrisSeHe