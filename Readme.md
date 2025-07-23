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
The dataset from Kaggle is stored locally because of size restrictions. The dataset can be found here: https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data 

- to run the pipeline please download the dataset from the link above
- create the following folders in your project:
    - data/raw_data/ -> place here the downloaded csv file
    - data/monthly_raw_data/ -> the monthly split data will be saved here by the script split_data_to_monthly.py

## Author
ChrisSeHe