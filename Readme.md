# Bitcoin Batch Pipeline

In this project a batch processing pipeline for the 1-minute Bitcoin price data is implemented. It can be used as backbone for a machine learning application. 

## Five Microservice Architecture
1. **Ingestion**: Loads historical 1-minute BTC price data from local CSV files (downloaded from Kaggle, s. below) and inserts it into a PostgreSQL database.
2. **Storage**: Reads the data from PostgreSQL and stores the monthly CSV files in an object storage (MinIO).
3. **Preprocessing**: Loads files from MinIO, cleans the data using Pandas, performs basic feature engineering (calculating price *range* = high - low), and writes the results to PostgreSQL.
4. **Aggregation**: Summarize preprocessed data into weekly and quarterly aggregates.
5. **Delivery**: [Planned] Expose the final aggregated data via a Flask API for downstream ML consumption.

## Status Update
Currently working on microservice 5 

## Tech
- Language: Python
- Data Processing: Pandas
- Database: PostgreSQL
- Object Storage: MinIO
- Containerization: Docker Compose
- Feature Engineering: Range = High - Low

## Data
The dataset from Kaggle is stored locally because of size restrictions. The dataset can be found here: https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data 

## To Run Pipeline
- please download the dataset from the link above
- create the following folders in your project:
    - data/raw_data/ -> place here the downloaded csv file
    - data/monthly_raw_data/ -> the monthly split data will be saved here by the 'script split_data_to_monthly.py'
- building up all docker containers may take ~10 minutes with standard equipment

## Author
ChrisSeHe