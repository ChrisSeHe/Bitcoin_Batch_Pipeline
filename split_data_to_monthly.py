import pandas as pd
import os

# Load dataset
df = pd.read_csv("data/raw_data/btcusd_1-min_data.csv")

# Print the first rows to check for correct parsing
print(df.head())

# Convert UNIX timestamp to datetime
df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')

# Create new column 'Month'
df['Month'] = df['Timestamp'].dt.to_period('M')

# Store monthly csv files in monthly_raw_data/ folder
output_dir = "data/monthly_raw_data"
os.makedirs(output_dir, exist_ok=True)

# Split and save file per month
for period, group in df.groupby('Month'):
    filename = f"{output_dir}/BTC_{period}.csv"
    group.drop(columns='Month').to_csv(filename, index=False)
    print(f"Saved: {filename}")