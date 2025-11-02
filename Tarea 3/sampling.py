import pandas as pd

# --- Configuration ---
# Path to your large CSV file
input_csv_path = 'PS_20174392719_1491204439457_log.csv'

# Desired number of records for your subsample
sample_size = 100000 

# Path for the new, smaller CSV file
output_csv_path = f'subsample_{sample_size}_records.csv'

# --- Script ---
print(f"Reading the large file: {input_csv_path}...")
# Read the entire CSV into a DataFrame
df = pd.read_csv(input_csv_path)

print(f"Original dataset has {len(df)} records.")

# Take a random sample of `sample_size` records.
# `random_state` is used for reproducibility, so you get the same sample every time.
print(f"Creating a random subsample of {sample_size} records...")
df_sample = df.sample(n=sample_size, random_state=42)

# Save the subsample to a new CSV file
df_sample.to_csv(output_csv_path, index=False)

print(f"Subsample successfully saved to: {output_csv_path}")
