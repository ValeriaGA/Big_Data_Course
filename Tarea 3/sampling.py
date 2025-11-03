import pandas as pd

# --- Configuration ---
# Path to your large CSV file
input_csv_path = 'PS_20174392719_1491204439457_log.csv'

# Desired number of records for your subsample
total_sample_size = 20000

# Path for the new, smaller CSV file
output_csv_path = f'subsample_balanced_{total_sample_size}_records.csv'

# --- Script ---
print(f"Reading the large file: {input_csv_path}...")
# Read the entire CSV into a DataFrame
df = pd.read_csv(input_csv_path)

print(f"Original dataset has {len(df):,} records.")

# Separate the DataFrame into fraudulent and non-fraudulent transactions
df_fraud = df[df['isFraud'] == 1]
df_not_fraud = df[df['isFraud'] == 0]

print(f"Found {len(df_fraud):,} fraudulent records and {len(df_not_fraud):,} non-fraudulent records.")

# Define the number of samples for each class
n_fraud = int(total_sample_size * 0.40)  # 40% fraudulent
n_not_fraud = int(total_sample_size * 0.60) # 60% non-fraudulent

print(f"Creating a balanced subsample of {total_sample_size:,} records...")
print(f"Sampling {n_fraud:,} fraudulent records (40%)")
print(f"Sampling {n_not_fraud:,} non-fraudulent records (60%)")

# Sample from each class
# `random_state` is used for reproducibility
sample_fraud = df_fraud.sample(n=n_fraud, random_state=42)
sample_not_fraud = df_not_fraud.sample(n=n_not_fraud, random_state=42)

# Combine the samples and shuffle them
df_balanced_sample = pd.concat([sample_fraud, sample_not_fraud]).sample(frac=1, random_state=42).reset_index(drop=True)

# Save the subsample to a new CSV file
df_balanced_sample.to_csv(output_csv_path, index=False)

print(f"Subsample successfully saved to: {output_csv_path}")
