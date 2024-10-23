import pandas as pd

# Sample data frames with multiple key columns (replace with your actual data)
key_cols = ['key1', 'key2', 'key3', 'key4', 'key5', 'key6']

# Create sample DataFrames
# df_a = pd.DataFrame({...})
# df_b = pd.DataFrame({...})

# Step 1: Set MultiIndex on key columns
df_a.set_index(key_cols, inplace=True)
df_b.set_index(key_cols, inplace=True)

# Step 2: Perform the initial left join using the indices
result_df = df_a.merge(df_b, how='left', left_index=True, right_index=True, suffixes=('_a', '_b')).reset_index()

# Step 3: Identify unmatched records in df_b after the initial join
df_b_matched = df_b.merge(df_a, how='left', left_index=True, right_index=True, indicator=True)
df_b_unmatched = df_b_matched[df_b_matched['_merge'] == 'left_only'].drop(columns=['_merge']).reset_index()

# Step 4: For unmatched df_b records, remove one key column (e.g., 'key6') from the index
# Reset index and set a reduced MultiIndex without 'key6'
reduced_key_cols = key_cols[:-1]  # Remove 'key6' from the list

df_a.reset_index(inplace=True)
df_b_unmatched.reset_index(inplace=True)

df_a.set_index(reduced_key_cols, inplace=True)
df_b_unmatched.set_index(reduced_key_cols, inplace=True)

# Step 5: Merge unmatched df_b records with df_a on reduced MultiIndex to get 'a', 'b', 'c'
df_a_subset = df_a[['a', 'b', 'c']].drop_duplicates()
df_b_unmatched = df_b_unmatched.merge(df_a_subset, how='left', left_index=True, right_index=True).reset_index()

# Step 6: Output df_a's all columns and 'a', 'b', 'c' for unmatched records
df_a_unmatched = df_a[df_a.index.isin(df_b_unmatched.set_index(reduced_key_cols).index)].reset_index()

# Display the results
print("Result after initial left join using MultiIndex:")
print(result_df)

print("\nUnmatched records in df_b after initial join:")
print(df_b_unmatched)

print("\nCorresponding df_a records for unmatched df_b records:")
print(df_a_unmatched)
