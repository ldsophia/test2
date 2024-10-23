import pandas as pd

# Sample data frames with multiple key columns (replace with your actual data)
# Assume 'key_cols' is the list of key columns
key_cols = ['key1', 'key2', 'key3', 'key4', 'key5', 'key6']

# Create sample DataFrames
# df_a = pd.DataFrame({
#     'key1': ['A', 'B', 'C', 'D'],
#     'key2': [1, 2, 3, 4],
#     'key3': ['X', 'Y', 'Z', 'X'],
#     'key4': ['K1', 'K2', 'K3', 'K4'],
#     'key5': [10, 20, 30, 40],
#     'key6': ['M', 'N', 'O', 'P'],
#     'a': ['A1', 'A2', 'A3', 'A4'],
#     'b': ['B1', 'B2', 'B3', 'B4'],
#     'c': ['C1', 'C2', 'C3', 'C4'],
#     'value_a': ['VA1', 'VA2', 'VA3', 'VA4']
# })

# df_b = pd.DataFrame({
#     'key1': ['C', 'D', 'E'],
#     'key2': [3, 4, 5],
#     'key3': ['Z', 'Y', 'X'],
#     'key4': ['K3', 'K2', 'K1'],
#     'key5': [30, 20, 50],
#     'key6': ['O', 'N', 'Q'],
#     'value_b': ['VB1', 'VB2', 'VB3']
# })

# Step 1: Create a composite key in both DataFrames
df_a['composite_key'] = df_a[key_cols].astype(str).agg('_'.join, axis=1)
df_b['composite_key'] = df_b[key_cols].astype(str).agg('_'.join, axis=1)

# Step 2: Perform the initial left join on the composite key
result_df = df_a.merge(df_b, how='left', on='composite_key', suffixes=('_a', '_b'))

# Step 3: Identify unmatched records in df_b after the initial join
df_b_matched = df_b.merge(df_a[['composite_key']], on='composite_key', how='left', indicator=True)
df_b_unmatched = df_b_matched[df_b_matched['_merge'] == 'left_only'].drop(columns=['_merge'])

# Step 4: For unmatched df_b records, remove one key column (e.g., 'key6') from the composite key
# Re-create composite keys without 'key6'
reduced_key_cols = key_cols[:-1]  # Remove 'key6' from the list
df_a['reduced_key'] = df_a[reduced_key_cols].astype(str).agg('_'.join, axis=1)
df_b_unmatched['reduced_key'] = df_b_unmatched[reduced_key_cols].astype(str).agg('_'.join, axis=1)

# Step 5: Merge unmatched df_b records with df_a on 'reduced_key' to get 'a', 'b', 'c'
df_a_subset = df_a[reduced_key_cols + ['a', 'b', 'c', 'reduced_key']].drop_duplicates(subset='reduced_key')
df_b_unmatched = df_b_unmatched.merge(df_a_subset, on='reduced_key', how='left')

# Step 6: Output df_a's all columns and 'a', 'b', 'c' for unmatched records
df_a_unmatched = df_a[df_a['reduced_key'].isin(df_b_unmatched['reduced_key'])].drop_duplicates(subset='reduced_key')

# Display the results
print("Result after initial left join on composite key:")
print(result_df)

print("\nUnmatched records in df_b after initial join:")
print(df_b_unmatched)

print("\nCorresponding df_a records for unmatched df_b records:")
print(df_a_unmatched)
