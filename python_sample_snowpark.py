import pandas as pd

# Sample data frames for demonstration (replace these with your actual data)
# df_a = pd.DataFrame({
#     'key': [1, 2, 3, 4],
#     'sub_group': ['X', 'Y', 'Z', 'X'],
#     'a': ['A1', 'A2', 'A3', 'A4'],
#     'b': ['B1', 'B2', 'B3', 'B4'],
#     'c': ['C1', 'C2', 'C3', 'C4'],
#     'value_a': ['VA1', 'VA2', 'VA3', 'VA4']
# })

# df_b = pd.DataFrame({
#     'key': [3, 4, 5],
#     'sub_group': ['Z', 'Y', 'X'],
#     'value_b': ['VB1', 'VB2', 'VB3']
# })

# Step 1: Perform the initial left join between df_a and df_b on ['key', 'sub_group']
result_df = df_a.merge(df_b, how='left', on=['key', 'sub_group'])

# Step 2: Identify unmatched records in df_b after the initial join
df_b_unmatched = df_b.merge(df_a[['key', 'sub_group']], on=['key', 'sub_group'], how='left', indicator=True)
df_b_unmatched = df_b_unmatched[df_b_unmatched['_merge'] == 'left_only'].drop(columns=['_merge'])

# Step 3: For unmatched df_b records, remove 'sub_group' from join keys and match on 'key' only to get 'a', 'b', 'c' from df_a
# Since 'a', 'b', 'c' are the same for the same 'key', we can drop duplicates
df_a_unique = df_a[['key', 'a', 'b', 'c']].drop_duplicates(subset='key')

# Merge unmatched df_b records with df_a_unique on 'key'
df_b_unmatched = df_b_unmatched.merge(df_a_unique, on='key', how='left')

# Step 4: Output df_a's all columns and 'a', 'b', 'c' for unmatched records
# Get df_a records corresponding to unmatched df_b records (matched on 'key' only)
df_a_unmatched = df_a[df_a['key'].isin(df_b_unmatched['key'])].drop_duplicates(subset='key')

# Display the results
print("Result after initial left join (df_a left join df_b on ['key', 'sub_group']):")
print(result_df)

print("\nUnmatched records in df_b after initial join (on ['key', 'sub_group']):")
print(df_b_unmatched)

print("\nCorresponding df_a records for unmatched df_b records (matched on 'key' only):")
print(df_a_unmatched)
