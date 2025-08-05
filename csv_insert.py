from db_connect import supabase
import pandas as pd

result = supabase.table('companies').select('*').execute()

df = pd.DataFrame(result.data).sortby('company_id')

# result = supabase.table('companies').insert({
#     'company_name': 'Test Company',
#     'industry': 'Tech',
#     'source': 'manual'
# }).execute()
# print(df)

df.to_csv("companies.csv", index=False)
