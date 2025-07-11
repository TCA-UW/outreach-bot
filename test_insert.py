from db_connect import supabase

# result = supabase.table('companies').select('*').execute()

result = supabase.table('companies').insert({
    'company_name': 'Test Company',
    'industry': 'Tech',
    'source': 'manual'
}).execute()
print(result)