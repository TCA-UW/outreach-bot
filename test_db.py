from db_connect import supabase

res = supabase.table("companies").select("company_id, company_name").execute()
print(len(res.data))