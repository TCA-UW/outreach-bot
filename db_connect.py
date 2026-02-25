import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

def get_client():
    return supabase

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass