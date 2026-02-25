# api.py
from dotenv import load_dotenv
load_dotenv()  
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import os
from fastapi.responses import FileResponse




from db_connect import supabase
from email_generation import anthropic_generate_for_company
from email_send import smtp_send
from email_check import validate_email_comprehensive

app = FastAPI()
@app.get("/")
def serve_dashboard():
    return FileResponse("outreach_dashboard.html")

# Allow the HTML file to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve your dashboard HTML at /
app.mount("/static", StaticFiles(directory="."), name="static")


# ── Companies ──
@app.get("/api/companies")
def list_companies():
    result = supabase.table("companies")\
        .select("company_id, company_name, website, description")\
        .order("company_id").execute()
    return result.data or []


# ── Contacts ──
@app.get("/api/companies/{company_id}/contacts")
def list_contacts(company_id: int):
    result = supabase.table("contacts")\
        .select("contact_id, contact_name, contact_title, email_address")\
        .eq("company_id", company_id).execute()
    rows = result.data or []
    # Filter to valid emails only
    valid = []
    for r in rows:
        addr = (r.get("email_address") or "").strip()
        if addr:
            try:
                verdict, _ = validate_email_comprehensive(addr)
                if verdict == "valid":
                    valid.append(r)
            except Exception:
                valid.append(r)  # include if validator fails
    return valid


# Drafts endpoint — fetch Unsent emails
@app.get("/api/companies/{company_id}/drafts")
def list_drafts(company_id: int):
    result = supabase.table("emails")\
        .select("email_id, subject, body, status, sent_at")\
        .eq("company_id", company_id)\
        .in_("status", ["Unsent", "Emailed"])\
        .order("email_id").execute()
    return result.data or []


class DraftBody(BaseModel):
    subject: str
    body: str

@app.post("/api/companies/{company_id}/drafts")
def create_draft(company_id: int, draft: DraftBody):
    res = supabase.table("emails").insert({
        "company_id": company_id,
        "status": "draft",
        "subject": draft.subject,
        "body": draft.body
    }).execute()
    return res.data[0] if res.data else {}

@app.patch("/api/drafts/{email_id}")
def update_draft(email_id: int, draft: DraftBody):
    supabase.table("emails")\
        .update({"subject": draft.subject, "body": draft.body})\
        .eq("email_id", email_id).execute()
    return {"ok": True}


# ── Generate with Anthropic ──
@app.post("/api/companies/{company_id}/generate")
def generate_draft(company_id: int):
    try:
        result = supabase.table("companies")\
            .select("*").eq("company_id", company_id)\
            .execute()
        
        if not result.data:
            raise HTTPException(404, f"Company {company_id} not found")
        
        company = result.data[0]  # safer than .single()
        
        result = anthropic_generate_for_company(company)
        
        # Handle whatever shape your function returns
        if isinstance(result, tuple):
            subject, body = result
        elif isinstance(result, dict):
            subject = result.get("subject", "UW Technology Consulting - Discovery Meeting")
            body = result.get("body", "")
        else:
            subject = "UW Technology Consulting - Discovery Meeting"
            body = str(result)
        
        res = supabase.table("emails").insert({
            "company_id": company_id,
            "status": "Unsent",          # was "draft"
            "subject": subject,
            "body": body
        }).execute()
        return res.data[0] if res.data else {}
    
    except HTTPException:
        raise
    except Exception as e:
        # This will show the real error in the response
        raise HTTPException(500, detail=str(e))


# ── Send emails ──
class SendBody(BaseModel):
    email_id: int
    contact_ids: list[int]
    subject: str
    body: str

@app.post("/api/send")
def send_emails(payload: SendBody):
    result = supabase.table("contacts")\
        .select("contact_id, contact_name, contact_title, email_address")\
        .in_("contact_id", payload.contact_ids).execute()
    contacts = result.data or []

    errors = []
    sent = 0
    for c in contacts:
        addr = c["email_address"]
        try:
            # Use the same to_html() logic already in email_send.py
            from email_send import smtp_send, to_html
            html_body = payload.body if payload.body.lstrip().startswith("<") else to_html(payload.body)
            smtp_send(addr, payload.subject, payload.body, body_html=html_body)
            sent += 1
        except Exception as e:
            errors.append(f"{addr}: {str(e)}")

    if sent:
        from datetime import datetime, timezone
        supabase.table("emails").update({
            "status": "Emailed",     # was "Sent"
            "sent_at": datetime.now(timezone.utc).isoformat()
        }).eq("email_id", payload.email_id).execute()
    return {"sent": sent, "errors": errors}


# ── Scanner ──
class ScanBody(BaseModel):
    keywords: list[str]
    lat: float
    lng: float
    radius_mi: int

@app.post("/api/scan")
def run_scan(payload: ScanBody):
    import subprocess, sys
    env = os.environ.copy()
    env["KEYWORDS"] = ",".join(payload.keywords)
    env["LAT"] = str(payload.lat)
    env["LNG"] = str(payload.lng)
    env["RADIUS_MI"] = str(payload.radius_mi)
    proc = subprocess.run(
        [sys.executable, "get_places_and_emails.py"],
        env=env, capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise HTTPException(500, proc.stderr[-2000:])
    return {"output": proc.stdout[-4000:]}