"""
Send confirmed outreach emails from Supabase drafts via SMTP.

Flow:
  1) Load draft emails from `emails` (status='draft'), optionally filtered by ONLY_COMPANY_ID
  2) For each draft, find user profiles for the same company_id with non-empty email_address
  3) For each recipient: preview -> require human confirmation -> send via SMTP
  4) On first successful send for a draft, mark the draft email row as 'sent' with sent_at timestamp

Safety:
  - CONFIRM_EACH=true forces y/N prompt per recipient
  - SEND_LIMIT caps total sends per run
  - Basic email syntax & DNS check for recipient domain
"""

import os
import re
import sys
import ssl
import smtplib
import socket
from email.message import EmailMessage
from datetime import datetime, timezone
from typing import List, Dict, Tuple

from dotenv import load_dotenv
from supabase import create_client, Client
import dns.resolver
import dns.exception

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Missing SUPABASE_URL or SUPABASE_KEY")
    sys.exit(1)

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"

FROM_NAME = os.getenv("FROM_NAME", "Technology Consulting Association (TCA)")
FROM_EMAIL = os.getenv("FROM_EMAIL")
REPLY_TO  = os.getenv("REPLY_TO", FROM_EMAIL)

SEND_LIMIT = int(os.getenv("SEND_LIMIT", "0"))  # 0 = unlimited
CONFIRM_EACH = os.getenv("CONFIRM_EACH", "true").lower() == "true"
START_COMPANY_ID = int(os.getenv("START_COMPANY_ID", "1"))

if not SMTP_HOST or not SMTP_USER or not SMTP_PASS or not FROM_EMAIL:
    print("❌ Missing SMTP settings. Required: SMTP_HOST, SMTP_USER, SMTP_PASS, FROM_EMAIL")
    sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


EMAIL_RE = re.compile(r"^(?P<local>[^@\s]+)@(?P<domain>[^@\s]+\.[A-Za-z0-9\-\.]+)$")

def valid_email_syntax(addr: str) -> Tuple[bool, str]:
    if not addr: return False, "empty"
    m = EMAIL_RE.match(addr.strip())
    return (True, "ok") if m else (False, "bad-syntax")

def domain_can_receive(domain: str) -> bool:
    """Prefer MX; if none, accept A/AAAA as weaker fallback."""
    try:
        idna = domain.encode("idna").decode("ascii")
    except Exception:
        return False
    resolver = dns.resolver.Resolver(configure=True)
    resolver.lifetime = 3.0
    resolver.timeout = 2.0
    try:
        ans = resolver.resolve(idna, "MX")
        return len(ans) > 0
    except dns.exception.DNSException:
        # fallback A/AAAA
        for rr in ("A", "AAAA"):
            try:
                ans = resolver.resolve(idna, rr)
                if len(ans) > 0:
                    return True
            except dns.exception.DNSException:
                continue
    return False

def to_plain_text(s: str) -> str:
    """Ensure CRLF and reasonable formatting for plain text."""
    return s.replace("\r\n", "\n").replace("\r", "\n")

# def to_html(s: str) -> str:
#     """Very light plaintext -> HTML conversion: paragraphs + <br> line breaks."""
#     import html
#     esc = html.escape(s)
#     newline = '\n'
#     br_tag = '<br>'
#     parts = [f"<p>{p.replace(newline, br_tag)}</p>" for p in esc.split("\n\n")]
#     return "\n".join(parts)


def to_html(s: str) -> str:
    """
    Render body as HTML.
    - If it already looks like HTML (starts with "<"), pass through.
    - Otherwise treat as Markdown and convert to HTML with sane lists + newlines.
    """
    if not s:
        return ""
    src = s.strip()
    if src.lstrip().startswith("<"):
        return src  # already HTML

    import markdown as md
    # 'extra' adds tables, code blocks; 'sane_lists' gives proper <ul>/<ol>;
    # 'nl2br' respects single newlines; 'smarty' nice quotes (optional)
    return md.markdown(
        src,
        extensions=["extra", "sane_lists", "nl2br", "smarty"]
    )

def ask_confirm(prompt: str) -> bool:
    if not CONFIRM_EACH:
        return True
    try:
        ans = input(f"{prompt} [y/N]: ").strip().lower()
        return ans in ("y", "yes")
    except EOFError:
        return False

def personalize_content(content: str, recipient: Dict, company: Dict) -> str:
    """Replace placeholders in email content with actual values."""
    if not content:
        return content
    
    # Available placeholders
    replacements = {
        "{name}": recipient.get("contact_name", "there"),
        "{first_name}": recipient.get("contact_name", "").split()[0] if recipient.get("contact_name") else "there",
        "{company}": company.get("company_name", "your company"),
        "{company_name}": company.get("company_name", "your company"),
        "{title}": recipient.get("contact_title", ""),
        "{contact_title}": recipient.get("contact_title", ""),
    }
    
    personalized = content
    for placeholder, value in replacements.items():
        personalized = personalized.replace(placeholder, str(value))
    
    return personalized

def fetch_company_ids_with_drafts(start_company_id: int) -> list[int]:
    """
    Return sorted distinct company_id values that have draft emails,
    starting from start_company_id (inclusive).
    """
    # Pull all drafts >= start_company_id and dedupe in Python
    rows = (sb.table("emails")
              .select("company_id,status")
              .eq("status", "draft")
              .gte("company_id", start_company_id)
              .order("company_id", desc=False)
              .execute()
            ).data or []
    seen = set()
    ordered = []
    for r in rows:
        cid = r.get("company_id")
        if cid is None:
            continue
        if cid not in seen:
            seen.add(cid)
            ordered.append(cid)
    return ordered

# supabase
def fetch_drafts(company_id: int) -> List[Dict]:
    """
    Fetch all draft email rows for a specific company_id.
    """
    q = (sb.table("emails")
           .select("email_id, company_id, subject, body, status, sent_at, outreach_person")
           .eq("status", "draft")
           .eq("company_id", company_id)
           .order("email_id", desc=False))
    return q.execute().data or []

def fetch_recipients_for_company(company_id: int) -> List[Dict]:
    """Fetch contacts (recipients) for a given company."""
    rows = (sb.table("contacts")
              .select("contact_id, contact_name, email_address, contact_title, contact_linkedin_url")
              .eq("company_id", company_id)
              .order("contact_id", desc=False)
            ).execute().data or []
    # Keep only non-empty email addresses
    return [r for r in rows if r.get("email_address")]

def fetch_company_info(company_id: int) -> Dict:
    """Fetch company information."""
    try:
        company = (sb.table("companies")
                     .select("company_id, company_name, description, website")
                     .eq("company_id", company_id)
                     .single()
                   ).execute().data
        return company or {}
    except Exception as e:
        print(f"⚠️  Could not fetch company info for company_id={company_id}: {e}")
        return {}

def mark_email_sent(email_id: int, sent_count: int):
    now = datetime.now(timezone.utc).isoformat()
    sb.table("emails").update({"status": "Sent", "sent_at": now}).eq("email_id", email_id).execute()

#smtp
def smtp_send(to_addr: str, subject: str, body_text: str, body_html: str | None = None):
    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_addr
    msg["Subject"] = subject
    if REPLY_TO:
        msg["Reply-To"] = REPLY_TO

    if body_html:
        # multipart/alternative: text then html
        msg.set_content(to_plain_text(body_text))
        msg.add_alternative(body_html, subtype="html")
        # msg.set_content(to_plain_text(personalized_body), subtype="plain", charset="utf-8")
        # msg.add_alternative(html_body, subtype="html", charset="utf-8")
    else:
        msg.set_content(to_plain_text(body_text))

    # Connect & send
    if SMTP_USE_TLS:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    else:
        # SSL-on-connect (e.g., port 465) or plain
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)

# ----- Main flow -----

def main():
    print("📨 Email Sender starting...")
    print(f"   FROM: {FROM_NAME} <{FROM_EMAIL}>   SMTP: {SMTP_HOST}:{SMTP_PORT} TLS={SMTP_USE_TLS}")
    print(f"   CONFIRM_EACH={CONFIRM_EACH}  COMPANY_LIMIT={SEND_LIMIT or '∞'}  START_COMPANY_ID={START_COMPANY_ID}")

    # drafts = fetch_drafts()
        # Determine which companies to process
    company_ids = fetch_company_ids_with_drafts(START_COMPANY_ID)
    if not company_ids:
        print("✅ No drafts to send (no companies with status='draft' at or after START_COMPANY_ID).")
        return

    # Limit to SEND_LIMIT companies (SEND_LIMIT now means 'company count')
    if SEND_LIMIT and len(company_ids) > SEND_LIMIT:
        company_ids = company_ids[:SEND_LIMIT]
    print(f"🧭 Companies to process: {company_ids}")

    total_sends = 0

    for company_id in company_ids:
        drafts = fetch_drafts(company_id)
        if not drafts:
            # Could happen if drafts were sent/changed between queries
            print(f"⚠️  No remaining drafts for company_id={company_id}, skipping.")
            continue

        # Company info (for personalization)
        company_info = fetch_company_info(company_id)
        if not company_info:
            print(f"⚠️  No company info for company_id={company_id}, skipping.")
            continue

        # Recipients for this company
        recipients = fetch_recipients_for_company(company_id)
        if not recipients:
            print(f"⚠️  No contacts with email for company_id={company_id}; skipping.")
            continue

        print(f"\n🏢 Processing company: {company_info.get('company_name','Unknown')} (company_id={company_id})")
        print(f"   Drafts found: {len(drafts)} | Recipients: {len(recipients)}")

        # Process each draft for this company (usually one, but supports many)
        for draft in drafts:
            email_id = draft["email_id"]
            subject = (draft.get("subject") or "").strip()
            body    = (draft.get("body") or "").strip()
            outreach_person = draft.get("outreach_person", "")

            any_sent = False

            for r in recipients:
                to_addr = r["email_address"].strip()
                ok, _ = valid_email_syntax(to_addr)
                if not ok:
                    print(f"⏭️  Skip invalid address (syntax): {to_addr}")
                    continue

                domain = to_addr.split("@", 1)[1]
                if not domain_can_receive(domain):
                    print(f"⏭️  Skip address (domain has no MX/A): {to_addr}")
                    continue

                # Personalize & preview
                personalized_subject = personalize_content(subject, r, company_info)
                personalized_body = personalize_content(body, r, company_info)
                html_body = personalized_body if personalized_body.lstrip().startswith("<") else to_html(personalized_body)

                # html_body = to_html(personalized_body)

                recipient_name = r.get("contact_name", "Unknown Contact")
                recipient_title = r.get("contact_title", "")
                title_display = f" ({recipient_title})" if recipient_title else ""

                print("\n" + "="*80)
                print(f"📧 EMAIL PREVIEW")
                print(f"To:      {recipient_name}{title_display} <{to_addr}>")
                print(f"Company: {company_info.get('company_name', 'Unknown')}")
                print(f"Subject: {personalized_subject}")
                print(f"From:    {outreach_person or FROM_EMAIL}")
                print("-" * 80)
                print("Body:")
                print(personalized_body[:1000] + ("..." if len(personalized_body) > 1000 else ""))
                print("="*80)

                if not ask_confirm("Send this email?"):
                    print("   ↪️  Skipped by user.")
                    continue

                try:
                    smtp_send(to_addr, personalized_subject, personalized_body, body_html=html_body)
                    any_sent = True
                    total_sends += 1
                    print(f"✅ Sent to {recipient_name} <{to_addr}>")
                except (smtplib.SMTPException, socket.error) as e:
                    print(f"❌ Send failed to {to_addr}: {e}")

            if any_sent:
                try:
                    mark_email_sent(email_id, sent_count=1)
                    print(f"🗂️  Marked email_id={email_id} as sent.")
                except Exception as e:
                    print(f"⚠️  Could not update status for email_id={email_id}: {e}")

    print(f"\n🏁 Done. Total emails sent this run: {total_sends}")


if __name__ == "__main__":
    main()