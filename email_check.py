# import os
# import re
# import sys
# import socket
# from typing import Optional, Tuple, List
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from dotenv import load_dotenv
# from supabase import create_client, Client
# import dns.resolver
# import dns.exception
# from tenacity import retry, stop_after_attempt, wait_exponential

# load_dotenv()

# SUPABASE_URL = os.getenv("SUPABASE_URL")
# SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# if not SUPABASE_URL or not SUPABASE_KEY:
#     print("❌ Missing SUPABASE_URL or SUPABASE_KEY")
#     sys.exit(1)

# # CONTROLS 
# DRY_RUN = True # true = print invalid emails, false = deletes/sets email as null
# HARD_DELETE = True  # true = delete entire contact row, false for setting email as null
# START_CONTACT_ID = 0
# LIMIT = 1000 # 0 for unlim although i think it automax at 1000

# sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# # syntax check
# LOCAL_PART_MAX = 64
# DOMAIN_MAX = 255
# EMAIL_RE = re.compile(
#     r"^(?P<local>[^@\s]+)@(?P<domain>[^@\s]+\.[A-Za-z0-9\-\.]+)$"
# )

# def basic_email_syntax_ok(addr: str) -> Tuple[bool, Optional[str]]:
#     if not addr or len(addr) > 320:
#         return False, "empty-or-too-long"
#     m = EMAIL_RE.match(addr.strip())
#     if not m:
#         return False, "regex-fail"
#     local = m.group("local")
#     domain = m.group("domain")
#     if len(local) > LOCAL_PART_MAX:
#         return False, "local-too-long"
#     if len(domain) > DOMAIN_MAX:
#         return False, "domain-too-long"
#     if ".." in domain or domain.startswith(".") or domain.endswith("."):
#         return False, "domain-dots"
#     return True, None

# # dns lookup
# resolver = dns.resolver.Resolver(configure=True)
# resolver.lifetime = 3.0    # total time per query
# resolver.timeout = 2.0     # per try

# @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=0.5, max=2))
# def has_mx(domain_idna: str) -> bool:
#     try:
#         answers = resolver.resolve(domain_idna, "MX")
#         return any(r.exchange for r in answers)
#     except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, dns.exception.DNSException):
#         return False

# @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=0.5, max=2))
# def has_a_or_aaaa(domain_idna: str) -> bool:
#     try:
#         # A
#         a_ok = False
#         try:
#             answers_a = resolver.resolve(domain_idna, "A")
#             a_ok = len(answers_a) > 0
#         except dns.exception.DNSException:
#             a_ok = False
#         # AAAA
#         aaaa_ok = False
#         try:
#             answers_aaaa = resolver.resolve(domain_idna, "AAAA")
#             aaaa_ok = len(answers_aaaa) > 0
#         except dns.exception.DNSException:
#             aaaa_ok = False
#         return a_ok or aaaa_ok
#     except dns.exception.DNSException:
#         return False

# def dns_valid_for_email(domain: str) -> Tuple[bool, str]:
#     """True if domain has MX, or (fallback) A/AAAA."""
#     try:
#         domain_idna = domain.encode("idna").decode("ascii")
#     except Exception:
#         return False, "idna-error"
#     if has_mx(domain_idna):
#         return True, "mx"
#     if has_a_or_aaaa(domain_idna):
#         # Some providers accept mail to bare A/AAAA; weaker signal but acceptable if MX absent
#         return True, "a_or_aaaa"
#     return False, "no-mx-no-a"

# #supabase
# def fetch_contacts_batch(start_contact_id: int, limit: int) -> List[dict]:
#     q = (sb.table("contacts")
#            .select("contact_id, company_id, contact_name, email_address")
#            .gte("contact_id", start_contact_id)
#            .order("contact_id", desc=False))
#     if limit and limit > 0:
#         q = q.limit(limit)
#     res = q.execute().data
#     # Filter out NULL/empty emails early
#     return [r for r in (res or []) if r.get("email_address")]

# def delete_contact(contact_id: int) -> bool:
#     try:
#         sb.table("contacts").delete().eq("contact_id", contact_id).execute()
#         return True
#     except Exception as e:
#         print(f"❌ Delete failed for contact_id={contact_id}: {e}")
#         return False

# def null_out_email(contact_id: int) -> bool:
#     """If you prefer not to delete the contact, clear the email only."""
#     try:
#         sb.table("contacts").update({"email_address": None}).eq("contact_id", contact_id).execute()
#         return True
#     except Exception as e:
#         print(f"❌ Update failed for contact_id={contact_id}: {e}")
#         return False

# def validate_one(row: dict) -> Tuple[int, str, bool, str]:
#     """
#     Returns: (contact_id, email, is_valid, reason)
#     """
#     cid = row["contact_id"]
#     email = row["email_address"].strip()
#     ok, reason = basic_email_syntax_ok(email)
#     if not ok:
#         return cid, email, False, f"syntax:{reason}"
#     domain = email.split("@", 1)[1]
#     good, why = dns_valid_for_email(domain)
#     return cid, email, good, f"dns:{why}"

# def main():
#     print("🔎 Email DNS validator starting …")
#     print(f"   START_CONTACT_ID={START_CONTACT_ID}, LIMIT={LIMIT or '∞'}, DRY_RUN={DRY_RUN}")
#     rows = fetch_contacts_batch(START_CONTACT_ID, LIMIT)
#     total = len(rows)
#     print(f"📥 Loaded {total} contacts with non-empty email_address")

#     if total == 0:
#         print("Nothing to do.")
#         return

#     # Concurrency to speed up DNS checks (safe: independent queries)
#     invalid: List[Tuple[int, str, str]] = []
#     valid_count = 0

#     with ThreadPoolExecutor(max_workers=20) as pool:
#         futures = {pool.submit(validate_one, r): r for r in rows}
#         for fut in as_completed(futures):
#             cid, email, good, reason = fut.result()
#             if good:
#                 valid_count += 1
#             else:
#                 invalid.append((cid, email, reason))

#     print(f"Valid: {valid_count} / {total} | Invalid: {len(invalid)}")

#     if not invalid:
#         print("✅ All emails passed DNS checks.")
#         return

#     print("\n❗ Invalid emails detected:")
#     for cid, email, reason in invalid[:25]:
#         print(f"   - contact_id={cid}  {email}  ({reason})")
#     if len(invalid) > 25:
#         print(f"   … and {len(invalid) - 25} more")

#     if DRY_RUN:
#         print("\n DRY_RUN=true → No deletions performed. Set DRY_RUN=false to remove invalid contacts.")
#         return

#     removed = 0
#     for cid, email, reason in invalid:
#         ok = delete_contact(cid) if HARD_DELETE else null_out_email(cid)
#         if ok:
#             removed += 1
#         else:
#             print(f"   ⚠️ Failed to remove/clear contact_id={cid} ({email})")

#     action = "deleted" if HARD_DELETE else "cleared email for"
#     print(f"\n🧹 Completed cleanup: {action} {removed} invalid contact(s).")

# if __name__ == "__main__":
#     main()
# import os
# import re
# import sys
# import random
# import string
# import socket
# import smtplib
# from typing import Optional, Tuple, List, Dict
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from dotenv import load_dotenv
# from supabase import create_client, Client
# import dns.name
# import dns.resolver
# import dns.exception
# from tenacity import retry, stop_after_attempt, wait_exponential

# load_dotenv()

# SUPABASE_URL = os.getenv("SUPABASE_URL")
# SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# if not SUPABASE_URL or not SUPABASE_KEY:
#     print("❌ Missing SUPABASE_URL or SUPABASE_KEY")
#     sys.exit(1)

# # =========================
# # CONTROLS
# # =========================
# DRY_RUN = True            # true = print invalid emails, false = delete or null them
# HARD_DELETE = True        # true = delete entire contact row, false to set email to null
# PAGE_SIZE = 1000          # contacts per page
# START_CONTACT_ID = 0      # resume point by contact_id
# MAX_WORKERS = 25          # concurrency for validation
# SMTP_PROBE = True         # enable SMTP RCPT TO check (recommended)
# SMTP_TIMEOUT = 8          # per-connection timeout seconds
# CATCHALL_TEST = True      # test a random address on the same domain to detect catch-all
# FROM_PROBE = "validator@probe.invalid"  # MAIL FROM used during SMTP probe (never delivered)

# # Deletion policy:
# #   - We delete/null ONLY for "hard-invalid"
# #   - We DO NOT delete for "unknown" (timeouts, temp errors, catch-all)
# DELETE_ON_REASONS = {"syntax", "dns:null_mx", "dns:no_mx_no_a", "smtp:hard_bounce"}

# sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# # =========================
# # Email syntax
# # =========================
# LOCAL_PART_MAX = 64
# DOMAIN_MAX = 255
# EMAIL_RE = re.compile(r"^(?P<local>[^@\s]+)@(?P<domain>[^@\s]+\.[A-Za-z0-9\-\.]+)$")

# def basic_email_syntax_ok(addr: str) -> Tuple[bool, Optional[str], Optional[str]]:
#     if not addr:
#         return False, None, "empty"
#     addr = addr.strip()
#     if len(addr) > 320:
#         return False, None, "too-long"
#     m = EMAIL_RE.match(addr)
#     if not m:
#         return False, None, "regex-fail"
#     local = m.group("local")
#     domain = m.group("domain")
#     if len(local) > LOCAL_PART_MAX:
#         return False, None, "local-too-long"
#     if len(domain) > DOMAIN_MAX:
#         return False, None, "domain-too-long"
#     if ".." in domain or domain.startswith(".") or domain.endswith("."):
#         return False, None, "domain-dots"
#     return True, domain, None

# # =========================
# # DNS helpers
# # =========================
# resolver = dns.resolver.Resolver(configure=True)
# resolver.lifetime = 3.5
# resolver.timeout = 2.0

# @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=0.4, max=2))
# def get_mx_records(domain_idna: str) -> List[Tuple[int, str]]:
#     """Return sorted [(preference, host), ...] or [] if no MX."""
#     try:
#         answers = resolver.resolve(domain_idna, "MX")
#         mx = []
#         for r in answers:
#             pref = int(r.preference)
#             host = str(r.exchange).rstrip(".")
#             mx.append((pref, host))
#         mx.sort(key=lambda x: x[0])
#         return mx
#     except dns.resolver.NoAnswer:
#         return []
#     except (dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, dns.exception.DNSException):
#         return []

# @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=0.4, max=2))
# def has_a_or_aaaa(domain_idna: str) -> bool:
#     try:
#         try:
#             if resolver.resolve(domain_idna, "A"): 
#                 return True
#         except dns.exception.DNSException:
#             pass
#         try:
#             if resolver.resolve(domain_idna, "AAAA"): 
#                 return True
#         except dns.exception.DNSException:
#             pass
#         return False
#     except dns.exception.DNSException:
#         return False

# def dns_status_for_email(domain: str) -> Tuple[str, Optional[List[Tuple[int,str]]]]:
#     """
#     Returns (status, mx_list_or_none)
#       status ∈ {"mx", "a_or_aaaa", "null_mx", "no_mx_no_a"}
#     Detect RFC 7505 null MX (MX 0 .)
#     """
#     try:
#         domain_idna = domain.encode("idna").decode("ascii")
#     except Exception:
#         return "no_mx_no_a", None

#     mx = get_mx_records(domain_idna)
#     # RFC 7505: MX 0 .
#     if any(pref == 0 and host == "" for pref, host in [(p, h if h != "." else "") for p, h in mx]):
#         return "null_mx", None

#     if mx:
#         return "mx", mx
#     if has_a_or_aaaa(domain_idna):
#         return "a_or_aaaa", None
#     return "no_mx_no_a", None

# # =========================
# # SMTP verification
# # =========================
# def _random_localpart(n: int = 12) -> str:
#     return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

# def smtp_probe_recipient(mx_hosts: List[str], recipient: str) -> Tuple[str, str]:
#     """
#     Try RCPT TO against the first working MX.
#     Returns (category, details)
#       category ∈ {"ok", "hard_bounce", "temp_fail", "unreachable"}
#     """
#     last_err = "no-connection"
#     for host in mx_hosts:
#         try:
#             with smtplib.SMTP(host, 25, timeout=SMTP_TIMEOUT) as s:
#                 code, _ = s.ehlo()
#                 if 200 <= code < 400:
#                     try:
#                         # Try STARTTLS if offered
#                         if s.has_extn("starttls"):
#                             code, _ = s.starttls()
#                             if 200 <= code < 400:
#                                 s.ehlo()
#                     except Exception:
#                         pass

#                 # MAIL FROM (a harmless probe sender)
#                 code, _ = s.mail(FROM_PROBE)
#                 if code >= 500:
#                     return "unreachable", f"mail-from-{code}"

#                 # RCPT TO
#                 code, msg = s.rcpt(recipient)
#                 if 200 <= code < 300:
#                     return "ok", f"{code}"
#                 if 500 <= code < 600:
#                     # Hard bounce (e.g., 550 5.1.1)
#                     return "hard_bounce", f"{code} {msg.decode(errors='ignore') if isinstance(msg, bytes) else msg}"
#                 if 400 <= code < 500:
#                     return "temp_fail", f"{code} {msg.decode(errors='ignore') if isinstance(msg, bytes) else msg}"

#                 # Unexpected but treat as unreachable/unknown
#                 last_err = f"rcpt-{code}"
#         except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError, smtplib.SMTPHeloError,
#                 smtplib.SMTPDataError, smtplib.SMTPRecipientsRefused, socket.timeout,
#                 OSError) as e:
#             last_err = type(e).__name__
#             continue
#     return "unreachable", last_err

# def is_catchall(mx_hosts: List[str], domain: str) -> bool:
#     test_rcpt = f"{_random_localpart()}@{domain}"
#     category, _ = smtp_probe_recipient(mx_hosts, test_rcpt)
#     return category == "ok"

# # =========================
# # Supabase helpers (pagination)
# # =========================
# def fetch_contacts_page(after_contact_id: int, page_size: int) -> List[Dict]:
#     q = (sb.table("contacts")
#            .select("contact_id, company_id, contact_name, email_address")
#            .gt("contact_id", after_contact_id)     # strict greater-than for cursor paging
#            .order("contact_id", desc=False)
#            .limit(page_size))
#     res = q.execute().data or []
#     # Filter out null/empty/whitespace emails early
#     clean = []
#     for r in res:
#         e = r.get("email_address")
#         if e and e.strip():
#             r["email_address"] = e.strip()
#             clean.append(r)
#     return clean

# def delete_contact(contact_id: int) -> bool:
#     try:
#         sb.table("contacts").delete().eq("contact_id", contact_id).execute()
#         return True
#     except Exception as e:
#         print(f"❌ Delete failed for contact_id={contact_id}: {e}")
#         return False

# def null_out_email(contact_id: int) -> bool:
#     try:
#         sb.table("contacts").update({"email_address": None}).eq("contact_id", contact_id).execute()
#         return True
#     except Exception as e:
#         print(f"❌ Update failed for contact_id={contact_id}: {e}")
#         return False

# # =========================
# # Validator
# # =========================
# def validate_one(row: Dict) -> Tuple[int, str, str, str]:
#     """
#     Returns: (contact_id, email, verdict, reason)
#       verdict ∈ {"valid", "hard-invalid", "unknown"}
#       reason: brief code for logging
#     """
#     cid = row["contact_id"]
#     email = row["email_address"]

#     ok, domain, why = basic_email_syntax_ok(email)
#     if not ok:
#         return cid, email, "hard-invalid", f"syntax:{why or 'fail'}"

#     # DNS checks
#     dns_status, mx_list = dns_status_for_email(domain)
#     if dns_status == "null_mx":
#         return cid, email, "hard-invalid", "dns:null_mx"
#     if dns_status == "no_mx_no_a":
#         return cid, email, "hard-invalid", "dns:no_mx_no_a"

#     # If SMTP probe disabled or no MX (but has A/AAAA), accept as valid (weak)
#     if not SMTP_PROBE or dns_status != "mx" or not mx_list:
#         # Without SMTP we can only say "valid (dns-ok)"
#         return cid, email, "valid", f"dns:{dns_status}"

#     # SMTP RCPT TO probe on the first few MX hosts
#     mx_hosts = [h for _, h in mx_list][:3]

#     # Catch-all detection
#     if CATCHALL_TEST and is_catchall(mx_hosts, domain):
#         # Can't tell if specific mailbox exists; don't delete—mark unknown
#         return cid, email, "unknown", "smtp:catchall"

#     category, details = smtp_probe_recipient(mx_hosts, email)
#     if category == "ok":
#         return cid, email, "valid", "smtp:ok"
#     if category == "hard_bounce":
#         return cid, email, "hard-invalid", "smtp:hard_bounce"
#     if category in ("temp_fail", "unreachable"):
#         return cid, email, "unknown", f"smtp:{category}"

#     # Fallback
#     return cid, email, "unknown", "smtp:unknown"

# # =========================
# # Main
# # =========================
# def main():
#     print("🔎 Email validator (DNS + optional SMTP) starting …")
#     print(f"   DRY_RUN={DRY_RUN}, HARD_DELETE={HARD_DELETE}, START_CONTACT_ID={START_CONTACT_ID}, PAGE_SIZE={PAGE_SIZE}, SMTP_PROBE={SMTP_PROBE}")

#     after_id = START_CONTACT_ID
#     total_checked = 0
#     valid_count = 0
#     hard_invalid: List[Tuple[int, str, str]] = []
#     unknowns: List[Tuple[int, str, str]] = []

#     while True:
#         page = fetch_contacts_page(after_id, PAGE_SIZE)
#         if not page:
#             break

#         # Advance cursor for next page
#         after_id = page[-1]["contact_id"]

#         with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
#             futures = {pool.submit(validate_one, r): r for r in page}
#             for fut in as_completed(futures):
#                 cid, email, verdict, reason = fut.result()
#                 total_checked += 1
#                 if verdict == "valid":
#                     valid_count += 1
#                 elif verdict == "hard-invalid":
#                     hard_invalid.append((cid, email, reason))
#                 else:
#                     unknowns.append((cid, email, reason))

#         # Optional: print progress every page
#         print(f"… progress: checked={total_checked}, valid={valid_count}, hard-invalid={len(hard_invalid)}, unknown={len(unknowns)}")

#     print("\n===== SUMMARY =====")
#     print(f"Checked: {total_checked}")
#     print(f"Valid:   {valid_count}")
#     print(f"Hard-invalid (safe to remove): {len(hard_invalid)}")
#     print(f"Unknown (do NOT auto-delete): {len(unknowns)}")

#     if hard_invalid:
#         print("\n❗ Hard-invalid examples:")
#         for cid, email, reason in hard_invalid[:25]:
#             print(f" - id={cid}  {email}  ({reason})")
#         if len(hard_invalid) > 25:
#             print(f"   … and {len(hard_invalid)-25} more")

#     if unknowns:
#         print("\nℹ️ Unknown examples (transient/catch-all/unreachable):")
#         for cid, email, reason in unknowns[:10]:
#             print(f" - id={cid}  {email}  ({reason})")
#         if len(unknowns) > 10:
#             print(f"   … and {len(unknowns)-10} more")

#     if DRY_RUN:
#         print("\nDRY_RUN=true → No deletions performed. Set DRY_RUN=false to apply cleanup of hard-invalids only.")
#         return

#     # Apply cleanup for hard-invalid only
#     removed = 0
#     for cid, email, reason in hard_invalid:
#         # Extra safeguard: only act on reasons we explicitly allow
#         reason_key = reason.split(":", 1)[0] if ":" in reason else reason
#         if reason in DELETE_ON_REASONS or reason_key in DELETE_ON_REASONS:
#             ok = delete_contact(cid) if HARD_DELETE else null_out_email(cid)
#             if ok:
#                 removed += 1
#             else:
#                 print(f"   ⚠️ Failed to remove/clear contact_id={cid} ({email})")
#         else:
#             print(f"   Skipped deletion for {cid} ({email}) due to reason={reason}")

#     action = "deleted" if HARD_DELETE else "cleared email for"
#     print(f"\n🧹 Completed cleanup: {action} {removed} hard-invalid contact(s).")

# if __name__ == "__main__":
#     main()

import os
import re
import sys
import random
import string
import socket
import smtplib
from typing import Optional, Tuple, List, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from supabase import create_client, Client
import dns.name
import dns.resolver
import dns.exception
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Missing SUPABASE_URL or SUPABASE_KEY")
    sys.exit(1)

# =========================
# CONTROLS
# =========================
DRY_RUN = True            # true = print invalid emails, false = delete or null them
HARD_DELETE = True        # true = delete entire contact row, false to set email to null
PAGE_SIZE = 1000          # contacts per page
START_CONTACT_ID = 0      # resume point by contact_id
MAX_WORKERS = 25          # concurrency for validation
SMTP_PROBE = True         # Enable SMTP probing for better validation
SMTP_TIMEOUT = 12         # timeout per connection
CATCHALL_TEST = True      # Test for catch-all domains
FROM_PROBE = "validator@probe.invalid"  # MAIL FROM used during SMTP probe

# More comprehensive deletion policy
DELETE_ON_REASONS = {
    # Syntax errors
    "syntax:empty", "syntax:too-long", "syntax:regex-fail", "syntax:local-too-long", 
    "syntax:domain-too-long", "syntax:domain-dots", "syntax:invalid-chars", 
    "syntax:multiple-at", "syntax:no-local", "syntax:no-domain", "syntax:invalid-tld",
    
    # DNS issues
    "dns:null_mx", "dns:no_mx_no_a", "dns:nxdomain",
    
    # SMTP hard bounces
    "smtp:hard_bounce", "smtp:user_unknown", "smtp:mailbox_full", "smtp:rejected"
}

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# Known invalid patterns and domains
# =========================
INVALID_PATTERNS = [
    r'\.{2,}',           # Multiple dots
    r'^\.|\.$',          # Starting or ending with dot
    r'@\.|\.$',          # Dot after @ or at end
    r'\s',               # Any whitespace
    r'[<>"]',            # Invalid characters
]

COMMON_INVALID_DOMAINS = {
    'example.com', 'test.com', 'localhost', 'invalid', 'fake.com',
    'domain.com', 'email.com', 'sample.com', 'demo.com', 'temp.com',
    'noemail.com', 'none.com', 'null.com', 'dummy.com', 'placeholder.com'
}

COMMON_INVALID_LOCALS = {
    'test', 'noreply', 'no-reply', 'donotreply', 'postmaster', 'admin',
    'info@', 'contact@', '@', '', 'null', 'none', 'example', 'sample',
    'demo', 'temp', 'fake', 'invalid', 'dummy', 'placeholder'
}

# =========================
# Enhanced Email syntax validation
# =========================
LOCAL_PART_MAX = 64
DOMAIN_MAX = 253  # RFC compliant
EMAIL_MAX = 254   # RFC compliant

def advanced_email_syntax_check(addr: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """Enhanced email syntax validation"""
    if not addr:
        return False, None, "empty"
    
    original_addr = addr
    addr = addr.strip().lower()
    
    if len(addr) > EMAIL_MAX:
        return False, None, "too-long"
    
    # Check for multiple @ symbols
    at_count = addr.count('@')
    if at_count != 1:
        if at_count == 0:
            return False, None, "no-at"
        else:
            return False, None, "multiple-at"
    
    # Split into local and domain parts
    try:
        local, domain = addr.split('@', 1)
    except ValueError:
        return False, None, "split-error"
    
    # Check for empty parts
    if not local:
        return False, None, "no-local"
    if not domain:
        return False, None, "no-domain"
    
    # Local part validation
    if len(local) > LOCAL_PART_MAX:
        return False, None, "local-too-long"
    
    # Check for invalid patterns in local part
    if local.startswith('.') or local.endswith('.') or '..' in local:
        return False, None, "invalid-local-dots"
    
    # Check for common invalid local parts
    if local in COMMON_INVALID_LOCALS:
        return False, None, "invalid-local"
    
    # Domain part validation
    if len(domain) > DOMAIN_MAX:
        return False, None, "domain-too-long"
    
    # Check for invalid patterns
    for pattern in INVALID_PATTERNS:
        if re.search(pattern, addr):
            return False, None, "invalid-chars"
    
    # Domain structure validation
    if domain.startswith('.') or domain.endswith('.') or '..' in domain:
        return False, None, "domain-dots"
    
    # Check for common invalid domains
    if domain in COMMON_INVALID_DOMAINS:
        return False, None, "invalid-domain"
    
    # Domain must have at least one dot
    if '.' not in domain:
        return False, None, "no-tld"
    
    domain_parts = domain.split('.')
    if len(domain_parts) < 2:
        return False, None, "invalid-domain-structure"
    
    # Validate each domain part
    for part in domain_parts:
        if not part:  # Empty part (like domain..com)
            return False, None, "empty-domain-part"
        if len(part) > 63:  # RFC limit for domain labels
            return False, None, "domain-label-too-long"
        if part.startswith('-') or part.endswith('-'):
            return False, None, "invalid-domain-hyphen"
        # Only allow alphanumeric and hyphens in domain parts
        if not re.match(r'^[a-zA-Z0-9-]+$', part):
            return False, None, "invalid-domain-chars"
    
    # TLD validation
    tld = domain_parts[-1]
    if len(tld) < 2:
        return False, None, "invalid-tld"
    if not tld.isalpha():
        return False, None, "invalid-tld"
    if len(tld) > 24:  # Longest current TLD
        return False, None, "tld-too-long"
    
    # Check for numeric-only TLD (invalid)
    if tld.isdigit():
        return False, None, "numeric-tld"
    
    return True, domain, None

# =========================
# Enhanced DNS helpers
# =========================
resolver = dns.resolver.Resolver(configure=True)
resolver.lifetime = 6.0
resolver.timeout = 3.0

# Cache for DNS results to avoid repeated lookups
dns_cache = {}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=0.5, max=3))
def get_mx_records(domain_idna: str) -> Tuple[List[Tuple[int, str]], str]:
    """Return ([(preference, host), ...], status)"""
    if domain_idna in dns_cache:
        return dns_cache[domain_idna]
    
    try:
        answers = resolver.resolve(domain_idna, "MX")
        mx = []
        for r in answers:
            pref = int(r.preference)
            host = str(r.exchange).rstrip(".")
            mx.append((pref, host))
        mx.sort(key=lambda x: x[0])
        result = (mx, "mx_found")
        dns_cache[domain_idna] = result
        return result
    except dns.resolver.NXDOMAIN:
        result = ([], "nxdomain")
        dns_cache[domain_idna] = result
        return result
    except dns.resolver.NoAnswer:
        result = ([], "no_mx")
        dns_cache[domain_idna] = result
        return result
    except (dns.resolver.NoNameservers, dns.exception.DNSException) as e:
        print(f"DNS error for {domain_idna}: {type(e).__name__}")
        result = ([], "dns_error")
        dns_cache[domain_idna] = result
        return result

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=0.5, max=3))
def has_a_or_aaaa(domain_idna: str) -> bool:
    cache_key = f"a_aaaa_{domain_idna}"
    if cache_key in dns_cache:
        return dns_cache[cache_key]
    
    has_records = False
    try:
        # Check A record
        try:
            if resolver.resolve(domain_idna, "A"):
                has_records = True
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            pass
        
        # Check AAAA record if A failed
        if not has_records:
            try:
                if resolver.resolve(domain_idna, "AAAA"):
                    has_records = True
            except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
                pass
                
    except dns.exception.DNSException:
        pass
    
    dns_cache[cache_key] = has_records
    return has_records

def comprehensive_dns_check(domain: str) -> Tuple[str, Optional[List[Tuple[int,str]]]]:
    """
    Enhanced DNS validation
    Returns (status, mx_list_or_none)
    """
    try:
        domain_idna = domain.encode("idna").decode("ascii")
    except Exception as e:
        return "idna_error", None

    # Get MX records
    mx_list, mx_status = get_mx_records(domain_idna)
    
    if mx_status == "nxdomain":
        return "nxdomain", None
    
    if mx_status == "dns_error":
        return "dns_error", None
    
    # Check for RFC 7505 null MX (explicit rejection)
    if mx_list and any(pref == 0 and host == "" for pref, host in [(p, h if h != "." else "") for p, h in mx_list]):
        return "null_mx", None
    
    if mx_list:
        return "mx_found", mx_list
    
    # No MX records, check for A/AAAA
    if has_a_or_aaaa(domain_idna):
        return "a_or_aaaa", None
    
    return "no_records", None

# =========================
# Enhanced SMTP validation
# =========================
def smtp_validate_email(mx_hosts: List[str], email: str) -> Tuple[str, str]:
    """
    Enhanced SMTP validation with better error categorization
    Returns (category, details)
    """
    last_error = "no_connection"
    
    # Try up to 3 MX hosts
    for host in mx_hosts[:3]:
        try:
            with smtplib.SMTP(host, 25, timeout=SMTP_TIMEOUT) as smtp:
                smtp.set_debuglevel(0)
                
                # EHLO/HELO
                code, response = smtp.ehlo()
                if not (200 <= code < 400):
                    try:
                        code, response = smtp.helo()
                        if not (200 <= code < 400):
                            continue
                    except Exception:
                        continue
                
                # Try STARTTLS if available
                try:
                    if smtp.has_extn("starttls"):
                        code, _ = smtp.starttls()
                        if 200 <= code < 400:
                            smtp.ehlo()
                except Exception:
                    pass
                
                # MAIL FROM
                try:
                    code, response = smtp.mail(FROM_PROBE)
                    if code >= 500:
                        return "mail_from_rejected", f"{code}"
                    if code >= 400:
                        continue
                except Exception as e:
                    continue
                
                # RCPT TO - the actual test
                try:
                    code, response = smtp.rcpt(email)
                    response_str = response.decode(errors='ignore') if isinstance(response, bytes) else str(response)
                    response_lower = response_str.lower()
                    
                    if 200 <= code < 300:
                        return "valid", f"{code}"
                    
                    elif 500 <= code < 600:
                        # Categorize hard bounces more precisely
                        if any(phrase in response_lower for phrase in [
                            "5.1.1", "user unknown", "no such user", "invalid recipient",
                            "recipient unknown", "mailbox not found", "does not exist"
                        ]):
                            return "user_unknown", f"{code} {response_str[:100]}"
                        elif any(phrase in response_lower for phrase in [
                            "5.2.2", "mailbox full", "quota exceeded", "over quota"
                        ]):
                            return "mailbox_full", f"{code} {response_str[:100]}"
                        elif any(phrase in response_lower for phrase in [
                            "5.7.1", "rejected", "blocked", "spam", "blacklist"
                        ]):
                            return "rejected", f"{code} {response_str[:100]}"
                        else:
                            return "hard_bounce", f"{code} {response_str[:100]}"
                    
                    elif 400 <= code < 500:
                        return "temp_fail", f"{code} {response_str[:100]}"
                    
                    else:
                        return "unexpected_code", f"{code} {response_str[:100]}"
                        
                except Exception as e:
                    last_error = f"rcpt_error_{type(e).__name__}"
                    continue
                    
        except Exception as e:
            last_error = f"connection_error_{type(e).__name__}"
            continue
    
    return "unreachable", last_error

def detect_catchall(mx_hosts: List[str], domain: str) -> bool:
    """Test multiple random addresses to detect catch-all"""
    if not mx_hosts:
        return False
    
    test_count = 3
    accepted = 0
    
    for _ in range(test_count):
        random_local = ''.join(random.choices(string.ascii_lowercase + string.digits, k=15))
        test_email = f"{random_local}@{domain}"
        
        try:
            category, _ = smtp_validate_email(mx_hosts[:2], test_email)
            if category == "valid":
                accepted += 1
        except Exception:
            continue
    
    # If 2+ random emails are accepted, likely catch-all
    return accepted >= 2

# =========================
# Supabase helpers
# =========================
def fetch_contacts_page(after_contact_id: int, page_size: int) -> List[Dict]:
    try:
        q = (sb.table("contacts")
               .select("contact_id, company_id, contact_name, email_address")
               .gt("contact_id", after_contact_id)
               .order("contact_id", desc=False)
               .limit(page_size))
        res = q.execute().data or []
        
        # Filter out null/empty emails
        clean = []
        for r in res:
            e = r.get("email_address")
            if e and str(e).strip():
                r["email_address"] = str(e).strip()
                clean.append(r)
        
        return clean
    except Exception as e:
        print(f"❌ Database error: {e}")
        return []

def delete_contact(contact_id: int) -> bool:
    try:
        sb.table("contacts").delete().eq("contact_id", contact_id).execute()
        return True
    except Exception as e:
        print(f"❌ Delete failed for contact_id={contact_id}: {e}")
        return False

def null_out_email(contact_id: int) -> bool:
    try:
        sb.table("contacts").update({"email_address": None}).eq("contact_id", contact_id).execute()
        return True
    except Exception as e:
        print(f"❌ Update failed for contact_id={contact_id}: {e}")
        return False

# =========================
# Main validation logic
# =========================
def validate_email_comprehensive(row: Dict) -> Tuple[int, str, str, str]:
    """
    Comprehensive email validation
    Returns: (contact_id, email, verdict, reason)
    """
    cid = row["contact_id"]
    email = row["email_address"]

    # Step 1: Advanced syntax check
    syntax_ok, domain, syntax_error = advanced_email_syntax_check(email)
    if not syntax_ok:
        return cid, email, "hard-invalid", f"syntax:{syntax_error}"

    # Step 2: DNS validation
    dns_status, mx_list = comprehensive_dns_check(domain)
    
    if dns_status == "nxdomain":
        return cid, email, "hard-invalid", "dns:nxdomain"
    if dns_status == "null_mx":
        return cid, email, "hard-invalid", "dns:null_mx"
    if dns_status == "idna_error":
        return cid, email, "hard-invalid", "dns:idna_error"
    if dns_status == "no_records":
        return cid, email, "hard-invalid", "dns:no_mx_no_a"
    if dns_status == "dns_error":
        return cid, email, "unknown", "dns:error"

    # Step 3: SMTP validation (if enabled and we have MX records)
    if not SMTP_PROBE:
        return cid, email, "valid", f"dns:{dns_status}"

    if dns_status != "mx_found" or not mx_list:
        # Domain has A/AAAA but no MX - technically valid but unusual
        return cid, email, "valid", f"dns:{dns_status}"

    mx_hosts = [host for _, host in mx_list]

    # Step 4: Catch-all detection
    try:
        if CATCHALL_TEST and detect_catchall(mx_hosts, domain):
            return cid, email, "unknown", "smtp:catchall"
    except Exception:
        pass  # Continue with regular validation

    # Step 5: SMTP recipient validation
    try:
        category, details = smtp_validate_email(mx_hosts, email)
        
        if category == "valid":
            return cid, email, "valid", "smtp:ok"
        elif category in ["user_unknown", "hard_bounce", "mailbox_full", "rejected"]:
            return cid, email, "hard-invalid", f"smtp:{category}"
        else:
            return cid, email, "unknown", f"smtp:{category}"
    
    except Exception as e:
        return cid, email, "unknown", f"smtp:error:{type(e).__name__}"

# =========================
# Main execution
# =========================
def main():
    print("🔍 Comprehensive Email Validator")
    print("=" * 50)
    print(f"Settings:")
    print(f"  DRY_RUN: {DRY_RUN}")
    print(f"  HARD_DELETE: {HARD_DELETE}")
    print(f"  SMTP_PROBE: {SMTP_PROBE}")
    print(f"  CATCHALL_TEST: {CATCHALL_TEST}")
    print(f"  MAX_WORKERS: {MAX_WORKERS}")
    print(f"  DELETE_ON_REASONS: {len(DELETE_ON_REASONS)} reasons")
    print()

    after_id = START_CONTACT_ID
    total_processed = 0
    valid_emails = 0
    invalid_emails: List[Tuple[int, str, str]] = []
    unknown_emails: List[Tuple[int, str, str]] = []

    try:
        while True:
            batch = fetch_contacts_page(after_id, PAGE_SIZE)
            if not batch:
                print("✅ No more contacts to process")
                break

            after_id = batch[-1]["contact_id"]
            
            print(f"Processing batch starting from contact_id {batch[0]['contact_id']}...")

            # Process batch with threading
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(validate_email_comprehensive, row): row for row in batch}
                
                for future in as_completed(futures):
                    try:
                        cid, email, verdict, reason = future.result()
                        total_processed += 1
                        
                        if verdict == "valid":
                            valid_emails += 1
                        elif verdict == "hard-invalid":
                            invalid_emails.append((cid, email, reason))
                        else:  # unknown
                            unknown_emails.append((cid, email, reason))
                        
                        # Progress indicator
                        if total_processed % 1000 == 0:
                            print(f"  Progress: {total_processed} processed")
                            
                    except Exception as e:
                        print(f"❌ Error processing contact: {e}")

            # Batch summary
            print(f"  Batch complete: {len(batch)} contacts processed")
            print(f"  Running totals - Valid: {valid_emails}, Invalid: {len(invalid_emails)}, Unknown: {len(unknown_emails)}")
            print()

    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")

    # Final summary
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(f"Total processed:    {total_processed:,}")
    print(f"Valid emails:       {valid_emails:,} ({valid_emails/total_processed*100:.1f}%)")
    print(f"Invalid emails:     {len(invalid_emails):,} ({len(invalid_emails)/total_processed*100:.1f}%)")
    print(f"Unknown emails:     {len(unknown_emails):,} ({len(unknown_emails)/total_processed*100:.1f}%)")

    # Detailed breakdown
    if invalid_emails:
        print(f"\n❌ INVALID EMAIL BREAKDOWN:")
        reason_counts = {}
        for _, _, reason in invalid_emails:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        
        for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {reason}: {count:,} emails")
        
        print(f"\nExamples of invalid emails:")
        shown = set()
        for cid, email, reason in invalid_emails[:20]:
            if reason not in shown:
                print(f"  {reason}: {email}")
                shown.add(reason)

    if unknown_emails:
        print(f"\n❓ UNKNOWN EMAIL BREAKDOWN:")
        reason_counts = {}
        for _, _, reason in unknown_emails:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        
        for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {reason}: {count:,} emails")

    # Apply changes if not dry run
    if DRY_RUN:
        deletable = len([x for x in invalid_emails if x[2] in DELETE_ON_REASONS])
        print(f"\n🔍 DRY RUN MODE - No changes applied")
        print(f"   Would delete: {deletable:,} emails")
        print(f"   To apply changes, set DRY_RUN = False")
    else:
        print(f"\n🗑️ APPLYING DELETIONS...")
        deleted_count = 0
        failed_count = 0
        
        for cid, email, reason in invalid_emails:
            if reason in DELETE_ON_REASONS:
                success = delete_contact(cid) if HARD_DELETE else null_out_email(cid)
                if success:
                    deleted_count += 1
                    if deleted_count % 500 == 0:
                        print(f"  Processed {deleted_count} deletions...")
                else:
                    failed_count += 1
        
        action = "deleted" if HARD_DELETE else "cleared"
        print(f"✅ Cleanup complete: {action} {deleted_count:,} contacts")
        if failed_count > 0:
            print(f"⚠️ Failed to process {failed_count:,} contacts")

if __name__ == "__main__":
    main()