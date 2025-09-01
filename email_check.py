import os
import re
import sys
import socket
from typing import Optional, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from supabase import create_client, Client
import dns.resolver
import dns.exception
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Missing SUPABASE_URL or SUPABASE_KEY")
    sys.exit(1)

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default

START_CONTACT_ID = _get_int("START_CONTACT_ID", 0)
LIMIT = _get_int("LIMIT", 0)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# syntax check
LOCAL_PART_MAX = 64
DOMAIN_MAX = 255
EMAIL_RE = re.compile(
    r"^(?P<local>[^@\s]+)@(?P<domain>[^@\s]+\.[A-Za-z0-9\-\.]+)$"
)

def basic_email_syntax_ok(addr: str) -> Tuple[bool, Optional[str]]:
    if not addr or len(addr) > 320:
        return False, "empty-or-too-long"
    m = EMAIL_RE.match(addr.strip())
    if not m:
        return False, "regex-fail"
    local = m.group("local")
    domain = m.group("domain")
    if len(local) > LOCAL_PART_MAX:
        return False, "local-too-long"
    if len(domain) > DOMAIN_MAX:
        return False, "domain-too-long"
    if ".." in domain or domain.startswith(".") or domain.endswith("."):
        return False, "domain-dots"
    return True, None

# --- DNS lookups --------------------------------------------------------------

resolver = dns.resolver.Resolver(configure=True)
resolver.lifetime = 3.0    # total time per query
resolver.timeout = 2.0     # per try

@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=0.5, max=2))
def has_mx(domain_idna: str) -> bool:
    try:
        answers = resolver.resolve(domain_idna, "MX")
        return any(r.exchange for r in answers)
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, dns.exception.DNSException):
        return False

@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=0.5, max=2))
def has_a_or_aaaa(domain_idna: str) -> bool:
    try:
        # A
        a_ok = False
        try:
            answers_a = resolver.resolve(domain_idna, "A")
            a_ok = len(answers_a) > 0
        except dns.exception.DNSException:
            a_ok = False
        # AAAA
        aaaa_ok = False
        try:
            answers_aaaa = resolver.resolve(domain_idna, "AAAA")
            aaaa_ok = len(answers_aaaa) > 0
        except dns.exception.DNSException:
            aaaa_ok = False
        return a_ok or aaaa_ok
    except dns.exception.DNSException:
        return False

def dns_valid_for_email(domain: str) -> Tuple[bool, str]:
    """True if domain has MX, or (fallback) A/AAAA."""
    try:
        domain_idna = domain.encode("idna").decode("ascii")
    except Exception:
        return False, "idna-error"
    if has_mx(domain_idna):
        return True, "mx"
    if has_a_or_aaaa(domain_idna):
        # Some providers accept mail to bare A/AAAA; weaker signal but acceptable if MX absent
        return True, "a_or_aaaa"
    return False, "no-mx-no-a"

#supabase
def fetch_contacts_batch(start_contact_id: int, limit: int) -> List[dict]:
    q = (sb.table("contacts")
           .select("contact_id, company_id, contact_name, email_address")
           .gte("contact_id", start_contact_id)
           .order("contact_id", desc=False))
    if limit and limit > 0:
        q = q.limit(limit)
    res = q.execute().data
    # Filter out NULL/empty emails early
    return [r for r in (res or []) if r.get("email_address")]

def delete_contact(contact_id: int) -> bool:
    try:
        sb.table("contacts").delete().eq("contact_id", contact_id).execute()
        return True
    except Exception as e:
        print(f"❌ Delete failed for contact_id={contact_id}: {e}")
        return False

def null_out_email(contact_id: int) -> bool:
    """If you prefer not to delete the contact, clear the email only."""
    try:
        sb.table("contacts").update({"email_address": None}).eq("contact_id", contact_id).execute()
        return True
    except Exception as e:
        print(f"❌ Update failed for contact_id={contact_id}: {e}")
        return False

# --- Pipeline ----------------------------------------------------------------

def validate_one(row: dict) -> Tuple[int, str, bool, str]:
    """
    Returns: (contact_id, email, is_valid, reason)
    """
    cid = row["contact_id"]
    email = row["email_address"].strip()
    ok, reason = basic_email_syntax_ok(email)
    if not ok:
        return cid, email, False, f"syntax:{reason}"
    domain = email.split("@", 1)[1]
    good, why = dns_valid_for_email(domain)
    return cid, email, good, f"dns:{why}"

def main():
    print("🔎 Email DNS validator starting …")
    print(f"   START_CONTACT_ID={START_CONTACT_ID}, LIMIT={LIMIT or '∞'}, DRY_RUN={DRY_RUN}")
    rows = fetch_contacts_batch(START_CONTACT_ID, LIMIT)
    total = len(rows)
    print(f"📥 Loaded {total} contacts with non-empty email_address")

    if total == 0:
        print("✅ Nothing to do.")
        return

    # Concurrency to speed up DNS checks (safe: independent queries)
    invalid: List[Tuple[int, str, str]] = []
    valid_count = 0

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(validate_one, r): r for r in rows}
        for fut in as_completed(futures):
            cid, email, good, reason = fut.result()
            if good:
                valid_count += 1
            else:
                invalid.append((cid, email, reason))

    print(f"🧮 Valid: {valid_count} / {total} | Invalid: {len(invalid)}")

    if not invalid:
        print("✅ All emails passed DNS checks.")
        return

    print("\n❗ Invalid emails detected:")
    for cid, email, reason in invalid[:25]:
        print(f"   - contact_id={cid}  {email}  ({reason})")
    if len(invalid) > 25:
        print(f"   … and {len(invalid) - 25} more")

    if DRY_RUN:
        print("\n🧪 DRY_RUN=true → No deletions performed. Set DRY_RUN=false to remove invalid contacts.")
        return

    HARD_DELETE = False  # true for hard delete, false for null

    removed = 0
    for cid, email, reason in invalid:
        ok = delete_contact(cid) if HARD_DELETE else null_out_email(cid)
        if ok:
            removed += 1
        else:
            print(f"   ⚠️ Failed to remove/clear contact_id={cid} ({email})")

    action = "deleted" if HARD_DELETE else "cleared email for"
    print(f"\n🧹 Completed cleanup: {action} {removed} invalid contact(s).")

if __name__ == "__main__":
    main()