import os
import re
import time
import requests
import googlemaps
import random
import string
import smtplib
import socket
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
from typing import Optional, Tuple, List, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import dns.resolver
import dns.exception
from tenacity import retry, stop_after_attempt, wait_exponential
from db_connect import supabase

# -----------------------------
# Config
# -----------------------------
load_dotenv()
gmaps = googlemaps.Client(key=os.getenv("GOOGLEMAPS_PLACES_KEY"))

# UW location
location = (47.65673397183744, -122.30658974412395)
radius = 32186  # 20 miles
keywords = [
    # "accounting","airport","amusement_park","aquarium","art_gallery","atm","bakery","bank",
    # "bar","beauty_salon","bicycle_store","book_store","bowling_alley","bus_station","cafe","campground",
    # "car_dealer","car_rental","car_repair","car_wash","casino",
    # "cemetery","city_hall",
    # "clothing_store","convenience_store","courthouse","dentist","department_store","doctor","drugstore",
    # "electrician","electronics_store","embassy","florist","funeral_home","furniture_store",
    # "gas_station","gym","hair_care","hardware_store","home_goods_store","hospital",
    # "insurance_agency","jewelry_store","laundry","lawyer","library","liquor_store",
    # "local_government_office","locksmith","lodging","meal_delivery","meal_takeaway","movie_rental",
    # "movie_theater","moving_company","museum","painter","park","pet_store","pharmacy",
    # "physiotherapist","plumber","post_office","primary_school","real_estate_agency","restaurant",
    # "roofing_contractor","rv_park","school","secondary_school","shoe_store","shopping_mall","spa","stadium",
    # "storage","store","supermarket","tourist_attraction","travel_agency",
    "university","veterinary_care","zoo"]

# Email validation settings
SMTP_PROBE = True           # Enable SMTP validation
CATCHALL_TEST = True        # Test for catch-all domains
SMTP_TIMEOUT = 12           # SMTP timeout in seconds
FROM_PROBE = "validator@probe.invalid"

# -----------------------------
# Advanced Email Validation (from your validator)
# -----------------------------

LOCAL_PART_MAX = 64
DOMAIN_MAX = 253
EMAIL_MAX = 254

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

# DNS setup
resolver = dns.resolver.Resolver(configure=True)
resolver.lifetime = 6.0
resolver.timeout = 3.0
dns_cache = {}

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
    """Enhanced DNS validation"""
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

def smtp_validate_email(mx_hosts: List[str], email: str) -> Tuple[str, str]:
    """Enhanced SMTP validation with better error categorization"""
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

def validate_email_comprehensive(email: str) -> Tuple[str, str]:
    """
    Comprehensive email validation
    Returns: (verdict, reason)
      verdict ∈ {"valid", "hard-invalid", "unknown"}
    """
    # Step 1: Advanced syntax check
    syntax_ok, domain, syntax_error = advanced_email_syntax_check(email)
    if not syntax_ok:
        return "hard-invalid", f"syntax:{syntax_error}"

    # Step 2: DNS validation
    dns_status, mx_list = comprehensive_dns_check(domain)
    
    if dns_status == "nxdomain":
        return "hard-invalid", "dns:nxdomain"
    if dns_status == "null_mx":
        return "hard-invalid", "dns:null_mx"
    if dns_status == "idna_error":
        return "hard-invalid", "dns:idna_error"
    if dns_status == "no_records":
        return "hard-invalid", "dns:no_mx_no_a"
    if dns_status == "dns_error":
        return "unknown", "dns:error"

    # Step 3: SMTP validation (if enabled and we have MX records)
    if not SMTP_PROBE:
        return "valid", f"dns:{dns_status}"

    if dns_status != "mx_found" or not mx_list:
        # Domain has A/AAAA but no MX - technically valid but unusual
        return "valid", f"dns:{dns_status}"

    mx_hosts = [host for _, host in mx_list]

    # Step 4: Catch-all detection
    try:
        if CATCHALL_TEST and detect_catchall(mx_hosts, domain):
            return "unknown", "smtp:catchall"
    except Exception:
        pass  # Continue with regular validation

    # Step 5: SMTP recipient validation
    try:
        category, details = smtp_validate_email(mx_hosts, email)
        
        if category == "valid":
            return "valid", "smtp:ok"
        elif category in ["user_unknown", "hard_bounce", "mailbox_full", "rejected"]:
            return "hard-invalid", f"smtp:{category}"
        else:
            return "unknown", f"smtp:{category}"
    
    except Exception as e:
        return "unknown", f"smtp:error:{type(e).__name__}"

# -----------------------------
# Email scraping functions (updated)
# -----------------------------

CONTACT_PATHS = [
    'contact','contact-us','about','about-us','team','support','staff','help',
    'info','email','reach-us','get-in-touch','leadership','management'
]

def find_emails_in_text(text):
    pattern = r'\b[a-zA-Z0-9]([a-zA-Z0-9._+-]*[a-zA-Z0-9])?@[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?\.[a-zA-Z]{2,}\b'
    emails = re.findall(pattern, text)
    if emails and isinstance(emails[0], tuple):
        return [m.group(0) for m in re.finditer(pattern, text)]
    return emails

def clean_email(email):
    if not email:
        return None
    email = email.strip()
    email = re.sub(r'[.,;:)}\]>"\'\s]+$', '', email)
    email = re.sub(r'^[.,;:({[\<"\'\s]+', '', email)
    parts = re.split(r'[\s,;|&<>"\'\(\)\[\]{}]', email)
    for part in parts:
        if '@' in part and '.' in part.split('@')[-1]:
            email = part
            break
    email = re.sub(r'[^a-zA-Z0-9@._+-].*$', '', email)
    if '@' in email and '.' in email.split('@')[-1]:
        return email.lower()
    return None

def scrape_emails_from_url(url):
    try:
        if url.startswith("mailto:"):
            email = url.split("mailto:")[1].split("?")[0].strip()
            cleaned = clean_email(email)
            return [cleaned] if cleaned else []

        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"}
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        found_emails = set()

        # mailto links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                email = href.split("mailto:")[1].split("?")[0].strip()
                cleaned = clean_email(email)
                if cleaned:
                    found_emails.add(cleaned)

        # hrefs that contain emails
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "@" in href and not href.startswith("mailto:"):
                for email in find_emails_in_text(href):
                    cleaned = clean_email(email)
                    if cleaned:
                        found_emails.add(cleaned)

        # link text
        for a in soup.find_all("a"):
            if a.text and "@" in a.text:
                for email in find_emails_in_text(a.text):
                    cleaned = clean_email(email)
                    if cleaned:
                        found_emails.add(cleaned)

        # visible text
        visible_text = soup.get_text(separator=" ", strip=True)
        for email in find_emails_in_text(visible_text):
            cleaned = clean_email(email)
            if cleaned:
                found_emails.add(cleaned)

        # data-email attributes
        for element in soup.find_all(attrs={"data-email": True}):
            cleaned = clean_email(element.get("data-email"))
            if cleaned:
                found_emails.add(cleaned)

        # obfuscated " [at] " / " [dot] "
        obfuscated_pattern = r'\b[a-zA-Z0-9._+-]+\s*\[at\]\s*[a-zA-Z0-9.-]+\s*\[dot\]\s*[a-zA-Z]{2,}\b'
        for email in re.findall(obfuscated_pattern, visible_text, re.IGNORECASE):
            normal = re.sub(r'\s*\[at\]\s*', '@', email, flags=re.IGNORECASE)
            normal = re.sub(r'\s*\[dot\]\s*', '.', normal, flags=re.IGNORECASE)
            cleaned = clean_email(normal)
            if cleaned:
                found_emails.add(cleaned)

        return list(found_emails)

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed for {url}: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to scrape {url}: {e}")
        return []

def find_contact_links(base_url):
    try:
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(base_url, timeout=15, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        links = set()
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            link_text = a.get_text().lower()
            for keyword in CONTACT_PATHS:
                if keyword in href or keyword in link_text:
                    links.add(urljoin(base_url, a['href']))
                    break
        return list(links)
    except Exception as e:
        print(f"[ERROR] Couldn't find contact links for {base_url}: {e}")
        return []

def extract_emails_from_website(website):
    if not website:
        return []
    if not website.startswith("http"):
        website = "https://" + website
    try:
        parsed = urlparse(website)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
    except:
        print(f"[ERROR] Invalid URL format: {website}")
        return []

    found = set()
    urls_to_scrape = {website}
    if website != base_url:
        urls_to_scrape.add(base_url)
    urls_to_scrape.update(find_contact_links(base_url)[:5])

    for url in urls_to_scrape:
        print(f"  → Scraping: {url}")
        found.update(scrape_emails_from_url(url))
        time.sleep(1)

    print(f"  → Found {len(found)} raw emails")
    
    # Apply comprehensive email validation
    valid_emails = []
    invalid_emails = []
    unknown_emails = []
    
    for email in found:
        verdict, reason = validate_email_comprehensive(email)
        if verdict == "valid":
            valid_emails.append(email)
            print(f"  ✅ Valid: {email} ({reason})")
        elif verdict == "hard-invalid":
            invalid_emails.append((email, reason))
            print(f"  ❌ Invalid: {email} ({reason})")
        else:  # unknown
            unknown_emails.append((email, reason))
            print(f"  ❓ Unknown: {email} ({reason})")
    
    print(f"  → Validation results: {len(valid_emails)} valid, {len(invalid_emails)} invalid, {len(unknown_emails)} unknown")
    
    # Only return definitely valid emails
    return valid_emails

# -----------------------------
# Main Google Places function
# -----------------------------

def main():
    print("🔍 Google Places with Advanced Email Validation")
    print("=" * 60)
    print(f"Settings:")
    print(f"  SMTP_PROBE: {SMTP_PROBE}")
    print(f"  CATCHALL_TEST: {CATCHALL_TEST}")
    print(f"  Location: {location}")
    print(f"  Radius: {radius/1609.34:.1f} miles")
    print(f"  Keywords: {keywords}")
    print()

    seen_place_ids = set()
    inserted = 0
    skipped_duplicate = 0
    skipped_no_website = 0
    skipped_no_valid_email = 0
    skipped_invalid_emails = 0

    for keyword in keywords:
        print(f"\n🔎 Searching for: {keyword}")
        try:
            places_result = gmaps.places_nearby(location=location, radius=radius, keyword=keyword)
        except Exception as e:
            print(f"[ERROR] Initial request for {keyword}: {e}")
            continue

        all_results = places_result.get("results", [])

        # Handle paging
        while 'next_page_token' in places_result:
            time.sleep(2)
            try:
                places_result = gmaps.places_nearby(
                    location=location,
                    radius=radius,
                    keyword=keyword,
                    page_token=places_result['next_page_token']
                )
                all_results.extend(places_result.get("results", []))
            except Exception as e:
                print(f"[ERROR] Paging {keyword}: {e}")
                break

        print(f"Found {len(all_results)} places for {keyword}")

        for i, place in enumerate(all_results, 1):
            place_id = place.get("place_id")
            if not place_id:
                continue
                
            if place_id in seen_place_ids:
                skipped_duplicate += 1
                continue
            seen_place_ids.add(place_id)

            name = place.get("name", "Unknown")
            print(f"\n[{i}/{len(all_results)}] Processing: {name}")

            # Get details to find website
            try:
                time.sleep(1)
                details = gmaps.place(place_id=place_id).get("result", {})
                description = details.get("formatted_address", "")
                website = details.get("website")
            except Exception as e:
                print(f"[ERROR] Getting details for {name}: {e}")
                continue

            # Must have a website
            if not website:
                print(f"[SKIP] No website found for {name}")
                skipped_no_website += 1
                continue

            # Check if company already exists
            try:
                existing = supabase.table("companies").select("company_name").eq("company_name", name).execute()
                if existing.data:
                    print(f"[SKIP] {name} already in database")
                    skipped_duplicate += 1
                    continue
            except Exception as e:
                print(f"[ERROR] Checking database for {name}: {e}")
                continue

            # Extract and validate emails
            print(f"[EMAIL VALIDATION] Scraping and validating emails for {name}")
            print(f"  Website: {website}")
            
            valid_emails = extract_emails_from_website(website)

            if not valid_emails:
                print(f"[SKIP] No valid emails found for {name}")
                skipped_no_valid_email += 1
                continue

            # Only proceed if we have at least one valid email
            print(f"[SUCCESS] Found {len(valid_emails)} valid emails for {name}")
            
            # Insert company
            try:
                supabase.table("companies").insert({
                    'company_name': name,
                    'description': description,
                    'website': website
                }).execute()
                print(f"[ADDED COMPANY] {name}")
                inserted += 1
            except Exception as e:
                print(f"[ERROR] Inserting company {name}: {e}")
                continue

            # Get company_id
            try:
                row = supabase.table("companies").select("company_id").eq("company_name", name).limit(1).single().execute().data
                company_id = row["company_id"]
            except Exception as e:
                print(f"[ERROR] Could not retrieve company_id for {name}: {e}")
                continue

            # Insert valid emails as contacts
            contacts_added = 0
            for email in valid_emails:
                try:
                    supabase.table("contacts").insert({
                        "company_name": name,
                        "company_id": company_id,
                        "email_address": email
                    }).execute()
                    print(f"[ADDED CONTACT] {email}")
                    contacts_added += 1
                except Exception as e:
                    print(f"[ERROR] Inserting contact {email}: {e}")
            
            print(f"  → Added {contacts_added} contacts for {name}")

    # Final summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"Companies added:           {inserted:,}")
    print(f"Skipped (duplicate):       {skipped_duplicate:,}")
    print(f"Skipped (no website):      {skipped_no_website:,}")
    print(f"Skipped (no valid emails): {skipped_no_valid_email:,}")
    print(f"Total places processed:    {len(seen_place_ids):,}")
    
    if inserted > 0:
        print(f"\n✅ Successfully added {inserted} companies with validated emails to database")
    else:
        print(f"\n⚠️ No companies were added - check validation settings or search criteria")

if __name__ == "__main__":
    main()