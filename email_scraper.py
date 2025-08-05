import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from db_connect import supabase

CONTACT_PATHS = ['contact', 'contact-us', 'about', 'about-us', 'team', 'support', 'staff', 'help', 
                 'info', 'email', 'reach-us', 'get-in-touch', 'leadership', 'management']

start_company_id = 2965

def find_emails_in_text(text):
    pattern = r'\b[a-zA-Z0-9]([a-zA-Z0-9._+-]*[a-zA-Z0-9])?@[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?\.[a-zA-Z]{2,}\b'
    emails = re.findall(pattern, text)
    
    if emails and isinstance(emails[0], tuple):
        full_emails = []
        for match in re.finditer(pattern, text):
            full_emails.append(match.group(0))
        return full_emails
    
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

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
        }    

        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        found_emails = set()

        # mailto links 
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                email = href.split("mailto:")[1].split("?")[0].strip()
                cleaned = clean_email(email)
                if cleaned:
                    found_emails.add(cleaned)

        # href attributes that contain emails
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "@" in href and not href.startswith("mailto:"):
                emails = find_emails_in_text(href)
                for email in emails:
                    cleaned = clean_email(email)
                    if cleaned:
                        found_emails.add(cleaned)

        # link text
        for a in soup.find_all("a"):
            if a.text and "@" in a.text:
                emails = find_emails_in_text(a.text)
                for email in emails:
                    cleaned = clean_email(email)
                    if cleaned:
                        found_emails.add(cleaned)

        # visible text
        visible_text = soup.get_text(separator=" ", strip=True)
        text_emails = find_emails_in_text(visible_text)
        for email in text_emails:
            cleaned = clean_email(email)
            if cleaned:
                found_emails.add(cleaned)

        # HTML attributes that might contain emails
        for element in soup.find_all(attrs={"data-email": True}):
            email = element.get("data-email")
            cleaned = clean_email(email)
            if cleaned:
                found_emails.add(cleaned)
        
        # obfuscated emails (like "name [at] domain [dot] com")
        obfuscated_pattern = r'\b[a-zA-Z0-9._+-]+\s*\[at\]\s*[a-zA-Z0-9.-]+\s*\[dot\]\s*[a-zA-Z]{2,}\b'
        obfuscated_emails = re.findall(obfuscated_pattern, visible_text, re.IGNORECASE)
        for email in obfuscated_emails:
            # convert to normal email
            normal_email = re.sub(r'\s*\[at\]\s*', '@', email, flags=re.IGNORECASE)
            normal_email = re.sub(r'\s*\[dot\]\s*', '.', normal_email, flags=re.IGNORECASE)
            cleaned = clean_email(normal_email)
            if cleaned:
                found_emails.add(cleaned)

        return list(found_emails)

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed for {url}: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to scrape {url}: {e}")
        return []

def is_valid_email(email):
    if not email or len(email) > 254:  # RFC 5321 limit
        return False
    
    # basic format check
    pattern = r'^[a-zA-Z0-9]([a-zA-Z0-9._+-]*[a-zA-Z0-9])?@[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False
    
    # Additional checks
    local, domain = email.split('@')
    
    # Local part checks
    if len(local) > 64 or len(local) == 0:  # RFC 5321 limit
        return False
    if local.startswith('.') or local.endswith('.'):
        return False
    if '..' in local:
        return False
    
    # Domain part checks
    if len(domain) > 253 or len(domain) == 0:
        return False
    if domain.startswith('.') or domain.endswith('.'):
        return False
    if '..' in domain:
        return False
    
    # on-email patterns
    invalid_patterns = [
        r'.*\.(png|jpg|jpeg|gif|pdf|doc|docx|zip|exe)$',  # File extensions
        r'.*@(example|test|localhost|127\.0\.0\.1)',      # Test domains
        r'.*@.*\.(local|test|example)$',                   # Test TLDs
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, email, re.IGNORECASE):
            return False
    
    return True

def find_contact_links(base_url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(base_url, timeout=15, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        links = set()

        # links in navigation, footer, throughout page
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            link_text = a.get_text().lower()
            
            # href and link text for contact keywords
            for keyword in CONTACT_PATHS:
                if keyword in href or keyword in link_text:
                    full_url = urljoin(base_url, a['href'])
                    links.add(full_url)
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
    urls_to_scrape = set()

    # exact URL
    urls_to_scrape.add(website)
    
    # base homepage
    if website != base_url:
        urls_to_scrape.add(base_url)

    # add contact pages
    contact_links = find_contact_links(base_url)
    urls_to_scrape.update(contact_links[:5])  # first 5 contact pages

    # scrape each URL
    for url in urls_to_scrape:
        print(f"  → Scraping: {url}")
        emails = scrape_emails_from_url(url)
        found.update(emails)
        time.sleep(1) 

    # validity
    valid_emails = [email for email in found if is_valid_email(email)]
    
    return list(set(valid_emails))

def run_email_scraper_on_companies(start_company_id=None):
    query = supabase.table("companies").select("company_id, company_name, website").order("company_id")
    
    if start_company_id:
        query = query.gt("company_id", start_company_id)

    response = query.execute()
    rows = response.data

    for row in rows:
        company_id = row['company_id']
        name = row['company_name']
        website = row.get('website')

        if not website:
            print(f"[SKIP] No website for {name}")
            continue

        print(f"Scraping {company_id} {name} ({website})")
        emails = extract_emails_from_website(website)

        if emails:
            # avoid duplicates
            existing_contacts = supabase.table("contacts").select("email_address").eq("company_id", company_id).execute().data
            existing_emails = {c['email_address'].lower() for c in existing_contacts if c.get('email_address')}

            new_emails_added = 0
            for email in emails:
                if email in existing_emails:
                    print(f"[SKIP] Email already exists: {email}")
                    continue

                try:
                    supabase.table("contacts").insert({
                        "company_name": name,
                        "company_id": company_id,
                        "email_address": email
                    }).execute()
                    print(f"[ADDED] {email} for {name}")
                    new_emails_added += 1
                except Exception as e:
                    print(f"[ERROR] Failed to insert {email}: {e}")
            
            if new_emails_added == 0:
                print(f"[INFO] No new emails added for {name} (all already existed)")
        else:
            print(f"[SKIP] No emails found for {name}")

        time.sleep(2)

if __name__ == "__main__":
    run_email_scraper_on_companies(start_company_id=start_company_id)