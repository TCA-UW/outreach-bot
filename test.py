import requests
from bs4 import BeautifulSoup, Comment
import re
from urllib.parse import urljoin

COMMON_PATHS = [
    "", "contact", "contact-us", "about", "about-us",
    "team", "support", "help", "staff", "info"
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

def extract_emails_from_text(text):
    return set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text))

def extract_emails_from_html(soup):
    emails = set()

    # From visible text
    text = soup.get_text()
    emails.update(extract_emails_from_text(text))

    # From mailto: links
    for a in soup.find_all('a', href=True):
        if a['href'].startswith("mailto:"):
            email = a['href'][7:].split('?')[0]
            emails.add(email)

    # From form action attributes
    for form in soup.find_all('form', action=True):
        if form['action'].startswith("mailto:"):
            email = form['action'][7:].split('?')[0]
            emails.add(email)

    # From HTML comments
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        emails.update(extract_emails_from_text(comment))

    return emails

def fetch_and_extract_emails(full_url):
    try:
        res = requests.get(full_url, headers=HEADERS, timeout=5)
        if res.status_code != 200:
            return set()
        soup = BeautifulSoup(res.text, 'html.parser')
        return extract_emails_from_html(soup)
    except Exception as e:
        print(f"Error fetching {full_url}: {e}")
        return set()

def scan_site_for_emails(base_url):
    if not base_url.startswith("http"):
        base_url = "https://" + base_url

    all_emails = set()

    print(f"\n🔍 Starting scan for: {base_url}\n")

    for path in COMMON_PATHS:
        full_url = urljoin(base_url, path)
        print(f"➡️  Scanning {full_url} ...")
        emails = fetch_and_extract_emails(full_url)

        if emails:
            print(f"   ✔️ Found: {emails}")
        else:
            print("   ❌ No emails found.")
        
        all_emails.update(emails)

    print("\n✅ Final list of unique emails found:")
    for email in sorted(all_emails):
        print(f" - {email}")

    return all_emails

# Example usage
if __name__ == "__main__":
    domain = "https://www.benbridge.com/"  # Change this to your target
    scan_site_for_emails(domain)
