import json
import os
import time
import sys
from collections import defaultdict
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv
from supabase import create_client, Client
from anthropic import Anthropic
from io import BytesIO

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, ANTHROPIC_API_KEY]):
    print("❌ Missing required environment variables:")
    if not SUPABASE_URL: print("  - SUPABASE_URL")
    if not SUPABASE_KEY: print("  - SUPABASE_KEY") 
    if not ANTHROPIC_API_KEY: print("  - ANTHROPIC_API_KEY")
    sys.exit(1)

OUTREACH_PERSON = os.getenv("OUTREACH_PERSON", "Technology Consulting Association (TCA)")
ONE_PER_CONTACT = os.getenv("ONE_PER_CONTACT", "false").lower() == "true"

def get_int_env(var_name: str, default: int, min_val: int = 0) -> int:
    """Get integer from environment with validation."""
    try:
        value = int(os.getenv(var_name, str(default)))
        if value < min_val:
            print(f"⚠️  {var_name}={value} is below minimum {min_val}, using {min_val}")
            return min_val
        return value
    except ValueError:
        print(f"⚠️  Invalid {var_name} value, using default: {default}")
        return default

MAX_EMAILS = get_int_env("MAX_EMAILS", 10)
START_COMPANY_ID = get_int_env("START_COMPANY_ID", 0)

MODEL = "claude-sonnet-4-20250514"
# "claude-opus-4-1-20250805"


# --- Email template pieces ----------------------------------------------------

TEMPLATE_TOP = """Hi {salutation},

I'm reaching out on behalf of the Technology Consulting Association (TCA) – a pro bono consulting group at the University of Washington dedicated to helping businesses streamline operations and accelerate growth through innovative technological solutions. {personalized}

Our members bring a diverse skillset across tech and business to deliver real, industry-ready results that can {relate}.
"""

SERVICES_BLOCK = """Our services include:
• Integrating AI features
• Implementing scalable cloud infrastructure
• Designing internal dashboards & web tools
• Analyzing product usage data
• Conducting market/competitor research
"""

LINKS_BLOCK = """
<p>
  <a href="https://www.linkedin.com/company/tca-uw/">LinkedIn</a> | 
  <a href="https://uwtechconsulting.com/">TCA Website</a> | 
  <a href="https://drive.google.com/file/d/1ADugKcdcinckR0r2pBXZt9khNCAuxxy1/view?usp=sharing">Partnership Guide</a>
</p>
"""

TEMPLATE_BOTTOM = """I’ve attached our partnership guide, which gives more detail on how we operate, 
what we offer, and past work. If you are open to a 15–20 minute conversation, we’d appreciate the 
chance to learn more about your goals and discuss how our student consultants might be of help. 
We understand you are very busy, so we’re happy to work around your schedule. We are looking 
forward to hearing from you!

Sincerely,
{outreach_person}
Outreach Director, Technology Consulting Association
outreach@uwtechconsulting.com 
"""

SUBJECT_TEMPLATE = "UW Technology Consulting - Discovery Meeting"

SYSTEM_INSTRUCTIONS = (
    "You are an educated, professional-sounding college student outreach director for a consulting "
    "club who thoroughly researchs each company before reaching out. "
    "Our club is the Technology Consulting Association (TCA) at the University of Washington."
    "Our mission is: Empowering businesses to unlock smarter operations and next-level efficiency "
    "through innovative technological solutions (don't use the word mission anywhere in your sentence)." 
    "These are the services we offer: "
    "AI Integration: Integrate lightweight AI tools and automation flows to enhance decision-making and efficiency"
    "Full-Stack Dev: Build scalable, user-friendly applications, pairing intuitive design with robust backends"
    "Cloud Computing: Design efficient cloud infrastructure with effortless scaling and optimized performance "
    "Data Analysis: Visualize data, identify patterns, and surface insights to inform strategy and support decisions."
    "System Design: Employ fault tolerant system architecture built for reliability and seamless integration. "
    "Market Research: Uncover trends, competitor strategies, and growth opportunities through tailored research and market analysis"
    "You must return STRICT JSON with keys "
    "`personalized` and `relate` only. No extra text.\n\n"
    "`personalized` = 2 sentences:\n"
    " - Sentence 1: A personal, professional, but friendly sentence referencing a unique fact of "
    " the company using the website and relevant outside sources without sounding scripted and cliche and without purple prose.\n"
    " - Sentence 2: A bridge relating the company's goals with TCA. Don't sound cheesy.\n"
    "Keep it specific, friendly, concise, personal, and professional.\n\n"
    "`relate` = a single concise clause (10–18 words) naming 2–3 concrete outcomes relevant to the company; "
    "this clause will follow the words 'can ' in a sentence, so do not start with a gerund.\n"
)

USER_TEMPLATE = (
    "Company: {company_name}\n"
    "Description (may be empty): {description}\n"
    "Website (may be empty): {website}\n"
    "Tone: helpful, professional, personal, personalized. Output JSON only."
)

# supabase

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
anth = Anthropic(api_key=ANTHROPIC_API_KEY)

def fetch_companies() -> List[Dict]:
    """Fetch companies with deterministic ordering, filtered by START_COMPANY_ID."""
    try:
        q = (sb.table("companies")
               .select("company_id, company_name, description, website")
               .gte("company_id", START_COMPANY_ID)
               .order("company_id", desc=False))
        result = q.execute()
        print(f"📊 Fetched {len(result.data)} companies (company_id >= {START_COMPANY_ID})")
        return result.data
    except Exception as e:
        print(f"❌ Error fetching companies: {e}")
        return []

def fetch_contacts() -> List[Dict]:
    """Fetch contacts with deterministic ordering."""
    try:
        q = (sb.table("contacts")
               .select("contact_id, company_id, company_name, contact_name, email_address, contact_title, contact_linkedin_url")
               .order("company_id", desc=False)
               .order("contact_id", desc=False))
        result = q.execute()
        print(f"📊 Fetched {len(result.data)} contacts")
        return result.data
    except Exception as e:
        print(f"❌ Error fetching contacts: {e}")
        return []

def existing_draft_exists(company_id: int, contact_name: Optional[str] = None) -> bool:
    """Check if a draft already exists for this company/contact combination."""
    try:
        rows = (
            sb.table("emails")
            .select("email_id, subject, status, body")
            .eq("company_id", company_id)
            .eq("status", "draft")
        ).execute().data
        
        if not rows:
            return False
        if not contact_name:
            return True
        for r in rows:
            if r.get("body") and contact_name in r["body"]:
                return True
        return False
    except Exception as e:
        print(f"⚠️  Error checking existing drafts for company {company_id}: {e}")
        return False

def upsert_email(company_id: int, subject: str, body: str) -> bool:
    """Insert email draft into database."""
    try:
        sb.table("emails").insert({
            "status": "draft",
            "company_id": company_id,
            "subject": subject,
            "body": body,
            "sent_at": None,
            "replied_at": None,
            "outreach_person": OUTREACH_PERSON,
        }).execute()
        return True
    except Exception as e:
        print(f"❌ Error inserting email for company {company_id}: {e}")
        return False

# batch processing

def get_salutation(contact: Optional[Dict], company_name: str) -> str:
    """Generate appropriate salutation for email."""
    if contact and contact.get("contact_name"):
        name = contact["contact_name"].strip()
        return name if name else f"{company_name} Team"
    return f"{company_name} Team"

def build_batch_items(companies: List[Dict], contacts_by_company: Dict[int, List[Dict]]) -> List[Tuple[str, Dict, Dict]]:
    """
    Build batch items for processing.
    Returns: List of (custom_id, api_payload, render_context) tuples
    """
    items = []
    processed_count = 0
    
    for comp in companies:
        if MAX_EMAILS > 0 and processed_count >= MAX_EMAILS:
            print(f"🛑 Reached MAX_EMAILS limit of {MAX_EMAILS}")
            break
            
        cid = comp["company_id"]
        cname = comp["company_name"]
        desc = comp.get("description") or ""
        site = comp.get("website") or ""

        targets = contacts_by_company.get(cid, [])
        iterator = targets if (ONE_PER_CONTACT and targets) else [None]

        for contact in iterator:
            if MAX_EMAILS > 0 and processed_count >= MAX_EMAILS:
                break
                
            contact_name = contact["contact_name"] if contact else None
            
            # Skip if draft already exists
            if existing_draft_exists(cid, contact_name):
                print(f"⏭️  Skipping company {cid} ({cname}) - draft already exists")
                continue

            custom_id = f"cid{cid}" + (f"-contact-{contact['contact_id']}" if contact else "-company")

            user_content = USER_TEMPLATE.format(
                company_name=cname, 
                description=desc, 
                website=site
            )

            payload = {
                "model": MODEL,
                "max_tokens": 240,
                "temperature": 0.4,
                "system": SYSTEM_INSTRUCTIONS,
                "messages": [{"role": "user", "content": user_content}]
            }

            render_ctx = {
                "company_id": cid,
                "company_name": cname,
                "contact_name": contact_name,
                "salutation": get_salutation(contact, cname),
            }

            items.append((custom_id, payload, render_ctx))
            processed_count += 1
            
    print(f"📝 Prepared {len(items)} batch items for processing")
    return items

def create_batch_file(items: List[Tuple[str, Dict, Dict]]) -> BytesIO:
    """Create JSONL file for batch processing."""
    buf = BytesIO()
    for custom_id, params, _ in items:
        line = {"custom_id": custom_id, "params": params}
        buf.write((json.dumps(line) + "\n").encode("utf-8"))
    buf.seek(0)
    return buf

def submit_batch(items: List[Tuple[str, Dict, Dict]]):
    """Submit batch to Anthropic Message Batches API."""
    try:
        # Check if batch processing is available
        if not hasattr(anth, 'beta') or not hasattr(anth.beta, 'messages') or not hasattr(anth.beta.messages, 'batches'):
            raise RuntimeError("Message Batches API requires anthropic SDK with beta.messages.batches support")
        
        # Convert items to the correct batch format
        requests = []
        for custom_id, params, _ in items:
            requests.append({
                "custom_id": custom_id,
                "params": params
            })
            
        batch = anth.beta.messages.batches.create(
            requests=requests
        )
        return batch
    except Exception as e:
        print(f"❌ Error submitting batch: {e}")
        print(f"💡 Make sure you have the latest anthropic SDK: pip install --upgrade anthropic")
        raise

def poll_batch_completion(batch_id: str, sleep_secs: int = 10, max_polls: int = 120):
    """Poll batch until completion with timeout."""
    print(f"⏳ Polling batch {batch_id} (max {max_polls} attempts, {sleep_secs}s intervals)")
    
    for attempt in range(max_polls):
        try:
            batch = anth.beta.messages.batches.retrieve(batch_id)
            status = batch.processing_status
            
            if attempt % 6 == 0 or status in ("ended", "failed", "expired", "canceled"):
                print(f"📊 Poll {attempt + 1}/{max_polls}: status={status}")
                
            if status == "ended":
                return batch
            elif status in ("failed", "expired", "canceled"):
                raise RuntimeError(f"Batch processing failed with status: {status}")
                
            time.sleep(sleep_secs)
        except Exception as e:
            print(f"❌ Error polling batch: {e}")
            if attempt < max_polls - 1:
                time.sleep(sleep_secs)
                continue
            raise
    
    raise TimeoutError(f"Batch did not complete within {max_polls * sleep_secs} seconds")

def download_and_parse_results(batch) -> Tuple[Dict[str, Dict], Dict[str, str]]:
    """Download batch results and parse them."""
    try:
        # Get results directly from the batch object
        if not hasattr(batch, 'results_url') or not batch.results_url:
            raise RuntimeError("Batch has no results_url - batch may not have completed successfully")
            
        # Stream results from the results URL
        results_stream = anth.beta.messages.batches.results(batch.id)
        
        results = {}
        failures = {}
        
        # Process each result from the stream
        for result_obj in results_stream:
            custom_id = result_obj.custom_id
            result = result_obj.result
            
            if result.type == "succeeded":
                # Extract the message content
                content = result.message.content
                text = "".join(
                    block.text for block in content 
                    if hasattr(block, 'text')
                ).strip()
                
                try:
                    data = json.loads(text)
                    personalized = data.get("personalized", "").strip()
                    relate = data.get("relate", "").strip().rstrip(" .")
                    results[custom_id] = {
                        "personalized": personalized, 
                        "relate": relate
                    }
                except json.JSONDecodeError:
                    failures[custom_id] = f"Invalid JSON output: {text[:100]}..."
            else:
                # Handle errors, cancellations, expirations
                error_msg = getattr(result, 'error', f"Request {result.type}")
                failures[custom_id] = f"{result.type}: {error_msg}"
        
        print(f"✅ Parsed {len(results)} successful results, {len(failures)} failures")
        return results, failures
        
    except Exception as e:
        print(f"❌ Error downloading/parsing results: {e}")
        raise

def compose_email_body(salutation: str, personalized: str, relate: str) -> str:
    """Compose the final email body."""
    top = TEMPLATE_TOP.format(
        salutation=salutation, 
        personalized=personalized, 
        relate=relate
    )
    bottom = TEMPLATE_BOTTOM.format(outreach_person=OUTREACH_PERSON)
    return f"{top}\n{SERVICES_BLOCK}\n{bottom}"

def save_email_drafts(results: Dict[str, Dict], context_by_id: Dict[str, Dict]) -> Tuple[int, int]:
    """Save generated emails to database."""
    drafted = 0
    skipped = 0
    
    for custom_id, content in results.items():
        ctx = context_by_id.get(custom_id)
        if not ctx:
            print(f"⚠️  Missing context for {custom_id}, skipping")
            continue
            
        company_id = ctx["company_id"]
        company_name = ctx["company_name"]
        contact_name = ctx["contact_name"]
        
        # Double-check for duplicates (race condition protection)
        if existing_draft_exists(company_id, contact_name):
            skipped += 1
            continue
        
        subject = SUBJECT_TEMPLATE.format(company_name=company_name)
        body = compose_email_body(
            ctx["salutation"], 
            content["personalized"], 
            content["relate"]
        )
        
        if upsert_email(company_id, subject, body):
            drafted += 1
            print(f"✅ Created draft for {company_name} (ID: {company_id})")
        else:
            print(f"❌ Failed to save draft for {company_name} (ID: {company_id})")
    
    return drafted, skipped

def main():
    """Main execution function."""
    print("🚀 Starting TCA Email Generation")
    print(f"📋 Configuration:")
    print(f"   • START_COMPANY_ID: {START_COMPANY_ID}")
    print(f"   • MAX_EMAILS: {MAX_EMAILS if MAX_EMAILS > 0 else 'unlimited'}")
    print(f"   • ONE_PER_CONTACT: {ONE_PER_CONTACT}")
    print(f"   • OUTREACH_PERSON: {OUTREACH_PERSON}")
    print()
    
    # Fetch data
    companies = fetch_companies()
    if not companies:
        print("❌ No companies found. Exiting.")
        return
        
    contacts = fetch_contacts()
    contacts_by_company = defaultdict(list)
    for contact in contacts:
        contacts_by_company[contact["company_id"]].append(contact)
    
    # Build batch items
    items = build_batch_items(companies, contacts_by_company)
    if not items:
        print("✅ No new emails to generate (all targets already have drafts or MAX_EMAILS is 0)")
        return
    
    print(f"📤 Submitting batch of {len(items)} items...")
    
    # Process batch
    try:
        batch = submit_batch(items)
        print(f"📨 Batch submitted: {batch.id}")
        print(f"📊 Initial status: {batch.processing_status}")
        
        # Poll for completion
        completed_batch = poll_batch_completion(batch.id)
        print(f"🎉 Batch ended successfully!")
        
        # Download and parse results
        results, failures = download_and_parse_results(completed_batch)
        
        # Save to database
        context_by_id = {custom_id: ctx for custom_id, _, ctx in items}
        drafted, skipped = save_email_drafts(results, context_by_id)
        
        # Summary
        print("\n📊 Final Results:")
        print(f"   • Drafts created: {drafted}")
        print(f"   • Skipped (duplicates): {skipped}")
        print(f"   • Failed generations: {len(failures)}")
        
        if failures:
            print("\n❌ Failures (showing first 5):")
            for custom_id, error in list(failures.items())[:5]:
                print(f"   • {custom_id}: {error}")
        
        print(f"\n✅ Email generation complete!")
        
    except Exception as e:
        print(f"❌ Fatal error during batch processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()