# outreach_dashboard.py
# UI: generate -> edit -> send, with valid-contact filtering
# pip install PySide6 supabase python-dotenv anthropic dnspython

import os, sys, threading, traceback, html, json
from typing import Dict, Any, List, Optional, Tuple
from functools import lru_cache

from dotenv import load_dotenv
load_dotenv()

ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
ANTHROPIC_FALLBACKS = [
    "claude-3-haiku-20240307",
    "claude-3-sonnet-20240229",
    "claude-3-opus-20240229"
]

from PySide6.QtCore import Qt, QObject, Signal
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QTableWidget, QTableWidgetItem, QHeaderView,
    QListWidget, QListWidgetItem, QTextEdit, QLineEdit, QMessageBox, QSplitter,
    QGroupBox
)

# ---------- ENV & external deps ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL")
FROM_NAME  = os.getenv("FROM_NAME", "Technology Consulting Association (TCA)")
REPLY_TO   = os.getenv("REPLY_TO", FROM_EMAIL)
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"

# Safe defaults for small UI actions
OUTREACH_PERSON = os.getenv("OUTREACH_PERSON", "Technology Consulting Association (TCA)")

# ---------- Soft imports (don’t crash UI) ----------
SUPABASE_OK = True
try:
    from db_connect import supabase          # must expose a client named 'supabase'
except Exception as e:
    SUPABASE_OK = False
    supabase = None
    SUPABASE_ERR = "".join(traceback.format_exception_only(type(e), e))

try:
    from anthropic import Anthropic
    anth = Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
    ANTH_OK = anth is not None
except Exception as e:
    anth = None
    ANTH_OK = False
    ANTH_ERR = "".join(traceback.format_exception_only(type(e), e))

try:
    from email_send import smtp_send         # must expose smtp_send(to, subj, text, body_html=None)
    SENDER_OK = True
except Exception as e:
    smtp_send = None
    SENDER_OK = False
    SENDER_ERR = "".join(traceback.format_exception_only(type(e), e))

# Optional: your strong validator
try:
    from email_check import validate_email_comprehensive
    VALIDATOR_OK = True
except Exception:
    validate_email_comprehensive = None
    VALIDATOR_OK = False

# Fallback DNS deliverability check
@lru_cache(maxsize=4096)
def _fallback_deliverable(addr: str) -> bool:
    try:
        import re, dns.resolver, dns.exception
        if not addr or "@" not in addr: return False
        if not re.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z0-9.-]+$", addr): return False
        domain = addr.split("@",1)[1]
        try: idna = domain.encode("idna").decode("ascii")
        except Exception: return False
        r = dns.resolver.Resolver(configure=True); r.timeout = 2.0; r.lifetime = 3.0
        try:
            if r.resolve(idna, "MX"): return True
        except dns.exception.DNSException:
            pass
        for rr in ("A","AAAA"):
            try:
                if r.resolve(idna, rr): return True
            except dns.exception.DNSException:
                continue
        return False
    except Exception:
        return False

@lru_cache(maxsize=4096)
def is_valid_email(addr: str) -> bool:
    if not addr: return False
    if VALIDATOR_OK and callable(validate_email_comprehensive):
        try:
            verdict, _ = validate_email_comprehensive(addr)
            return verdict == "valid"
        except Exception:
            pass
    return _fallback_deliverable(addr)

# ---------- Your copy-pasted email generation templates ----------
def html_escape(s: Optional[str]) -> str:
    return html.escape(s or "")

def anti_trim(s: str) -> str:
    if not s: return s
    s = s.replace("Our services include", "Our ser\u200Bvices include")
    s = s.replace("Sincerely", "Sincere\u200Bly")
    s = s.replace("Technology Consulting Association", "Technology Consulting Associ\u200Bation")
    s = s.replace("outreach@uwtechconsulting.com", "outreach\u200B@uwtechconsulting.com")
    return s

WRAPPER_OPEN = (
    '<div style="font-family:Arial,Helvetica,sans-serif;'
    'font-size:14px; line-height:1.6; color:#202124;">'
)
WRAPPER_CLOSE = "</div>"

TEMPLATE_TOP = """\
<p style="margin:0 0 12px 0;">Hi {salutation},</p>

<p style="margin:0 0 12px 0;">I’m reaching out on behalf of the Technology Consulting Association (TCA) – a pro bono consulting group at the University of Washington dedicated to helping businesses streamline operations and accelerate growth through innovative technological solutions. {personalized}</p>

<p style="margin:0 0 18px 0;">Our members bring a diverse skillset across tech and business to deliver real, industry-ready results that can {relate}.</p>
"""

SERVICES_BLOCK = """\
<p style="margin:0 0 8px 0;"><strong style="color:#202124;">Our services include:</strong></p>
<ul style="margin:0 0 18px 18px; padding:0;">
  <li style="margin:0 6px 0 0;"><strong style="color:#202124;">Integrating AI features</strong></li>
  <li style="margin:0 6px 0 0;"><strong style="color:#202124;">Implementing scalable cloud infrastructure</strong></li>
  <li style="margin:0 6px 0 0;"><strong style="color:#202124;">Designing internal dashboards &amp; web tools</strong></li>
  <li style="margin:0 6px 0 0;"><strong style="color:#202124;">Analyzing product usage data</strong></li>
  <li style="margin:0 6px 0 0;"><strong style="color:#202124;">Conducting market/competitor research</strong></li>
</ul>
"""

def link(href: str, text: str) -> str:
    return f'<a href="{html_escape(href)}" style="color:#1a73e8; text-decoration:underline;">{html_escape(text)}</a>'

LINKS_BLOCK = " | ".join([
    link("https://www.linkedin.com/company/tca-uw/", "LinkedIn"),
    link("https://uwtechconsulting.com/", "TCA Website"),
    link("https://drive.google.com/file/d/1ADugKcdcinckR0r2pBXZt9khNCAuxxy1/view?usp=sharing", "Partnership Guide"),
])

TEMPLATE_BOTTOM = """\
<p style="margin:0 0 18px 0;">I’ve attached our <strong>partnership guide</strong>, which gives more detail on how we operate, what we offer, and past work. If you are open to a 15–20 minute conversation, we’d appreciate the chance to learn more about your goals and discuss how our student consultants might be of help. We understand you are very busy, so we’re happy to work around your schedule. We are looking forward to hearing from you!</p>

<p style="margin:0 0 4px 0;">Sincerely,</p>
<p style="margin:0 0 2px 0;">{outreach_person}</p>
<p style="margin:0 0 2px 0;">Outreach Director, Technology Consulting Association</p>
<p style="margin:0 0 14px 0;"><a href="mailto:outreach@uwtechconsulting.com" style="color:#1a73e8; text-decoration:underline;">outreach@uwtechconsulting.com</a></p>

<p style="margin:0;">
  {links}
</p>
"""

SUBJECT_TEMPLATE = "UW Technology Consulting - Discovery Meeting"

SYSTEM_INSTRUCTIONS = (
    "You are an educated, professional-sounding college student outreach director for a consulting "
    "club who thoroughly researches each company before reaching out. "
    "Our club is the Technology Consulting Association (TCA) at the University of Washington. "
    "Our mission is: Empowering businesses to unlock smarter operations and next-level efficiency "
    "through innovative technological solutions (don't use the word mission anywhere in your sentence). "
    "These are the services we offer: "
    "AI Integration: Integrate lightweight AI tools and automation flows to enhance decision-making and efficiency. "
    "Full-Stack Dev: Build scalable, user-friendly applications, pairing intuitive design with robust backends. "
    "Cloud Computing: Design efficient cloud infrastructure with effortless scaling and optimized performance. "
    "Data Analysis: Visualize data, identify patterns, and surface insights to inform strategy and support decisions. "
    "System Design: Employ fault tolerant system architecture built for reliability and seamless integration. "
    "Market Research: Uncover trends, competitor strategies, and growth opportunities through tailored research and market analysis. "
    "\n\n"
    "You MUST return ONLY valid JSON with exactly these keys: 'personalized' and 'relate'. "
    "Do not include any other text before or after the JSON. "
    "Do not wrap the JSON in markdown code blocks. "
    "RULES:"
    "First sentence (Why them): Compliment something specific and factual about their business - their mission, unique concept, recent achievement, or interesting approach. Be authentic, not generic. "
    "Second sentence (Why us): Connect their uniqueness to technology opportunities. Use phrases like 'we see opportunities to...' or 'we could help you explore...'. "
    "Connection: The two sentences must flow together logically. "
    "No assumptions: Don't assume they have problems. "
    "Avoid long, wordy sentences and em-dashes. 30 words max per sentence. "
    "Example format: {\"personalized\": \"...\", \"relate\": \"...\"}\n\n"
)

USER_TEMPLATE = (
    "Company: {company_name}\n"
    "Description (may be empty): {description}\n"
    "Website (may be empty): {website}\n"
    "Tone: helpful, professional, personal, personalized. Output JSON only."
)

def compose_email_body(salutation: str, personalized: str, relate: str) -> str:
    top = TEMPLATE_TOP.format(
        salutation=html_escape(salutation),
        personalized=html_escape(personalized),
        relate=html_escape(relate),
    )
    bottom = TEMPLATE_BOTTOM.format(
        outreach_person=html_escape(OUTREACH_PERSON),
        links=LINKS_BLOCK
    )
    html_body = WRAPPER_OPEN + top + SERVICES_BLOCK + bottom + WRAPPER_CLOSE
    return anti_trim(html_body)

# ---------- Supabase helpers ----------
def sb_companies() -> List[Dict[str, Any]]:
    if not SUPABASE_OK:
        raise RuntimeError(f"Supabase import failed:\n{SUPABASE_ERR}")
    return (supabase.table("companies")
            .select("company_id, company_name, website, description")
            .order("company_id", desc=False)
            ).execute().data or []

def sb_contacts(company_id: int) -> List[Dict[str, Any]]:
    if not SUPABASE_OK:
        raise RuntimeError(f"Supabase import failed:\n{SUPABASE_ERR}")
    rows = (supabase.table("contacts")
            .select("contact_id, contact_name, contact_title, email_address")
            .eq("company_id", company_id)
            .order("contact_id", desc=False)).execute().data or []
    # filter valid emails only
    out = []
    for r in rows:
        e = (r.get("email_address") or "").strip()
        if e and is_valid_email(e):
            r["email_address"] = e
            out.append(r)
    return out

def sb_drafts(company_id: int) -> List[Dict[str, Any]]:
    if not SUPABASE_OK:
        raise RuntimeError(f"Supabase import failed:\n{SUPABASE_ERR}")
    return (supabase.table("emails")
            .select("email_id, company_id, subject, body, status")
            .eq("company_id", company_id)
            .eq("status", "draft")
            .order("email_id", desc=False)
            ).execute().data or []

def sb_insert_draft(company_id: int, subject: str, body: str) -> int:
    if not SUPABASE_OK:
        raise RuntimeError(f"Supabase import failed:\n{SUPABASE_ERR}")
    res = supabase.table("emails").insert({
        "status": "draft",
        "company_id": company_id,
        "subject": subject,
        "body": body,
        "sent_at": None,
        "replied_at": None,
        "outreach_person": OUTREACH_PERSON,
    }).execute()
    # try to return id
    try:
        if res.data and "email_id" in res.data[0]:
            return res.data[0]["email_id"]
    except Exception:
        pass
    row = (supabase.table("emails")
           .select("email_id").eq("company_id", company_id)
           .eq("status", "draft")
           .order("email_id", desc=True)
           .limit(1).single().execute().data)
    return row["email_id"]

def sb_update_draft(email_id: int, subject: str, body: str) -> None:
    if not SUPABASE_OK:
        raise RuntimeError(f"Supabase import failed:\n{SUPABASE_ERR}")
    supabase.table("emails").update({"subject": subject, "body": body}).eq("email_id", email_id).execute()

def sb_mark_sent(email_id: int) -> None:
    if not SUPABASE_OK:
        raise RuntimeError(f"Supabase import failed:\n{SUPABASE_ERR}")
    supabase.table("emails").update({"status": "sent"}).eq("email_id", email_id).execute()

# ---------- Anthropic one-off generation ----------
def anthropic_generate_for_company(company: Dict[str, Any]) -> Tuple[str, str]:
    """
    Returns (subject, html_body). Tries ANTHROPIC_MODEL then fallbacks on 404.
    """
    if not ANTH_OK:
        raise RuntimeError("Anthropic client not available (check ANTHROPIC_API_KEY and install).")

    cname = company.get("company_name", "")
    desc  = company.get("description", "") or ""
    site  = company.get("website", "") or ""
    salutation = f"{cname} Team"

    user_prompt = USER_TEMPLATE.format(company_name=cname, description=desc, website=site)

    # Try primary + fallbacks
    tried = []
    models_to_try = [ANTHROPIC_MODEL] + [m for m in ANTHROPIC_FALLBACKS if m != ANTHROPIC_MODEL]

    last_err = None
    for mdl in models_to_try:
        tried.append(mdl)
        try:
            msg = anth.messages.create(
                model=mdl,
                max_tokens=240,
                temperature=0.4,
                system=SYSTEM_INSTRUCTIONS,
                messages=[{"role": "user", "content": user_prompt}],
            )

            text = "".join(getattr(b, "text", "") for b in getattr(msg, "content", [])).strip()
            if text.startswith("```"):
                lines = text.splitlines()
                if len(lines) >= 2:
                    text = "\n".join(lines[1:-1]).strip()
            if "{" in text and "}" in text:
                text = text[text.find("{"): text.rfind("}")+1]

            data = json.loads(text)
            personalized = (data.get("personalized") or "").strip()
            relate = (data.get("relate") or "").strip().rstrip(" .")
            if not personalized or not relate:
                raise RuntimeError(f"Model returned missing fields: {data}")

            subject = SUBJECT_TEMPLATE
            body_html = compose_email_body(salutation, personalized, relate)
            return subject, body_html

        except Exception as e:
            # If it's a not-found error, try next model; otherwise raise
            msg = str(e).lower()
            last_err = e
            if "not_found" in msg or "not found" in msg or "model" in msg and "404" in msg:
                continue
            raise

    # If we exhausted all models
    raise RuntimeError(
        "All Anthropic model attempts failed.\n"
        f"Tried: {', '.join(tried)}\n"
        f"Last error: {last_err}"
    )

# ---------- Thread helper ----------
class Worker(QObject):
    finished = Signal(object, object)  # (result, error)
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn, self.args, self.kwargs = fn, args, kwargs
    def run(self):
        try:
            self.finished.emit(self.fn(*self.args, **self.kwargs), None)
        except Exception as e:
            self.finished.emit(None, e)

def run_in_thread(fn, cb, *args, **kwargs):
    w = Worker(fn, *args, **kwargs)
    w.finished.connect(cb)
    t = threading.Thread(target=w.run, daemon=True)
    t.start()

def exstr(e: Exception) -> str:
    return "".join(traceback.format_exception(type(e), e, e.__traceback__))

# ---------- UI tabs ----------
class CompaniesTab(QWidget):
    company_selected = Signal(dict)
    def __init__(self, log: QTextEdit):
        super().__init__()
        self.log = log
        self.rows: List[Dict[str, Any]] = []

        lay = QVBoxLayout(self)
        top = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.diag_btn = QPushButton("Diagnostics")
        self.refresh_btn.clicked.connect(self.refresh)
        self.diag_btn.clicked.connect(self.diagnostics)
        top.addWidget(self.refresh_btn); top.addWidget(self.diag_btn); top.addStretch(1)
        lay.addLayout(top)

        self.table = QTableWidget(0,4)
        self.table.setHorizontalHeaderLabels(["ID","Name","Website","Description"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.doubleClicked.connect(self.emit_selected)
        lay.addWidget(self.table, 1)

    def refresh(self):
        self.refresh_btn.setDisabled(True)
        run_in_thread(sb_companies, self._refreshed)

    def _refreshed(self, res, err):
        self.refresh_btn.setDisabled(False)
        if err:
            QMessageBox.critical(self, "Error", str(err))
            self.log.append("❌ Companies refresh failed:\n" + exstr(err)); return
        self.rows = res or []
        self.table.setRowCount(len(self.rows))
        for r, row in enumerate(self.rows):
            self.table.setItem(r,0,QTableWidgetItem(str(row["company_id"])))
            self.table.setItem(r,1,QTableWidgetItem(row.get("company_name","")))
            self.table.setItem(r,2,QTableWidgetItem(row.get("website","") or ""))
            self.table.setItem(r,3,QTableWidgetItem(row.get("description","") or ""))
        self.log.append(f"✅ Loaded {len(self.rows)} companies.")

    def emit_selected(self):
        idx = self.table.currentRow()
        if 0 <= idx < len(self.rows):
            self.company_selected.emit(self.rows[idx])

    def diagnostics(self):
        msgs = [
            f"Supabase: {'OK' if SUPABASE_OK else 'ERROR'}",
            f"Anthropic: {'OK' if ANTH_OK else 'MISSING'}",
            f"Sender (smtp_send): {'OK' if SENDER_OK else 'MISSING'}",
            f"Validator: {'OK' if VALIDATOR_OK else 'fallback DNS'}",
        ]
        if not SUPABASE_OK: msgs.append(SUPABASE_ERR)
        if not ANTH_OK: msgs.append("Check ANTHROPIC_API_KEY and `pip install anthropic`")
        if not SENDER_OK: msgs.append(SENDER_ERR)
        self.log.append("🔎 Diagnostics:\n" + "\n".join(msgs) + "\n")

class ComposeTab(QWidget):
    def __init__(self, log: QTextEdit):
        super().__init__()
        self.log = log
        self.company: Optional[Dict[str, Any]] = None
        self.contacts: List[Dict[str, Any]] = []
        self.drafts: List[Dict[str, Any]] = []
        self.active_draft: Optional[Dict[str, Any]] = None

        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        self.company_label = QLabel("No company selected")
        self.load_btn = QPushButton("Load Contacts/Drafts")
        self.gen_btn  = QPushButton("Generate Draft")   # NEW
        self.save_btn = QPushButton("Save Draft")
        self.send_btn = QPushButton("Send to Selected")

        self.load_btn.clicked.connect(self.load_company_data)
        self.gen_btn.clicked.connect(self.generate_draft_now)
        self.save_btn.clicked.connect(self.save_draft)
        self.send_btn.clicked.connect(self.send_now)
        if not SENDER_OK:
            self.send_btn.setDisabled(True); self.send_btn.setToolTip("email_send.smtp_send not available")

        header.addWidget(self.company_label, 1)
        header.addWidget(self.load_btn)
        header.addWidget(self.gen_btn)
        header.addWidget(self.save_btn)
        header.addWidget(self.send_btn)
        layout.addLayout(header)

        split = QSplitter(Qt.Horizontal)

        left = QWidget(); lv = QVBoxLayout(left)
        lv.addWidget(QLabel("Recipients (valid emails only)"))
        self.contact_list = QListWidget(); lv.addWidget(self.contact_list, 1)
        lv.addWidget(QLabel("Drafts"))
        self.draft_list = QListWidget(); lv.addWidget(self.draft_list, 1)
        self.draft_list.itemSelectionChanged.connect(self._pick_draft)

        right = QWidget(); rv = QVBoxLayout(right)
        editor = QGroupBox("Editor")
        ev = QVBoxLayout(editor)
        self.subj_edit = QLineEdit()
        self.body_edit = QTextEdit()
        ev.addWidget(QLabel("Subject")); ev.addWidget(self.subj_edit)
        ev.addWidget(QLabel("Body (HTML or plain text)")); ev.addWidget(self.body_edit, 1)
        rv.addWidget(editor, 1)

        split.addWidget(left); split.addWidget(right); split.setStretchFactor(1,2)
        layout.addWidget(split, 1)

    def set_company(self, company: Dict[str, Any]):
        self.company = company
        self.company_label.setText(f"{company.get('company_name','(unknown)')} — {company.get('website','')}")
        self.contact_list.clear(); self.draft_list.clear()
        self.subj_edit.clear(); self.body_edit.clear()
        self.contacts, self.drafts, self.active_draft = [], [], None

    def load_company_data(self):
        if not self.company:
            QMessageBox.information(self, "Pick a company", "Select a company first.")
            return
        cid = int(self.company["company_id"])

        def after_contacts(res, err):
            if err:
                QMessageBox.critical(self, "Error", str(err))
                self.log.append("❌ Contacts load failed:\n" + exstr(err)); return
            self.contacts = res or []
            self.contact_list.clear()
            for c in self.contacts:
                label = f"{c.get('contact_name') or '(no name)'}  <{c['email_address']}>"
                it = QListWidgetItem(label)
                it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
                it.setCheckState(Qt.Unchecked)
                it.setData(Qt.UserRole, c)
                self.contact_list.addItem(it)
            run_in_thread(lambda: sb_drafts(cid), after_drafts)

        def after_drafts(res, err):
            if err:
                QMessageBox.critical(self, "Error", str(err))
                self.log.append("❌ Drafts load failed:\n" + exstr(err)); return
            self.drafts = res or []
            self.draft_list.clear()
            for d in self.drafts:
                it = QListWidgetItem(f"#{d['email_id']}  {d.get('subject') or '(no subject)'}")
                it.setData(Qt.UserRole, d)
                self.draft_list.addItem(it)
            self.log.append(f"✅ {len(self.contacts)} valid contact(s); {len(self.drafts)} draft(s).")

        run_in_thread(lambda: sb_contacts(cid), after_contacts)

    # ---- Generate a new draft via Anthropic for the selected company
    def generate_draft_now(self):
        if not self.company:
            QMessageBox.information(self, "Pick a company", "Select a company first.")
            return
        if not ANTH_OK:
            QMessageBox.warning(self, "Anthropic", "ANTHROPIC_API_KEY missing or anthropic not installed.")
            return

        cid = int(self.company["company_id"])

        def done(res, err):
            if err:
                QMessageBox.critical(self, "Generation failed", str(err))
                self.log.append("❌ Generation failure:\n" + exstr(err))
                return
            subj, body_html = res
            # insert to DB as draft
            try:
                new_id = sb_insert_draft(cid, subj, body_html)
                self.log.append(f"✅ Inserted draft #{new_id}")
                # refresh and select it
                self.load_company_data()
                QMessageBox.information(self, "Draft created", f"Draft generated and saved (ID #{new_id}).")
            except Exception as e:
                QMessageBox.critical(self, "DB insert failed", str(e))
                self.log.append("❌ Draft insert failed:\n" + exstr(e))

        run_in_thread(lambda: anthropic_generate_for_company(self.company), done)

    def _pick_draft(self):
        items = self.draft_list.selectedItems()
        if not items: return
        d = items[0].data(Qt.UserRole)
        self.active_draft = d
        self.subj_edit.setText(d.get("subject") or "")
        self.body_edit.setPlainText(d.get("body") or "")

    def save_draft(self):
        if not self.active_draft:
            QMessageBox.information(self, "No draft", "Select a draft first.")
            return
        eid = int(self.active_draft["email_id"])
        subject = self.subj_edit.text().strip()
        body = self.body_edit.toPlainText().strip()
        try:
            sb_update_draft(eid, subject, body)
            self.active_draft["subject"] = subject
            self.active_draft["body"] = body
            QMessageBox.information(self, "Saved", "Draft updated.")
        except Exception as e:
            QMessageBox.critical(self, "Save failed", str(e))
            self.log.append("❌ Save draft failed:\n" + exstr(e))

    def _selected_recipients(self) -> List[Dict[str, Any]]:
        out = []
        for i in range(self.contact_list.count()):
            it = self.contact_list.item(i)
            if it.checkState() == Qt.Checked:
                out.append(it.data(Qt.UserRole))
        return out

    def send_now(self):
        if not self.active_draft:
            QMessageBox.information(self, "No draft", "Select a draft first."); return
        if not SENDER_OK:
            QMessageBox.warning(self, "Sender missing", "email_send.smtp_send not available."); return

        recips = self._selected_recipients()
        if not recips:
            QMessageBox.information(self, "No recipients", "Check at least one recipient."); return

        subj = self.subj_edit.text().strip()
        body = self.body_edit.toPlainText().strip()
        body_html = body if body.lstrip().startswith("<") else None

        sent = 0; errs = []
        for r in recips:
            to = r["email_address"]
            try:
                smtp_send(to, subj, body, body_html=body_html)
                sent += 1
            except Exception as e:
                errs.append(f"{to}: {e}")

        if sent:
            try:
                sb_mark_sent(int(self.active_draft["email_id"]))
            except Exception as e:
                self.log.append("⚠️ Could not mark sent: " + str(e))
            QMessageBox.information(self, "Sent", f"Emails sent: {sent}")
        if errs:
            self.log.append("❌ Some sends failed:\n" + "\n".join(errs))

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Outreach Dashboard (Generate → Edit → Send)")
        self.resize(1200, 820)

        self.log = QTextEdit(); self.log.setReadOnly(True)

        self.tabs = QTabWidget()
        self.companies = CompaniesTab(self.log)
        self.compose   = ComposeTab(self.log)
        self.tabs.addTab(self.companies, "Companies")
        self.tabs.addTab(self.compose,   "Compose & Send")

        central = QWidget(); v = QVBoxLayout(central)
        v.addWidget(self.tabs, 1)
        v.addWidget(QLabel("Log")); v.addWidget(self.log)
        self.setCentralWidget(central)

        self.companies.company_selected.connect(self.on_company_selected)
        self.companies.refresh()

    def on_company_selected(self, company: Dict[str, Any]):
        self.compose.set_company(company)
        self.tabs.setCurrentWidget(self.compose)
        # auto-load contacts/drafts
        self.compose.load_company_data()

def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
