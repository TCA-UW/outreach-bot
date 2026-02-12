# outreach_dashboard_qt.py
# pip install pyside6 dnspython supabase

import os, sys, threading, subprocess, traceback
from functools import lru_cache
from typing import List, Dict, Any, Optional

from PySide6.QtCore import Qt, Signal, QObject, QThread, Slot
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QApplication, QWidget, QMainWindow, QTabWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QSpinBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QMessageBox, QListWidget, QListWidgetItem, QTextEdit, QSplitter,
    QGroupBox, QFileDialog
)


# ---------- Paths / Repo root ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GEN_SCRIPT = os.path.join(BASE_DIR, "email_generation.py")
SEND_SCRIPT = os.path.join(BASE_DIR, "email_send.py")
SCANNER_SCRIPT = os.path.join(BASE_DIR, "get_places_and_emails.py")


# ---------- Log helper ----------
def exc_str(e: Exception) -> str:
    return "".join(traceback.format_exception(type(e), e, e.__traceback__))

# ---------- Try imports, but don't crash UI ----------
SUPABASE_OK = True
SUPABASE_ERR = ""
try:
    from db_connect import supabase
except Exception as e:
    SUPABASE_OK = False
    supabase = None
    SUPABASE_ERR = exc_str(e)

SENDER_OK = True
SENDER_ERR = ""
try:
    from email_send import smtp_send
except Exception as e:
    smtp_send = None
    SENDER_OK = False
    SENDER_ERR = exc_str(e)

VALIDATOR_OK = True
VALIDATOR_ERR = ""
try:
    from email_check import validate_email_comprehensive
except Exception as e:
    validate_email_comprehensive = None
    VALIDATOR_OK = False
    VALIDATOR_ERR = exc_str(e)

COMPOSER_OK = True
try:
    from email_generation import compose_email_body, SUBJECT_TEMPLATE
except Exception:
    compose_email_body = None
    SUBJECT_TEMPLATE = "UW Technology Consulting - Discovery Meeting"
    COMPOSER_OK = False

ANTH_GEN_OK = True
try:
    from email_generation import anthropic_generate_for_company
except Exception:
    anthropic_generate_for_company = None
    ANTH_GEN_OK = False


# ---------- Lightweight fallback DNS check ----------
@lru_cache(maxsize=4096)
def _fallback_deliverable(addr: str) -> bool:
    try:
        import re, dns.resolver, dns.exception
        if not addr or "@" not in addr: return False
        if not re.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z0-9.-]+$", addr): return False
        domain = addr.split("@", 1)[1]
        try: 
            idna = domain.encode("idna").decode("ascii")
        except Exception: 
            return False
        r = dns.resolver.Resolver(configure=True)
        r.lifetime, r.timeout = 3.0, 2.0
        try:
            if r.resolve(idna, "MX"):
                return True
        except dns.exception.DNSException:
            pass
        for rr in ("A", "AAAA"):
            try:
                if r.resolve(idna, rr): 
                    return True
            except dns.exception.DNSException:
                continue
        return False
    except Exception:
        return False


# ---------- Email validity ----------
@lru_cache(maxsize=4096)
def is_valid_email(addr: str) -> bool:
    if not addr: 
        return False
    if VALIDATOR_OK and callable(validate_email_comprehensive):
        try:
            verdict, _ = validate_email_comprehensive(addr)
            return verdict == "valid"
        except Exception:
            pass
    return _fallback_deliverable(addr)


# ---------- Supabase helpers ----------
def sb_list_companies() -> List[Dict[str, Any]]:
    if not SUPABASE_OK:
        raise RuntimeError("Supabase client import failed.\n" + SUPABASE_ERR)
    result = supabase.table("companies").select("company_id, company_name, website, description").order("company_id", desc=False).execute()
    return result.data or []


def sb_list_contacts(company_id: int) -> List[Dict[str, Any]]:
    if not SUPABASE_OK:
        raise RuntimeError("Supabase client import failed.\n" + SUPABASE_ERR)
    result = supabase.table("contacts").select("contact_id, contact_name, contact_title, email_address").eq("company_id", company_id).order("contact_id", desc=False).execute()
    rows = result.data or []
    return [r for r in rows if r.get("email_address") and is_valid_email(r["email_address"].strip())]


def sb_list_drafts(company_id: int) -> List[Dict[str, Any]]:
    if not SUPABASE_OK:
        raise RuntimeError("Supabase client import failed.\n" + SUPABASE_ERR)
    result = supabase.table("emails").select("email_id, company_id, subject, body, status").eq("company_id", company_id).eq("status", "draft").order("email_id", desc=False).execute()
    return result.data or []


def sb_update_draft(email_id: int, subject: str, body: str) -> None:
    if not SUPABASE_OK:
        raise RuntimeError("Supabase client import failed.\n" + SUPABASE_ERR)
    supabase.table("emails").update({"subject": subject, "body": body}).eq("email_id", email_id).execute()


def sb_insert_draft(company_id: int, subject: str, body: str) -> int:
    if not SUPABASE_OK:
        raise RuntimeError("Supabase client import failed.\n" + SUPABASE_ERR)
    res = supabase.table("emails").insert({
        "company_id": company_id, "status": "draft", "subject": subject, "body": body
    }).execute()
    
    if res.data and len(res.data) > 0 and "email_id" in res.data[0]:
        return res.data[0]["email_id"]
    
    row = supabase.table("emails").select("email_id").eq("company_id", company_id).eq("status", "draft").order("email_id", desc=True).limit(1).execute()
    if row.data and len(row.data) > 0:
        return row.data[0]["email_id"]
    
    raise RuntimeError("Failed to retrieve email_id after insert")


def sb_mark_sent(email_id: int) -> None:
    if not SUPABASE_OK:
        raise RuntimeError("Supabase client import failed.\n" + SUPABASE_ERR)
    supabase.table("emails").update({"status": "sent"}).eq("email_id", email_id).execute()


# ---------- Generator ----------
def generate_one_draft_for_company(company: Dict[str, Any]) -> Optional[int]:
    if not ANTH_GEN_OK or not callable(anthropic_generate_for_company):
        raise RuntimeError("Anthropic draft generation not available (check ANTHROPIC_API_KEY and email_generation).")
    subject, body = anthropic_generate_for_company(company)
    cid = int(company["company_id"])
    return sb_insert_draft(cid, subject, body)


# ---------- Proper Qt Threading ----------
class WorkerThread(QThread):
    """Worker thread that properly cleans up after itself"""
    finished = Signal(object, object)  # (result, error)
    
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.setTerminationEnabled(True)
    
    def run(self):
        try:
            result = self.fn(*self.args, **self.kwargs)
            self.finished.emit(result, None)
        except Exception as e:
            self.finished.emit(None, e)


class ThreadManager(QObject):
    """Manages thread lifecycle to prevent premature destruction"""
    
    def __init__(self):
        super().__init__()
        self.active_threads = []
    
    def start_thread(self, fn, callback, *args, **kwargs):
        """Start a thread and manage its lifecycle"""
        thread = WorkerThread(fn, *args, **kwargs)
        
        # Store reference to prevent premature GC
        self.active_threads.append(thread)
        
        # Connect callback
        thread.finished.connect(callback)
        
        # Clean up when finished
        def cleanup():
            thread.wait(5000)  # Wait up to 5 seconds for thread to finish
            if thread in self.active_threads:
                self.active_threads.remove(thread)
            thread.deleteLater()
        
        thread.finished.connect(cleanup)
        thread.start()
        
        return thread
    
    def cleanup_all(self):
        """Wait for all threads to finish on shutdown"""
        for thread in self.active_threads[:]:  # Copy list to avoid modification during iteration
            if thread.isRunning():
                thread.quit()
                thread.wait(2000)  # Wait up to 2 seconds per thread


# Global thread manager
_thread_manager = ThreadManager()


def run_in_thread(fn, cb, *args, **kwargs):
    """Helper function to run a function in a managed thread"""
    return _thread_manager.start_thread(fn, cb, *args, **kwargs)


# ---------- UI ----------
class DiscoverTab(QWidget):
    def __init__(self, log: QTextEdit):
        super().__init__()
        self.log = log
        lay = QVBoxLayout(self)

        grid = QHBoxLayout()
        self.kw_edit = QLineEdit("university, veterinary_care, zoo")
        self.lat_edit = QLineEdit("47.65673397183744")
        self.lng_edit = QLineEdit("-122.30658974412395")
        self.radius_spin = QSpinBox()
        self.radius_spin.setRange(1, 1000)
        self.radius_spin.setValue(20)

        for label, w in [("Keywords", self.kw_edit), ("Lat", self.lat_edit),
                         ("Lng", self.lng_edit), ("Radius (mi)", self.radius_spin)]:
            grid.addWidget(QLabel(label))
            grid.addWidget(w)
        lay.addLayout(grid)

        self.hint = QLabel("Run your scanner (e.g., get_places_and_emails.py) separately. Then Refresh companies.")
        self.hint.setStyleSheet("color:#666")
        lay.addWidget(self.hint)

        self.run_btn = QPushButton("Run scanner now")
        self.run_btn.clicked.connect(self.run_scanner)
        lay.addWidget(self.run_btn)

        self.preview_btn = QPushButton("Preview scanner command")
        self.preview_btn.clicked.connect(self.preview_cmd)
        lay.addWidget(self.preview_btn)

    def preview_cmd(self):
        self.log.append("Scanner parameters:")
        self.log.append(f"  keywords={self.kw_edit.text()}")
        self.log.append(f"  lat={self.lat_edit.text()} lng={self.lng_edit.text()} radius_m={round(self.radius_spin.value() * 1609.34)}")
        self.log.append("Run your script manually, then click Companies → Refresh.\n")

    @Slot()
    def run_scanner(self):
        def job():
            if not os.path.isfile(SCANNER_SCRIPT):
                raise RuntimeError(f"Scanner script not found: {SCANNER_SCRIPT}")

            env = os.environ.copy()
            env["KEYWORDS"] = self.kw_edit.text()
            env["LAT"] = self.lat_edit.text()
            env["LNG"] = self.lng_edit.text()
            env["RADIUS_MI"] = str(self.radius_spin.value())

            proc = subprocess.run(
                [sys.executable, SCANNER_SCRIPT],
                cwd=BASE_DIR,
                env=env,
                capture_output=True,
                text=True
            )
            out = (proc.stdout or "")[-4000:]
            err = (proc.stderr or "")[-2000:]
            if proc.returncode != 0:
                raise RuntimeError(f"Scanner failed ({proc.returncode}).\n{err}\n{out}")
            return out

        @Slot(object, object)
        def done(res, err):
            self.run_btn.setDisabled(False)
            if err:
                self.log.append("❌ Scanner failed:\n" + exc_str(err))
                QMessageBox.critical(self, "Scanner failed", str(err))
            else:
                self.log.append("✅ Scanner finished:\n" + (res or "(no output)"))
                QMessageBox.information(self, "Scanner complete", "Scanner finished. Now refresh Companies.")

        self.run_btn.setDisabled(True)
        run_in_thread(job, done)


class CompaniesTab(QWidget):
    company_selected = Signal(dict)
    
    def __init__(self, log: QTextEdit):
        super().__init__()
        self.log = log
        lay = QVBoxLayout(self)
        
        top = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh)
        self.diag_btn = QPushButton("Diagnostics")
        self.diag_btn.clicked.connect(self.diagnostics)
        top.addWidget(self.refresh_btn)
        top.addWidget(self.diag_btn)
        top.addStretch(1)
        lay.addLayout(top)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["ID", "Name", "Website", "Description"])
        for i, mode in enumerate([QHeaderView.ResizeToContents, QHeaderView.Stretch, QHeaderView.Stretch, QHeaderView.Stretch]):
            self.table.horizontalHeader().setSectionResizeMode(i, mode)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.doubleClicked.connect(self.emit_selected)
        lay.addWidget(self.table, 1)

        self.data = []

    @Slot()
    def refresh(self):
        self.refresh_btn.setDisabled(True)
        run_in_thread(sb_list_companies, self._refreshed)

    @Slot(object, object)
    def _refreshed(self, res, err):
        self.refresh_btn.setDisabled(False)
        if err:
            self.log.append("❌ Companies refresh failed:\n" + exc_str(err))
            QMessageBox.critical(self, "Supabase error", str(err))
            return
        self.data = res or []
        self.table.setRowCount(len(self.data))
        for r, row in enumerate(self.data):
            self.table.setItem(r, 0, QTableWidgetItem(str(row["company_id"])))
            self.table.setItem(r, 1, QTableWidgetItem(row.get("company_name", "")))
            self.table.setItem(r, 2, QTableWidgetItem(row.get("website", "") or ""))
            self.table.setItem(r, 3, QTableWidgetItem(row.get("description", "") or ""))
        self.log.append(f"✅ Loaded {len(self.data)} companies.")

    @Slot()
    def emit_selected(self):
        idx = self.table.currentRow()
        if 0 <= idx < len(self.data):
            self.company_selected.emit(self.data[idx])

    @Slot()
    def diagnostics(self):
        msgs = []
        msgs.append(f"Supabase import: {'OK' if SUPABASE_OK else 'ERROR'}")
        if not SUPABASE_OK: 
            msgs.append("  " + (SUPABASE_ERR.splitlines()[-1] if SUPABASE_ERR else "Unknown error"))
        msgs.append(f"email_check.validate_email_comprehensive: {'OK' if VALIDATOR_OK else 'MISSING'}")
        if not VALIDATOR_OK: 
            msgs.append("  " + (VALIDATOR_ERR.splitlines()[-1] if VALIDATOR_ERR else "Unknown error"))
        msgs.append(f"email_send.smtp_send: {'OK' if SENDER_OK else 'MISSING'}")
        if not SENDER_OK: 
            msgs.append("  " + (SENDER_ERR.splitlines()[-1] if SENDER_ERR else "Unknown error"))
        msgs.append(f"email_generation.anthropic_generate_for_company: {'OK' if ANTH_GEN_OK else 'MISSING'}")
        msgs.append(f"email_generation.py path: {GEN_SCRIPT} ({'exists' if os.path.isfile(GEN_SCRIPT) else 'NOT FOUND'})")
        self.log.append("\n".join(["🔎 Diagnostics:"] + msgs) + "\n")


class ComposeTab(QWidget):
    def __init__(self, log: QTextEdit):
        super().__init__()
        self.log = log
        self.company = None
        self.contacts = []
        self.drafts = []
        self.active_draft = None

        main = QVBoxLayout(self)
        head = QHBoxLayout()
        self.company_label = QLabel("No company selected")
        head.addWidget(self.company_label, 1)
        self.load_btn = QPushButton("Load contacts & drafts")
        self.load_btn.clicked.connect(self.load_company_data)
        self.gen_btn = QPushButton("Generate draft")
        self.gen_btn.clicked.connect(self.generate_draft_now)
        if not ANTH_GEN_OK:
            self.gen_btn.setDisabled(True)
            self.gen_btn.setToolTip("Anthropic generation not available")
        head.addWidget(self.load_btn)
        head.addWidget(self.gen_btn)
        main.addLayout(head)

        split = QSplitter(Qt.Horizontal)
        left = QWidget()
        ll = QVBoxLayout(left)
        ll.addWidget(QLabel("Recipients (valid emails only)"))
        self.contact_list = QListWidget()
        ll.addWidget(self.contact_list, 1)
        ll.addWidget(QLabel("Drafts"))
        self.draft_list = QListWidget()
        self.draft_list.itemSelectionChanged.connect(self.select_draft)
        ll.addWidget(self.draft_list, 1)

        right = QWidget()
        rl = QVBoxLayout(right)
        form = QGroupBox("Editor")
        fl = QVBoxLayout(form)
        self.subj_edit = QLineEdit()
        self.body_edit = QTextEdit()
        fl.addWidget(QLabel("Subject"))
        fl.addWidget(self.subj_edit)
        fl.addWidget(QLabel("Body (HTML or plain text)"))
        fl.addWidget(self.body_edit, 1)
        rl.addWidget(form, 1)
        
        actions = QHBoxLayout()
        self.save_btn = QPushButton("Save Draft")
        self.save_btn.clicked.connect(self.save_draft)
        self.send_btn = QPushButton("Send to Selected")
        self.send_btn.clicked.connect(self.send_now)
        if not SENDER_OK:
            self.send_btn.setDisabled(True)
            self.send_btn.setToolTip("email_send.smtp_send missing")
        actions.addStretch(1)
        actions.addWidget(self.save_btn)
        actions.addWidget(self.send_btn)
        rl.addLayout(actions)

        split.addWidget(left)
        split.addWidget(right)
        split.setStretchFactor(1, 2)
        main.addWidget(split, 1)

    def set_company(self, company: Dict[str, Any]):
        self.company = company
        self.company_label.setText(f"{company.get('company_name', '(unknown)')} — {company.get('website', '')}")
        self.contacts = []
        self.drafts = []
        self.active_draft = None
        self.contact_list.clear()
        self.draft_list.clear()
        self.subj_edit.clear()
        self.body_edit.clear()

    @Slot()
    def load_company_data(self):
        if not self.company:
            QMessageBox.information(self, "Pick a company", "Select a company in the Companies tab.")
            return
        cid = int(self.company["company_id"])
        
        self.load_btn.setDisabled(True)

        @Slot(object, object)
        def after_contacts(res, err):
            if err:
                self.load_btn.setDisabled(False)
                self.log.append("❌ Contacts load failed:\n" + exc_str(err))
                QMessageBox.critical(self, "Error", str(err))
                return
            self.contacts = res or []
            self.contact_list.clear()
            for c in self.contacts:
                it = QListWidgetItem(f"{c.get('contact_name') or '(no name)'}  <{c['email_address']}>")
                it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
                it.setCheckState(Qt.Unchecked)
                it.setData(Qt.UserRole, c)
                self.contact_list.addItem(it)
            
            # Now load drafts
            run_in_thread(lambda: sb_list_drafts(cid), after_drafts)

        @Slot(object, object)
        def after_drafts(res, err):
            self.load_btn.setDisabled(False)
            if err:
                self.log.append("❌ Drafts load failed:\n" + exc_str(err))
                QMessageBox.critical(self, "Error", str(err))
                return
            self.drafts = res or []
            self.draft_list.clear()
            for d in self.drafts:
                it = QListWidgetItem(f"#{d['email_id']}  {d.get('subject') or '(no subject)'}")
                it.setData(Qt.UserRole, d)
                self.draft_list.addItem(it)
            self.log.append(f"✅ {len(self.contacts)} valid contacts; {len(self.drafts)} drafts.")
            if self.drafts:
                self.draft_list.setCurrentRow(0)
        
        run_in_thread(lambda: sb_list_contacts(cid), after_contacts)

    @Slot()
    def select_draft(self):
        items = self.draft_list.selectedItems()
        if not items:
            self.active_draft = None
            self.subj_edit.clear()
            self.body_edit.clear()
            return
        d = items[0].data(Qt.UserRole)
        self.active_draft = d
        self.subj_edit.setText(d.get("subject") or "")
        self.body_edit.setPlainText(d.get("body") or "")

    @Slot()
    def generate_draft_now(self):
        if not self.company:
            QMessageBox.information(self, "Pick a company", "Select a company in the Companies tab first.")
            return
        
        self.gen_btn.setDisabled(True)
        
        def job():
            return generate_one_draft_for_company(self.company)
        
        @Slot(object, object)
        def done(eid, err):
            self.gen_btn.setDisabled(False)
            if err:
                self.log.append("❌ Generate draft failed:\n" + exc_str(err))
                QMessageBox.critical(self, "Error", str(err))
                return
            self.log.append(f"✅ Generated draft #{eid}. Reloading list.")
            self.load_company_data()
            QMessageBox.information(self, "Draft created", f"Draft #{eid} created. You can edit and send.")
        
        run_in_thread(job, done)

    @Slot()
    def save_draft(self):
        if not self.active_draft:
            QMessageBox.information(self, "No draft", "Select a draft first.")
            return
        eid = int(self.active_draft["email_id"])
        subject = self.subj_edit.text().strip()
        body = self.body_edit.toPlainText().strip()
        
        self.save_btn.setDisabled(True)
        
        @Slot(object, object)
        def done(_res, err):
            self.save_btn.setDisabled(False)
            if err:
                self.log.append("❌ Saving draft failed:\n" + exc_str(err))
                QMessageBox.critical(self, "Error", str(err))
                return
            self.active_draft["subject"] = subject
            self.active_draft["body"] = body
            self.log.append(f"✅ Draft #{self.active_draft['email_id']} saved successfully.")
            QMessageBox.information(self, "Saved", "Draft updated.")
        
        run_in_thread(lambda: sb_update_draft(eid, subject, body), done)

    def _selected_recips(self) -> List[Dict[str, Any]]:
        out = []
        for i in range(self.contact_list.count()):
            it = self.contact_list.item(i)
            if it.checkState() == Qt.Checked:
                out.append(it.data(Qt.UserRole))
        return out

    @Slot()
    def send_now(self):
        if not SENDER_OK or smtp_send is None:
            QMessageBox.warning(self, "Sender missing", "email_send.smtp_send not available.")
            return
        if not self.active_draft:
            QMessageBox.information(self, "No draft", "Select a draft first.")
            return
        recips = self._selected_recips()
        if not recips:
            QMessageBox.information(self, "No recipients", "Check at least one recipient.")
            return

        subj = self.subj_edit.text().strip()
        body = self.body_edit.toPlainText().strip()
        html_body = body if body.lstrip().startswith("<") else None
        email_id = int(self.active_draft["email_id"])

        self.send_btn.setDisabled(True)
        
        def job():
            errs = []
            sent = 0
            for r in recips:
                to = r["email_address"]
                try:
                    smtp_send(to, subj, body, body_html=html_body)
                    sent += 1
                except Exception as e:
                    errs.append(f"{to}: {e}")
            return sent, errs, email_id
        
        @Slot(object, object)
        def done(result, err):
            self.send_btn.setDisabled(False)
            if err:
                self.log.append("❌ Send failed:\n" + exc_str(err))
                QMessageBox.critical(self, "Error", str(err))
                return
            sent, errs, eid = result
            if sent:
                try:
                    sb_mark_sent(eid)
                    self.log.append(f"✅ Sent {sent} emails. Draft #{eid} marked as sent.")
                except Exception as e:
                    self.log.append("⚠️ Could not mark sent: " + str(e))
                QMessageBox.information(self, "Sent", f"Emails sent: {sent}")
            if errs:
                self.log.append("❌ Some sends failed:\n" + "\n".join(errs))
                QMessageBox.warning(self, "Partial failure", f"Sent {sent}, but {len(errs)} failed. Check log.")
        
        run_in_thread(job, done)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Outreach Dashboard (Qt)")
        self.resize(1200, 820)

        self.log = QTextEdit()
        self.log.setReadOnly(True)

        self.tabs = QTabWidget()
        self.discover = DiscoverTab(self.log)
        self.companies = CompaniesTab(self.log)
        self.compose = ComposeTab(self.log)

        self.tabs.addTab(self.discover, "Discover")
        self.tabs.addTab(self.companies, "Companies")
        self.tabs.addTab(self.compose, "Compose & Send")

        wrapper = QWidget()
        lay = QVBoxLayout(wrapper)
        lay.addWidget(self.tabs, 1)
        lay.addWidget(QLabel("Log"))
        lay.addWidget(self.log, 0)
        self.setCentralWidget(wrapper)

        self.companies.company_selected.connect(self.on_company_selected)

        # Menu
        bar = self.menuBar()
        file_menu = bar.addMenu("&File")
        act = QAction("Quit", self)
        act.triggered.connect(self.close_application)
        file_menu.addAction(act)

        tools = bar.addMenu("&Tools")
        ref = QAction("Refresh Companies", self)
        ref.triggered.connect(self.companies.refresh)
        tools.addAction(ref)

        # First load
        self.companies.refresh()

    @Slot(dict)
    def on_company_selected(self, company: Dict[str, Any]):
        self.compose.set_company(company)
        self.tabs.setCurrentWidget(self.compose)
        
        if ANTH_GEN_OK:
            def job():
                return generate_one_draft_for_company(company)
            
            @Slot(object, object)
            def after(eid, err):
                if err:
                    self.log.append("⚠️ Auto-generate draft failed: " + str(err) + " — loading existing data.")
                else:
                    self.log.append("✅ Generated draft #" + str(eid) + ". Loading contacts & drafts.")
                self.compose.load_company_data()
            
            run_in_thread(job, after)
        else:
            self.log.append("Anthropic generation not available — loading existing contacts & drafts.")
            self.compose.load_company_data()
    
    @Slot()
    def close_application(self):
        """Clean shutdown of application"""
        self.log.append("Shutting down...")
        _thread_manager.cleanup_all()
        QApplication.instance().quit()
    
    def closeEvent(self, event):
        """Handle window close event"""
        _thread_manager.cleanup_all()
        event.accept()


def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    exit_code = app.exec()
    
    # Ensure all threads are cleaned up before exit
    _thread_manager.cleanup_all()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()