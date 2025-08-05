import sys
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel, QTableWidget,
    QTableWidgetItem, QHeaderView, QComboBox, QLineEdit
)
from PyQt5.QtGui import QColor
from db_connect import supabase 

class CompanyViewer(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Company Database Viewer")
        self.resize(1200, 650)  

        layout = QVBoxLayout()
        self.setLayout(layout)

        title = QLabel("Company Records")
        title.setStyleSheet("font-size: 20px; font-weight: bold; margin-bottom: 10px;")
        layout.addWidget(title)

        self.table = QTableWidget()
        layout.addWidget(self.table)

        self.load_data()

    def load_data(self):
        all_companies = []
        page_size = 1000
        start = 0

        while True:
            # Pull companies, contacts, and emails in one query
            response = (
                supabase.table("companies")
                .select("company_id, company_name, website, contacts(email_address), emails(outreach_person, status)")
                .order("company_id", desc=False)
                .range(start, start + page_size - 1)
                .execute()
            )
            batch = response.data or []
            all_companies.extend(batch)
            if len(batch) < page_size:
                break
            start += page_size

        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["ID", "Company Name", "Website", "Emails", "Outreach Person", "Status"])
        self.table.setRowCount(len(all_companies))

        for row_idx, company in enumerate(all_companies):
            company_id = company["company_id"]
            name = company.get("company_name", "")
            website = company.get("website", "")

            # Emails list from related contacts
            contact_list = company.get("contacts", [])
            emails = ", ".join(
                [c.get("email_address") for c in contact_list if c.get("email_address")]
            )

            # Outreach and Status from related emails table
            email_info = company.get("emails", [])
            outreach_person = ""
            status_value = "Unsent"
            if email_info:
                first_email_record = email_info[0]
                outreach_person = first_email_record.get("outreach_person", "") or ""
                status_value = first_email_record.get("status", "Unsent") or "Unsent"

            # Basic info
            self.table.setItem(row_idx, 0, QTableWidgetItem(str(company_id)))
            self.table.setItem(row_idx, 1, QTableWidgetItem(name))
            self.table.setItem(row_idx, 2, QTableWidgetItem(website))
            self.table.setItem(row_idx, 3, QTableWidgetItem(emails))

            # Outreach Person input
            outreach_input = QLineEdit(outreach_person)
            outreach_input.editingFinished.connect(
                lambda cid=company_id, widget=outreach_input: self.update_outreach_person(cid, widget.text())
            )
            self.table.setCellWidget(row_idx, 4, outreach_input)

            # Status dropdown
            status_dropdown = QComboBox()
            status_dropdown.addItems(["Unsent", "Emailed", "In Talks", "Meeting Scheduled", "Rejected"])
            status_dropdown.setCurrentText(status_value)
            status_dropdown.currentTextChanged.connect(
                lambda value, cid=company_id, row=row_idx: self.update_status(cid, value, row)
            )
            self.table.setCellWidget(row_idx, 5, status_dropdown)

            if status_value == "Rejected":
                self.set_row_color(row_idx, QColor(255, 171, 168))

        self.table.horizontalHeader().setDefaultSectionSize(200)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)


    def set_row_color(self, row, color):
        """Set the background color for the entire row."""
        for col in range(self.table.columnCount()):
            item = self.table.item(row, col)
            if item:
                item.setBackground(color)

    def update_outreach_person(self, company_id, new_value):
        """Update or insert outreach person while preserving status."""
        # Get current status from UI
        row_idx = self.find_row_by_company_id(company_id)
        status_widget = self.table.cellWidget(row_idx, 5)
        current_status = status_widget.currentText() if status_widget else "Unsent"

        # Get company name from table
        company_name_item = self.table.item(row_idx, 1)
        company_name = company_name_item.text() if company_name_item else str(company_id)

        # Check if an entry already exists for this company
        existing = supabase.table("emails").select("email_id").eq("company_id", company_id).execute().data
        if existing:
            supabase.table("emails").update({
                "outreach_person": new_value,
                "status": current_status
            }).eq("company_id", company_id).execute()
            print(f"✅ Updated outreach_person for '{company_name}' to '{new_value}'")
        else:
            supabase.table("emails").insert({
                "company_id": company_id,
                "outreach_person": new_value,
                "status": current_status
            }).execute()
            print(f"✅ Inserted outreach_person for '{company_name}' as '{new_value}'")

    def update_status(self, company_id, new_status, row_idx):
        """Update or insert status while preserving outreach person."""
        # Get current outreach person from UI
        outreach_widget = self.table.cellWidget(row_idx, 4)
        current_outreach = outreach_widget.text() if outreach_widget else ""

        # Get company name from table
        company_name_item = self.table.item(row_idx, 1)
        company_name = company_name_item.text() if company_name_item else str(company_id)

        # Check if an entry already exists for this company
        existing = supabase.table("emails").select("email_id").eq("company_id", company_id).execute().data
        if existing:
            supabase.table("emails").update({
                "status": new_status,
                "outreach_person": current_outreach
            }).eq("company_id", company_id).execute()
            print(f"✅ Updated status for '{company_name}' to '{new_status}'")
        else:
            supabase.table("emails").insert({
                "company_id": company_id,
                "status": new_status,
                "outreach_person": current_outreach
            }).execute()
            print(f"✅ Inserted status for '{company_name}' as '{new_status}'")

        if new_status == "Rejected":
            self.set_row_color(row_idx, QColor(255, 181, 179)) 
        else:
            self.clear_row_color(row_idx) 

    def find_row_by_company_id(self, company_id):
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item and item.text() == str(company_id):
                return row
        return -1
    
    def clear_row_color(self, row):
        for col in range(self.table.columnCount()):
            item = self.table.item(row, col)
            if item:
                item.setBackground(self.table.palette().base())

if __name__ == "__main__":
    app = QApplication(sys.argv)
    viewer = CompanyViewer()
    viewer.show()
    sys.exit(app.exec_())