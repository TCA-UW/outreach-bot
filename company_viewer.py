import sys
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QTableWidget,
    QTableWidgetItem, QHeaderView, QComboBox, QLineEdit, QPushButton, QInputDialog, QMessageBox
)
from PyQt5.QtGui import QColor
from PyQt5.QtCore import Qt, QTimer
from db_connect import supabase 

class CompanyViewer(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Company Database Viewer")
        self.resize(1200, 650)

        layout = QVBoxLayout()
        self.setLayout(layout)

        header_layout = QVBoxLayout()

        title = QLabel("Company Records")
        title.setStyleSheet("font-size: 20px; font-weight: bold; margin-bottom: 10px;")
        header_layout.addWidget(title)

        button_layout = QHBoxLayout()
        
        add_column_btn = QPushButton("Add New Column")
        add_column_btn.clicked.connect(self.add_new_column)
        button_layout.addWidget(add_column_btn)

        delete_column_btn = QPushButton("Delete Column")
        delete_column_btn.clicked.connect(self.delete_column)
        button_layout.addWidget(delete_column_btn)

        button_layout.addStretch()
        header_layout.addLayout(button_layout)

        layout.addLayout(header_layout)

        self.table = QTableWidget()
        layout.addWidget(self.table)

        self.load_data()

    def get_table_columns(self):
        try:
            response = supabase.table("companies").select("*").limit(1).execute()
            if response.data:
                return list(response.data[0].keys())
            else:
                return ["company_id", "company_name", "website"]
        except Exception as e:
            print(f"Error getting table columns: {e}")
            return ["company_id", "company_name", "website"]

    def add_new_column(self):
        column_name, ok = QInputDialog.getText(self, "Add Column", "Enter new column name:")
        if ok and column_name.strip():
            column_name = column_name.strip().lower().replace(" ", "_")
            
            existing_columns = self.get_table_columns()
            if column_name in existing_columns:
                QMessageBox.warning(self, "Column Exists", f"Column '{column_name}' already exists!")
                return
            
            try:
                sql = f'ALTER TABLE companies ADD COLUMN "{column_name}" TEXT;'
                try:
                    supabase.rpc("execute_sql", {"sql": sql}).execute()
                    print(f"✅ Added column '{column_name}' to companies table using RPC.")
                except Exception as rpc_error:
                    print(f"RPC method failed: {rpc_error}")
                    raise rpc_error
                
                QMessageBox.information(self, "Success", f"Column '{column_name}' added successfully!")
                self.load_data()
                
            except Exception as e:
                error_msg = f"Failed to add column: {str(e)}\n\n"
                error_msg += "To enable column operations, you need to create an RPC function in Supabase:\n\n"
                error_msg += "1. Go to Supabase Dashboard > SQL Editor\n"
                error_msg += "2. Run this SQL to create the function:\n\n"
                error_msg += "CREATE OR REPLACE FUNCTION execute_sql(sql text)\n"
                error_msg += "RETURNS void AS $$\n"
                error_msg += "BEGIN\n"
                error_msg += "  EXECUTE sql;\n"
                error_msg += "END;\n"
                error_msg += "$$ LANGUAGE plpgsql SECURITY DEFINER;"
                
                QMessageBox.critical(self, "Error", error_msg)
                print(f"❌ Failed to add column: {e}")

    def delete_column(self):
        all_columns = self.get_table_columns()
        protected_columns = ["company_id", "created_at", "updated_at", "company_name", "website", "email", "description"]
        deletable_columns = [col for col in all_columns if col not in protected_columns]
        
        if not deletable_columns:
            QMessageBox.information(self, "No Columns", "No deletable columns found.")
            return
        
        column_name, ok = QInputDialog.getItem(self, "Delete Column", 
                                             "Select column to delete:", 
                                             deletable_columns, 0, False)
        
        if ok and column_name:
            reply = QMessageBox.question(self, "Confirm Deletion", 
                                       f"Are you sure you want to delete column '{column_name}'?\n\n"
                                       f"This action cannot be undone and will permanently remove "
                                       f"all data in this column.",
                                       QMessageBox.Yes | QMessageBox.No,
                                       QMessageBox.No)
            
            if reply == QMessageBox.Yes:
                try:
                    sql = f'ALTER TABLE companies DROP COLUMN "{column_name}";'
                    supabase.rpc("execute_sql", {"sql": sql}).execute()
                    print(f"✅ Deleted column '{column_name}' from companies table.")
                    QMessageBox.information(self, "Success", f"Column '{column_name}' deleted successfully!")
                    self.load_data()
                    
                except Exception as e:
                    error_msg = f"Failed to delete column: {str(e)}\n\n"
                    error_msg += "Make sure the execute_sql RPC function is set up in Supabase."
                    QMessageBox.critical(self, "Error", error_msg)
                    print(f"❌ Failed to delete column: {e}")

    def load_data(self):
        all_db_columns = self.get_table_columns()
        
        all_companies = []
        page_size = 1000
        start = 0

        while True:
            response = (
                supabase.table("companies")
                .select("*, contacts(email_address), emails(outreach_person, status)")
                .order("company_id", desc=False)
                .range(start, start + page_size - 1)
                .execute()
            )
            batch = response.data or []
            all_companies.extend(batch)
            if len(batch) < page_size:
                break
            start += page_size

        filtered_companies = []
        for company in all_companies:
            contact_list = company.get("contacts", [])
            email_addresses = [c.get("email_address") for c in contact_list if c.get("email_address")]
            if email_addresses:
                filtered_companies.append(company)

        core_columns = ["company_name", "website"]
        core_display_names = ["Company Name", "Website"]
        
        excluded_columns = ["company_id", "company_name", "website", "created_at", "updated_at", "contacts", "emails", "description"]
        additional_columns = [col for col in all_db_columns if col not in excluded_columns]
        
        display_columns = core_display_names + ["Emails", "Outreach Person", "Status"] + additional_columns
        
        self.table.setColumnCount(len(display_columns))
        self.table.setHorizontalHeaderLabels(display_columns)
        self.table.setRowCount(len(filtered_companies))

        for row_idx, company in enumerate(filtered_companies):
            company_id = company["company_id"]
            name = company.get("company_name", "")
            website = company.get("website", "")
            contact_list = company.get("contacts", [])
            emails = ", ".join([c.get("email_address") for c in contact_list if c.get("email_address")])

            email_info = company.get("emails", [])
            outreach_person = ""
            status_value = "Unsent"
            if email_info:
                first_email_record = email_info[0]
                outreach_person = first_email_record.get("outreach_person", "") or ""
                status_value = first_email_record.get("status", "Unsent") or "Unsent"

            col_idx = 0
            self.table.setItem(row_idx, col_idx, QTableWidgetItem(name))
            col_idx += 1
            self.table.setItem(row_idx, col_idx, QTableWidgetItem(website))
            col_idx += 1
            
            self.table.setItem(row_idx, col_idx, QTableWidgetItem(emails))
            col_idx += 1

            outreach_input = QLineEdit(outreach_person)
            outreach_input.editingFinished.connect(
                lambda cid=company_id, widget=outreach_input: self.update_outreach_person(cid, widget.text())
            )
            self.table.setCellWidget(row_idx, col_idx, outreach_input)
            col_idx += 1

            status_dropdown = QComboBox()
            status_dropdown.addItems(["Unsent", "Emailed", "In Talks", "Meeting Scheduled", "Rejected"])
            status_dropdown.setCurrentText(status_value)
            status_dropdown.currentTextChanged.connect(
                lambda value, cid=company_id, row=row_idx: self.update_status(cid, value, row)
            )
            self.table.setCellWidget(row_idx, col_idx, status_dropdown)
            col_idx += 1

            for additional_col in additional_columns:
                col_value = company.get(additional_col, "")
                if col_value is None:
                    col_value = ""
                
                additional_input = QLineEdit(str(col_value))
                additional_input.editingFinished.connect(
                    lambda cid=company_id, col_name=additional_col, widget=additional_input: 
                    self.update_additional_column(cid, col_name, widget.text())
                )
                self.table.setCellWidget(row_idx, col_idx, additional_input)
                col_idx += 1

            if status_value == "Rejected":
                self.set_full_row_color(row_idx, QColor(255, 171, 168))

        self.table.horizontalHeader().setDefaultSectionSize(200)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)

    def set_full_row_color(self, row, color):
        for col in range(self.table.columnCount()):
            item = self.table.item(row, col)
            if item:
                item.setBackground(color)
            else:
                widget = self.table.cellWidget(row, col)
                if widget:
                    widget.setStyleSheet(f"background-color: rgb({color.red()}, {color.green()}, {color.blue()});")

    def update_outreach_person(self, company_id, new_value):
        row_idx = self.find_row_by_company_id(company_id)
        if row_idx == -1:
            return
            
        status_widget = self.table.cellWidget(row_idx, 4)
        current_status = status_widget.currentText() if status_widget else "Unsent"

        try:
            existing = supabase.table("emails").select("email_id").eq("company_id", company_id).execute().data
            if existing:
                supabase.table("emails").update({
                    "outreach_person": new_value,
                    "status": current_status
                }).eq("company_id", company_id).execute()
            else:
                supabase.table("emails").insert({
                    "company_id": company_id,
                    "outreach_person": new_value,
                    "status": current_status
                }).execute()
        except Exception as e:
            print(f"Error updating outreach person: {e}")

    def update_status(self, company_id, new_status, row_idx):
        outreach_widget = self.table.cellWidget(row_idx, 3)
        current_outreach = outreach_widget.text() if outreach_widget else ""

        try:
            existing = supabase.table("emails").select("email_id").eq("company_id", company_id).execute().data
            if existing:
                supabase.table("emails").update({
                    "status": new_status,
                    "outreach_person": current_outreach
                }).eq("company_id", company_id).execute()
            else:
                supabase.table("emails").insert({
                    "company_id": company_id,
                    "status": new_status,
                    "outreach_person": current_outreach
                }).execute()

            if new_status == "Rejected":
                self.set_full_row_color(row_idx, QColor(255, 181, 179)) 
            else:
                self.clear_row_color(row_idx)
        except Exception as e:
            print(f"Error updating status: {e}")

    def update_additional_column(self, company_id, column_name, new_value):
        try:
            supabase.table("companies").update({column_name: new_value}).eq("company_id", company_id).execute()
        except Exception as e:
            print(f"Error updating additional column '{column_name}': {e}")

    def find_row_by_company_id(self, company_id):
        for row in range(self.table.rowCount()):
            name_item = self.table.item(row, 0)
            if name_item:
                company_name = name_item.text()
                try:
                    response = supabase.table("companies").select("company_id").eq("company_name", company_name).limit(1).execute()
                    if response.data and response.data[0]["company_id"] == company_id:
                        return row
                except Exception as e:
                    print(f"Error finding row: {e}")
        return -1

    def clear_row_color(self, row):
        for col in range(self.table.columnCount()):
            item = self.table.item(row, col)
            if item:
                item.setBackground(self.table.palette().base())
            else:
                widget = self.table.cellWidget(row, col)
                if widget:
                    widget.setStyleSheet("")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    viewer = CompanyViewer()
    viewer.show()
    sys.exit(app.exec_())
