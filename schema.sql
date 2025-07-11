CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    industry VARCHAR(100),
    description TEXT,
    source VARCHAR(50) -- 'google_maps', 'linkedin', 'manual', etc.
);

CREATE TABLE contacts (
    company_id INTEGER,
    contact_name VARCHAR(255),
    email_address VARCHAR(255),
    contact_title VARCHAR(255),
    contact_linkedin_url VARCHAR(255),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

CREATE TABLE emails (
    email_id SERIAL PRIMARY KEY,
    company_id INTEGER,
    subject VARCHAR(255),
    body TEXT,
    status VARCHAR(50), -- 'pending', 'approved', 'sent', 'opened', 'replied'
    sent_at TIMESTAMP,
    replied_at TIMESTAMP,
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

CREATE INDEX idx_companies_name ON companies(company_name);
CREATE INDEX idx_emails_status ON emails(status);
CREATE INDEX idx_emails_company_id ON emails(company_id);