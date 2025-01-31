CREATE TABLE IF NOT EXISTS solar_contacts_without_salesforce_founder (
    id TEXT NOT NULL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    linkedin TEXT,
    title TEXT,
    email TEXT,
    email_status TEXT,
    id_tracker TEXT,
    website TEXT,
    company_linkedin TEXT,
    work_history JSONB[]
)