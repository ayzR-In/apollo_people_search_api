CREATE TABLE api_contacts (
    id TEXT NOT NULL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    title TEXT,
    linkedin TEXT,
    company_linkedin TEXT,
    website TEXT,
    email TEXT,
    email_status TEXT,
    id_tracker TEXT,
    work_history TEXT[],
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    enriched BOOLEAN DEFAULT false
);