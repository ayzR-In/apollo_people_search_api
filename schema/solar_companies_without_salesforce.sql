CREATE TABLE IF NOT EXISTS solar_companies_without_salesforce (
        company TEXT NOT NULL,
        company_name_email TEXT,
        employees BIGINT,
        industry TEXT,
        website TEXT,
        company_linkedin_url TEXT,
        facebook_url TEXT,
        twitter_url TEXT,
        company_street TEXT,
        company_city TEXT,
        company_state TEXT,
        company_country TEXT,
        company_postal_code TEXT,
        company_address TEXT,
        keywords TEXT[],
        company_phone TEXT,
        seo_discription TEXT,
        technologies TEXT[],
        total_funding BIGINT,
        latest_funding TEXT,
        latest_funding_amount BIGINT,
        last_raised_at TEXT,
        annual_revenue BIGINT,
        number_of_retail_location BIGINT,
        short_description TEXT,
        founded_year BIGINT,
        competed BOOLEAN DEFAULT FALSE,
        PRIMARY KEY (company, company_linkedin_url)
);

-- INSERTION 

INSERT INTO solar_companies_without_salesforce
WITH more_than_5_employees AS (
	SELECT *
	FROM solar_companies
	WHERE employees > 5
	ORDER BY employees DESC
),
companies_with_website AS (
SELECT * 
FROM more_than_5_employees
WHERE website != 'NaN'
),
solar_companies_with_description AS (
SELECT *
FROM companies_with_website
WHERE short_description != 'NaN'
)
SELECT *
FROM solar_companies_with_description
WHERE 'Salesforce' != ANY (technologies)