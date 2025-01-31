import psycopg2
import psycopg2.extras
import pandas as pd

hostname = "whitewolfden.myddns.me"
database = "abhinav-dev"
username = "kingslayer"
password = "4Khc5D8+6T.3"
port_id = 6204
conn = None

csv_path = r'C:\Users\StingerStain\Desktop\Git\apollo_people_search_api\staging-to-raw\solar.csv'

create_table_query='''
    CREATE TABLE IF NOT EXISTS solar_companies (
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
        PRIMARY KEY (company, company_linkedin_url)
    );
    '''

insert_query = '''
    INSERT INTO solar_companies (
    company, company_name_email, employees, industry,
    website, company_linkedin_url, facebook_url, twitter_url,
    company_street, company_city, company_state, company_country,
    company_postal_code, company_address, keywords, company_phone,
    seo_discription, technologies, total_funding, latest_funding,
    latest_funding_amount, last_raised_at, annual_revenue, number_of_retail_location,
    short_description, founded_year
    ) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, string_to_array(COALESCE(%s, ''), ', '), %s,
    %s, string_to_array(COALESCE(%s, ''), ', '), %s, %s,
    %s, %s, %s, %s,
    %s, %s
    ) ON CONFLICT (company, company_linkedin_url) DO NOTHING;
'''

try:
    with psycopg2.connect(
        host = hostname,
        database = database,
        user = username,
        password = password,
        port = port_id
    ) as conn:
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            # Comment below line in production
            cur.execute('DROP TABLE IF EXISTS company_data')
            
            # Create table
            cur.execute(create_table_query)
            
            # Column selection
            df = pd.read_csv(csv_path)
            df = df.drop(['Account Stage','Lists', 'Account Owner', 'Apollo Account Id', 'SIC Codes', 'Logo Url'], axis=1)
            
            # Transformations
            df['# Employees'] = df['# Employees'].fillna(0).astype('int')
            df['Total Funding'] = df['Total Funding'].fillna(0).astype('int')
            df['Latest Funding Amount'] = df['Latest Funding Amount'].fillna(0).astype('int')
            df['Last Raised At'] = df['Last Raised At'].fillna(0).astype('string')
            df['Annual Revenue'] = df['Annual Revenue'].fillna(0).astype('int')
            df['Number of Retail Locations'] = df['Number of Retail Locations'].fillna(0).astype('int')
            df['Founded Year'] = df['Founded Year'].fillna(0).astype('int')
            
            df.replace('', None, inplace=True)
            
            # Handle string-to-array conversion for keywords and technologies
            df['Keywords'] = df['Keywords'].apply(lambda x: '' if pd.isna(x) else x)
            df['Technologies'] = df['Technologies'].apply(lambda x: '' if pd.isna(x) else x)
            
            # Batch insert for efficiency
            rows = [tuple(row) for _, row in df.iterrows()]
            psycopg2.extras.execute_batch(cur, insert_query, rows)
            conn.commit()

except Exception as error:
    print(error)
finally:
    if conn is not None:
        conn.close()