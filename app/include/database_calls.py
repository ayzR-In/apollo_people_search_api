import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras
import json
import os

def get_db_connection():
    """Create and return a database connection"""
    conn = psycopg2.connect(
        host='localhost',
        database='airflow-test-1',
        user='postgres',
        password='ayaz1234',
        port=5400
    )
    return conn

def get_urls():
    # Create SQLAlchemy engine
    connection_string = f'postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}'
    engine = create_engine(connection_string)

    # SQL query
    query = """
    SELECT website
    FROM prospects.renewable_n_environment p
    WHERE website NOT IN (
        SELECT company_url
        FROM api_call_logs
        GROUP BY company_url
        HAVING COUNT(DISTINCT title) = 7  -- The number of titles you're checking
    );
    """

    # Execute query and load results into a pandas DataFrame
    with engine.connect() as connection:
        df = pd.read_sql(sql=query, con=connection.connection)

    # Convert DataFrame to list
    urls_list = df['website'].tolist()

    engine.dispose()

    return urls_list

def insert_data(data_tuples):
    conn = None
    try:
        with psycopg2.connect(
            host = os.getenv('DB_HOST'),
            database = os.getenv('DB_NAME'),
            user = os.getenv('DB_USER'),
            password = os.getenv('DB_PASSWORD'),
            port = os.getenv('DB_PORT')
        ) as conn:
            
            with conn.cursor() as cur:
                insert_query = """
                INSERT INTO solar_contacts_without_salesforce_founder
                (id, first_name, last_name, linkedin, title, email, email_status, id_tracker,
                website, company_linkedin, work_history)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::JSONB[])
                ON CONFLICT (id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                linkedin = EXCLUDED.linkedin,
                title = EXCLUDED.title,
                email = EXCLUDED.email,
                email_status = EXCLUDED.email_status,
                id_tracker = EXCLUDED.id_tracker,
                website = EXCLUDED.website,
                company_linkedin = EXCLUDED.company_linkedin,
                work_history = EXCLUDED.work_history;
                """

                formatted_data = [
                    (*t[:-1], [json.dumps(r) for r in t[-1]])
                    for t in data_tuples
                ]

                psycopg2.extras.execute_batch(cur, insert_query, formatted_data)
                conn.commit()

    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def status_logger(company_url, title, status='complete'):
    """Log an API call to the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Try to insert, if there's a conflict (same company_url and title), update the status
    upsert_query = """
    INSERT INTO api_call_logs (company_url, title, status, updated_at)
    VALUES (%s, %s, %s, NOW())
    ON CONFLICT (company_url, title) 
    DO UPDATE SET status = %s, updated_at = NOW();
    """
    
    cursor.execute(upsert_query, (company_url, title, status, status))
    conn.commit()
    cursor.close()
    conn.close()

def get_pending_titles_for_url(company_url):
    """Get titles that haven't been processed for the given URL"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_titles = ['founder', 'operations', 'technology', 'revenue', 'sales', 'project', 'implementation']
    
    # Query to find titles that have already been processed for this URL
    query = """
    SELECT title FROM api_call_log
    WHERE company_url = %s AND status = 'completed';
    """
    
    cursor.execute(query, (company_url,))
    completed_titles = [row[0] for row in cursor.fetchall()]
    
    # Find pending titles (titles that haven't been processed)
    pending_titles = [title for title in all_titles if title not in completed_titles]
    
    cursor.close()
    conn.close()
    
    return pending_titles

from dotenv import load_dotenv
load_dotenv()
titles = get_pending_titles_for_url('youtube.com')
print(titles)