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
        database='mydatabase',
        user='myuser',
        password='mypassword',
        port=5432
    )
    return conn

def get_urls():
    # Create SQLAlchemy engine
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(connection_string)

    # SQL query
    query = """
    SELECT website
    FROM company_data p
    WHERE website NOT IN (
        SELECT company_url
        FROM api_call_logs
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
                INSERT INTO api_contacts
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

def status_logger(company_url):
    """Log an API call to the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Try to insert, if there's a conflict (same company_url and title), update the status
    upsert_query = """
    INSERT INTO api_call_logs (company_url, created_date, updated_at)
    VALUES (%s, NOW(), NOW())
    ON CONFLICT (company_url) 
    DO UPDATE SET updated_at = NOW();
    """
    
    cursor.execute(upsert_query, (company_url,))
    conn.commit()
    cursor.close()
    conn.close()
