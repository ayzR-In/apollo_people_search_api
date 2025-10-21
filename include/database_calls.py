import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras
import json
import os
import logging



# Logger for api_call_info
api_logger = logging.getLogger("api_calls")
api_logger.setLevel(logging.INFO)

# File handler for api_calls_info
api_handler = logging.FileHandler("info_api_calls.log")
api_handler.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
api_handler.setFormatter(format)

api_logger.addHandler(api_handler)


def get_db_connection():
    """Create and return a database connection"""
    conn = psycopg2.connect(
        host = os.getenv('DB_HOST'),
        database = os.getenv('DB_NAME'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
        port = 6203
    )
    return conn

def get_urls():
    # Create SQLAlchemy engine
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(connection_string)

    # SQL query
    query = """
    SELECT website
    FROM prospects.chaitanya_egypt_sa_comapnies p
    WHERE website NOT IN ('NaN') 
    AND website NOT IN (
        SELECT company_url
        FROM public.api_call_logs_chaitanya_egypt_sa_companies
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
                INSERT INTO contacts.chaitanya_egypt_sa_data
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
        api_logger.error(f"Error inserting the contact")
    finally:
        if conn is not None:
            conn.close()

def status_logger(company_url):
    """Log an API call to the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Try to insert, if there's a conflict (same company_url and title), update the status
    upsert_query = """
    INSERT INTO public.api_call_logs_chaitanya_egypt_sa_companies (company_url, created_date, updated_at)
    VALUES (%s, NOW(), NOW())
    ON CONFLICT (company_url) 
    DO UPDATE SET updated_at = NOW();
    """
    
    cursor.execute(upsert_query, (company_url,))
    conn.commit()
    # logging.info(f"Succesfully logged API call for: {company_url}")
    cursor.close()
    conn.close()
