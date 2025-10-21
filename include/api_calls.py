# Libraries
from ratelimit import limits, sleep_and_retry
import requests
import os
from urllib.parse import quote
import logging
import time

# Internal Functions
from include.elt import extract_data_contact, extract_data_people
from include.database_calls import insert_data, status_logger

#@sleep_and_retry
#@limits(calls=150, period=60)

# Logger for api_call_info
api_logger = logging.getLogger("api_calls")
api_logger.setLevel(logging.INFO)

# File handler for api_calls_info
api_handler = logging.FileHandler("info_api_calls.log")
api_handler.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
api_handler.setFormatter(format)

api_logger.addHandler(api_handler)


def people_search_api(company_url,current_page):

    base_url = "https://api.apollo.io/api/v1/mixed_people/search"

    default_params = {
        'q_organization_domains': company_url,
        'page': current_page,
        'per_page': 100,
    }

    seniorities = ['owner', 'founder', 'c_suite', 'partner', 'vp', 'head', 'director']
    titles = ['IT']
    location = ['Egypt','Kenya', 'South Africa', 'Nigeria', 'Rwanda']
    #email_status = ["verified"]
    

    query_params = []
    query_params.extend([('organization_locations[]', l) for l in location])
    query_params.extend([('person_seniorities[]', s) for s in seniorities])
    query_params.extend([('person_titles[]', t) for t in titles])
    #query_params.extend([('contact_email_status[]', v) for v in email_status])
    

    query_params.extend(default_params.items())

    # Convert params to URL-encoded string
    query_string = '&'.join(f"{quote(str(k))}={quote(str(v))}" for k, v in query_params)
    url = f"{base_url}?{query_string}"

    headers = {
        "accept": "application/json",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "x-api-key": os.getenv('APOLLO_API_KEY')
    }

    try:
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            status_logger(company_url)
            # logging.info(f"API request succesful for {company_url}, page {current_page}")
        else:
            api_logger.warning(f"API request for {company_url}, page {current_page} returned status {response.status_code}")

        # Getting the limit hourly and minute limit
        rate_limit_hourly = int(response.headers.get("x-hourly-requests-left") or 0)
        rate_limit_minute = int(response.headers.get("x-minute-requests-left") or 0)
        rate_limit_daily = int(response.headers.get("x-24-hour-requests-left") or 0)

        response.raise_for_status()
        data = response.json() 

        return data, rate_limit_hourly, rate_limit_minute, rate_limit_daily
    
    except requests.exceptions.RequestException as e:
        api_logger.error(f"API request failed for {company_url} page {current_page}", exc_info=True)
        return None, 0, 0, 0

def calling_api(company_url, current_page):
    api_logger.info(f"API call starting for {company_url}, page  {current_page}")

    # Calling API
    response, rate_limit_hourly, rate_limit_minute, rate_limit_daily = people_search_api(company_url, current_page)

    # If API request failed, stop execution
    if response is None:
        api_logger.error("API request failed. Stopping execution.")
        return 0, 0, 0

    current_page = int(response['pagination']['page'])
    total_pages = int(response['pagination']['total_pages'])

    if response.get('contacts'):
        data_tuple = extract_data_contact(response, 'contacts')
        #print(data_tuple)
        insert_data(data_tuple)
        api_logger.info(f"Succesfully inserted {len(data_tuple)} contact records in the database")

    if response.get('people'):
        data_tuple = extract_data_people(response, 'people')
        #print(data_tuple)
        insert_data(data_tuple)
        api_logger.info(f"Succesfully inserted {len(data_tuple)} people records in the database")

    if (total_pages > 1) and (current_page < total_pages):  
        return calling_api(company_url, current_page + 1)
    
    # logging.info(f"API call completed for {company_url}")
    return rate_limit_hourly, rate_limit_minute, rate_limit_daily
