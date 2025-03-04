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

# API Rate Limits
daily_limit = 2000
hourly_limit = 400
minute_limit = 200

api_calls_daily_done = 0
api_calls_hourly_done = 0
api_calls_per_minute_done = 0 

def rate_limiter():
    global api_calls_daily_done, api_calls_hourly_done, api_calls_per_minute_done

    if api_calls_daily_done == daily_limit:
        time.sleep(86410)
        api_calls_daily_done = 0

    if api_calls_hourly_done == hourly_limit:
        time.sleep(3610)
        api_calls_hourly_done = 0

    if api_calls_per_minute_done == minute_limit:
        time.sleep(70)
        api_calls_per_minute_done = 0


def people_search_api(company_url, current_page):
    global api_calls_daily_done, api_calls_hourly_done, api_calls_per_minute_done

    base_url = "https://api.apollo.io/api/v1/mixed_people/search"

    default_params = {
        'q_organization_domains': company_url,
        'page': current_page,
        'per_page': 100,
        'person_seniorities[]': ["owner"]
    }

    # Convert params to URL-encoded string
    query_string = '&'.join(f"{quote(str(k))}={quote(str(v))}" for k, v in default_params.items())
    url = f"{base_url}?{query_string}"

    headers = {
        "accept": "application/json",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "x-api-key": os.getenv('API_KEY')
    }

    rate_limiter()

    try:
        response = requests.post(url, headers=headers)

        api_calls_daily_done += 100
        api_calls_hourly_done += 100
        api_calls_per_minute_done += 100

        if response.status_code == 200:
            print('updating status')
            status_logger(company_url)
        response.raise_for_status()
        data = response.json()
        logging.info(f'API request successful for {company_url} page {current_page}')
        return data
    except requests.exceptions.RequestException as e:
        print('API Error:', e)
        return None

def calling_api(company_url):
    response = people_search_api(company_url)
    if response:
        current_page = int(response['pagination']['page'])
        total_pages = response['pagination']['total_pages']

    if response['contacts']:
        data_tuple = extract_data_contact(response, 'contacts')
        insert_data(data_tuple)


    if response['people']:
        data_tuple = extract_data_people(response, 'people')
        insert_data(data_tuple)

    if (total_pages > 1) and (current_page < total_pages): 
        calling_api(company_url, current_page + 1)