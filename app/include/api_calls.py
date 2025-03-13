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

def people_search_api(company_url,current_page):

    base_url = "https://api.apollo.io/api/v1/mixed_people/search"

    default_params = {
        'q_organization_domains': company_url,
        'page': current_page,
        'per_page': 100,
    }

    seniorities = ['owner', 'founder', 'c_suite', 'partner', 'vp', 'head', 'director', 'manager']
    titles = ['Founder', 'Operations', 'Technology', 'Revenue', 'Sales', 'Project', 'Implementation']

    query_params = []
    query_params.extend([('person_seniorities[]', s) for s in seniorities])
    query_params.extend([('person_titles[]', t) for t in titles])

    query_params.extend(default_params.items())

    # Convert params to URL-encoded string
    query_string = '&'.join(f"{quote(str(k))}={quote(str(v))}" for k, v in query_params)
    url = f"{base_url}?{query_string}"

    headers = {
        "accept": "application/json",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "x-api-key": 'QqptYKkMpzTBN5q266dKpA'
    }

    

    try:
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            status_logger(company_url)

        # Getting the limit hourly and minute limit
        rate_limit_hourly = int(response.headers.get("x-hourly-requests-left"))
        rate_limit_minute = int(response.headers.get("x-minute-requests-left"))

        response.raise_for_status()
        data = response.json()
        logging.info(f'API request successful for {company_url} page {current_page}')
        return data, rate_limit_hourly, rate_limit_minute
    except requests.exceptions.RequestException as e:
        print('API Error:', e)
        return None

def calling_api(company_url, current_page):

    # Calling API
    response, rate_limit_hourly, rate_limit_minute = people_search_api(company_url, current_page)

    # If API request failed, stop execution
    if response is None:
        logging.error("API request failed. Stopping execution.")
        return

    current_page = int(response['pagination']['page'])
    total_pages = response['pagination']['total_pages']

    if response.get('contacts'):
        data_tuple = extract_data_contact(response, 'contacts')
        insert_data(data_tuple)

    if response.get('people'):
        data_tuple = extract_data_people(response, 'people')
        insert_data(data_tuple)

    if (total_pages > 1) and (current_page < total_pages):  
        calling_api(company_url, current_page + 1)
    
    return rate_limit_hourly, rate_limit_minute
