# Libraries
import pandas as pd

def extract_data_contact(response, contact_variable):
    data_tuples = []
    try:
        for i in range(len(response[contact_variable])):
            id = response[contact_variable][i]['id']
            first_name = response[contact_variable][i]['first_name']
            last_name = response[contact_variable][i]['last_name']
            linkedIn = response[contact_variable][i]['linkedin_url']
            title = response[contact_variable][i]['title']
            email = response[contact_variable][i]['email']
            email_status = response[contact_variable][i]['email_status']
            id_tracker = 'contact'
            website = response[contact_variable][i]['account']['website_url']
            company_linkedin = response[contact_variable][i]['account']['linkedin_url']
    
        records = []
        for j in range(len(response[contact_variable][i]['employment_history'])):
            record = {
                'company_name': response[contact_variable][i]['employment_history'][j]['organization_name'],
                'org_id': response[contact_variable][i]['employment_history'][j]['_id'],
                'title': response[contact_variable][i]['employment_history'][j]['title'],
                'start_date': response[contact_variable][i]['employment_history'][j]['start_date'],
                'end_date': response[contact_variable][i]['employment_history'][j]['end_date'],
            }
            records.append(record)
        thistuple = (id, first_name, last_name, linkedIn, title, email, email_status, id_tracker, website, company_linkedin, records)
        data_tuples.append(thistuple)
    except Exception as error:
        print(error)
    
    return data_tuples

def extract_data_people(response, contact_variable):
    data_tuples = []
    try:
        for i in range(len(response[contact_variable])):
            id = response[contact_variable][i]['id']
            first_name = response[contact_variable][i]['first_name']
            last_name = response[contact_variable][i]['last_name']
            linkedIn = response[contact_variable][i]['linkedin_url']
            title = response[contact_variable][i]['title']
            email = response[contact_variable][i]['email']
            email_status = ''
            id_tracker = 'people'
            website = response[contact_variable][i]['account']['website_url']
            company_linkedin = response[contact_variable][i]['account']['linkedin_url']
    
        records = []
        for j in range(len(response[contact_variable][i]['employment_history'])):
            record = {
                'company_name': response[contact_variable][i]['employment_history'][j]['organization_name'],
                'org_id': response[contact_variable][i]['employment_history'][j]['_id'],
                'title': response[contact_variable][i]['employment_history'][j]['title'],
                'start_date': response[contact_variable][i]['employment_history'][j]['start_date'],
                'end_date': response[contact_variable][i]['employment_history'][j]['end_date'],
            }
            records.append(record)
        thistuple = (id, first_name, last_name, linkedIn, title, email, email_status, id_tracker, website, company_linkedin, records)
        data_tuples.append(thistuple)
    except Exception as error:
        print(error)
    
    return data_tuples

"""def default_params(company_url, current_page):
    
    titles = ['Founder', 'Operations', 'Technology', 'Revenue', 'Sales', 'Project', 'Implementation']
    
    default_params = {
        'q_organization_domains': company_url,
        'page': current_page,
        'per_page': 100
    }

    # Append new key-value pairs
    default_params['person_titles[]'] = titles[0]
    default_params['person_titles[]'] = titles[1]"""

    