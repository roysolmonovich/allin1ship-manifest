from flask import session
from utils import context, APICallService
import json
import config


def create_customer(excel_customer, req_context):
    """Create a customer object with customer data from a working dictionary"""
    full_name = excel_customer['Full Name']
    name_list = full_name.split(' ')
    first_name = name_list[0]
    last_name = name_list[-1]
    if len(name_list) > 2:
        middle_name = str(name_list[1:len(name_list) - 1])
    else:
        middle_name = ''

    # Create customer object
    customer = {
        'GivenName': first_name,
        'MiddleName': middle_name,
        'FamilyName': last_name,
        'PrimaryPhone': {
            'FreeFormNumber': excel_customer['Phone']
        },
        'PrimaryEmailAddr': {
            'Address': excel_customer['Email']
        }
    }

    uri = '/customer?minorversion=' + config.API_MINORVERSION
    response = APICallService.post_request(req_context, uri, customer)
    return response


def get_companyInfo(req_context):
    """Get CompanyInfo of connected QBO company"""
    uri = "/companyinfo/" + req_context.realm_id + "?minorversion=" + config.API_MINORVERSION
    response = APICallService.get_request(req_context, uri)
    return response


def query_customer_by_name(customer_name, req_context):
    if customer_name == 'all':
        uri = f"/query?query=SELECT * FROM Customer&minorversion={config.API_MINORVERSION}"
    else:
        customer_name = customer_name.replace('\'', r'\'')
        uri = f"/query?query=SELECT * FROM Customer WHERE CompanyName = '{customer_name}'&minorversion={config.API_MINORVERSION}"
    response = APICallService.get_request(req_context, uri)
    return response


def query_customer_by_id(customer_id, req_context):
    uri = f"/customer/{customer_id}"
    response = APICallService.get_request(req_context, uri)
    return response
