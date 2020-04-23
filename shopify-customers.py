
# ---
# name: shopify-customers
# deployed: true
# config: index
# title: Shopify Customers
# description: Returns a list of customers from Shopify
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Returns" for a listing of the available properties.
#     required: false
#   - name: filter
#     type: string
#     description: Filter to apply with key/values specified as a URL query string where the keys correspond to the properties to filter.
#     required: false
# returns:
#   - name: id
#     type: integer
#     description: The unique identifier for the customer
#   - name: first_name
#     type: string
#     description: The first name of the customer
#   - name: last_name
#     type: string
#     description: The last name of the customer
#   - name: email
#     type: string
#     description: The email for the customer
#   - name: verified_email
#     type: boolean
#     description: Whether or not the email for the customer is verified
#   - name: phone
#     type: string
#     description: The phone number for the customer
#   - name: created_at
#     type: string
#     description: The date when the customer was created
#   - name: updated_at
#     type: string
#     description:  The date the information for the customer was last updated
#   - name: state
#     type: string
#     description: The state of the shop for the customer
#   - name: tax_exempt
#     type: boolean
#     description: Whether or not the customer is exempt from paying taxes on their order
#   - name: tax_exemptions
#     type: string
#     description: List of specific tax exemptions for the customer
#   - name: orders_count
#     type: integer
#     description: The number of orders associated with the customer
#   - name: total_spent
#     type: integer
#     description: The total amount spent by the customer
#   - name: currency
#     type: string
#     description: The currency that the customer used when they paid for their last order
#   - name: last_order_id
#     type: integer
#     description: The id of the last order for the customer
#   - name: last_order_name
#     type: string
#     description: The name of the last order for the customer
#   - name: accepts_marketing
#     type: boolean
#     description: Whether the customer has consented to receive marketing material via email
#   - name: marketing_opt_in_level
#     type: string
#     description: The marketing subscription opt-in level that the customer gave when they consented to receive marketing material by email
#   - name: accepts_marketing_updated_at
#     type: string
#     description: The date when the customer consented or objected to receiving marketing material by email
#   - name: note
#     type: string
#     description: A note about the customer
#   - name: tags
#     type: string
#     description: The tags associated with the customer given as a comma-delimited list
#   - name: address_id
#     type: integer
#     description: The identifier for the default address for the customer
#   - name: address_customer_id
#     type: integer
#     description: The customer identifier for the default address for the customer
#   - name: address_first_name
#     type: string
#     description: The first name listed for the default address for the customer
#   - name: address_last_name
#     type: string
#     description: The last name listed for the default address for the customer
#   - name: address_name
#     type: string
#     description: The name listed for the default address for the customer
#   - name: address_phone
#     type: string
#     description: The phone number associated with the default address for the customer
#   - name: address_company
#     type: string
#     description: The company listed for the default address for the customer
#   - name: address1
#     type: string
#     description: The first line of the default address for the customer
#   - name: address2
#     type: string
#     description: The second line of the default address for the customer
#   - name: address_city
#     type: string
#     description: The city of the default address for the customer
#   - name: address_province
#     type: string
#     description: The province of the default address for the customer
#   - name: address_province_code
#     type: string
#     description: The province code of the default address for the customer
#   - name: address_zip
#     type: string
#     description: The postal code of the default address for the customer
#   - name: address_country
#     type: string
#     description: The country of the default address for the customer
#   - name: address_country_name
#     type: string
#     description: The normalized country name of the default address for the customer
#   - name: address_country_code
#     type: string
#     description: The country code of the default address for the customer
# examples:
#   - '""'
#   - '"id, email, first_name, last_name"'
# ---

import json
import urllib
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import *
from decimal import *
from collections import OrderedDict

# main function entry point
def flexio_handler(flex):

    flex.output.content_type = 'application/x-ndjson'
    for item in get_data(flex.vars):
        result = json.dumps(item, default=to_string) + "\n"
        flex.output.write(result)

def get_data(params):

    # get the api key and company domain from the variable input
    auth_token = dict(params).get('shopify_connection',{}).get('access_token')
    api_base_uri = dict(params).get('shopify_connection',{}).get('api_base_uri')

    # see here for more info:
    # https://shopify.dev/docs/admin-api/rest/reference/customers/customer#index-2020-04
    # https://shopify.dev/tutorials/make-paginated-requests-to-rest-admin-api

    headers = {
        'X-Shopify-Access-Token': auth_token
    }
    url = api_base_uri + '/admin/api/2020-04/customers.json'

    page_size = 250
    url_query_params = {'limit': page_size}
    url_query_str = urllib.parse.urlencode(url_query_params)
    page_url = url + '?' + url_query_str

    while True:

        response = requests_retry_session().get(page_url, headers=headers)
        response.raise_for_status()
        content = response.json()
        data = content.get('customers',[])

        if len(data) == 0: # sanity check in case there's an issue with cursor
            break

        # build up each row and append it to the result
        for item in data:
            yield get_item_info(item)

        page_url = response.links.get('next',{}).get('url')
        if page_url is None:
            break

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def to_date(value):
    # TODO: convert if needed
    return value

def to_number(value):
    try:
        v = value
        return float(v)
    except ValueError:
        return value

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (int)):
        return str(value)
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def get_item_info(item):

    # map this function's property names to the API's property names
    info = OrderedDict()

    info['id'] = item.get('id')
    info['first_name'] = item.get('first_name')
    info['last_name'] = item.get('last_name')
    info['email'] = item.get('email')
    info['verified_email'] = item.get('verified_email')
    info['phone'] = item.get('phone')
    info['created_at'] = to_date(item.get('created_at'))
    info['updated_at'] = to_date(item.get('updated_at'))
    info['state'] = item.get('state')
    info['tax_exempt'] = item.get('tax_exempt')
    info['tax_exemptions'] = ', '.join(item.get('tax_exemptions',[])) # convert to comma-delimited string; space follows api convention in tags property
    info['orders_count'] = item.get('orders_count')
    info['total_spent'] = to_number(item.get('total_spent'))
    info['currency'] = item.get('currency')
    info['last_order_id'] = item.get('last_order_id')
    info['last_order_name'] = item.get('last_order_name')
    info['accepts_marketing'] = item.get('accepts_marketing')
    info['marketing_opt_in_level'] = item.get('marketing_opt_in_level')
    info['accepts_marketing_updated_at'] = to_date(item.get('accepts_marketing_updated_at'))
    info['note'] = item.get('note')
    info['tags'] = item.get('tags')
    info['address_id'] = item.get('default_address',{}).get('id')
    info['address_customer_id'] = item.get('default_address',{}).get('customer_id')
    info['address_first_name'] = item.get('default_address',{}).get('first_name')
    info['address_last_name'] = item.get('default_address',{}).get('last_name')
    info['address_name'] = item.get('default_address',{}).get('name')
    info['address_phone'] = item.get('default_address',{}).get('phone')
    info['address_company'] = item.get('default_address',{}).get('company')
    info['address_street1'] = item.get('default_address',{}).get('address1')
    info['address_street2'] = item.get('default_address',{}).get('address2')
    info['address_city'] = item.get('default_address',{}).get('city')
    info['address_province'] = item.get('default_address',{}).get('province')
    info['address_province_code'] = item.get('default_address',{}).get('province_code')
    info['address_zip'] = item.get('default_address',{}).get('zip')
    info['address_country'] = item.get('default_address',{}).get('country')
    info['address_country_name'] = item.get('default_address',{}).get('country_name')
    info['address_country_code'] = item.get('default_address',{}).get('country_code')

    return info
