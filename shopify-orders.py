
# ---
# name: shopify-orders
# deployed: true
# config: index
# title: Shopify Orders
# description: Returns a list of orders from Shopify
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
#     description: The id of the order
#   - name: app_id
#     type: integer
#     description: The id of the app that created the order
#   - name: customer_id
#     type: integer
#     description: The id associated with the customer placing the order
#   - name: billing_address_first_name
#     type: string
#     -description: The first name of the person associated with the payment method
#   - name: billing_address_last_name
#     type: string
#     -description: The last name of the person associated with the payment method
#   - name: billing_address_name
#     type: string
#     -description: The full name of the person associated with the payment method
#   - name: billing_address_phone
#     type: string
#     description: The phone number at the billing address
#   - name: billing_address_company
#     type: string
#     description: The company of the person associated with the billing address
#   - name: billing_address_street1
#     type: string
#     description: The street address of the billing address
#   - name: billing_address_street2
#     type: string
#     description: An optional additional field for the street address of the billing address
#   - name: billing_address_city
#     type: string
#     description: The city, town, or village of the billing address
#   - name: billing_address_province
#     type: string
#     -description: The name of the region of the billing address
#   - name: billing_address_province_code
#     type: string
#     description: The two-letter abbreviation of the region of the billing address
#   - name: billing_address_zip
#     type: string
#     description: The postal code of the billing address
#   - name: billing_address_country
#     type: string
#     -description: The name of the country of the billing address
#   - name: billing_address_country_code
#     type: string
#     description: The two-letter code (ISO 3166-1 format) for the country of the billing address
#   - name: billing_address_latitude
#     type: number
#     description: The latitude of the billing address
#   - name: billing_address_longitude
#     type: number
#     description: The longitude of the billing address
#   - name: shipping_address_first_name
#     type: string
#     -description: The first name of the person associated with the shipping address
#   - name: shipping_address_last_name
#     type: string
#     -description: The last name of the person associated with the shipping address
#   - name: shipping_address_name
#     type: string
#     -description: The full name of the person associated with the shipping address
#   - name: shipping_address_phone
#     type: string
#     description: The phone number at the shipping address
#   - name: shipping_address_company
#     type: string
#     description: The company of the person associated with the shipping address
#   - name: shipping_address_street1
#     type: string
#     description: The street address of the shipping address
#   - name: shipping_address_street2
#     type: string
#     description: An optional additional field for the street address of the shipping address
#   - name: shipping_address_city
#     type: string
#     description: The city, town, or village of the shipping address
#   - name: shipping_address_province
#     type: string
#     -description: The name of the region of the shipping address
#   - name: shipping_address_province_code
#     type: string
#     description: The two-letter abbreviation of the region of the shipping address
#   - name: shipping_address_zip
#     type: string
#     description: The postal code of the shipping address
#   - name: shipping_address_country
#     type: string
#     -description: The name of the country of the shipping address
#   - name: shipping_address_country_code
#     type: string
#     description: The two-letter code (ISO 3166-1 format) for the country of the shipping address
#   - name: shipping_address_latitude
#     type: number
#     description: The latitude of the shipping address
#   - name: shipping_address_longitude
#     type: number
#     description: The longitude of the shipping address
#   - name: created_at
#     type: string
#     description: The date and time when the order was created
#   - name: updated_at
#     type: string
#     description: The date and time when the order was last modified
#   - name: processed_at
#     type: string
#     description: The date and time when an order was processed
#   - name: cancelled_at
#     type: string
#     description: The date and time when the order was canceled
#   - name: closed_at
#     type: string
#     description: The date and time when the order was closed
#   - name: currency
#     type: string
#     description: The three-letter code (ISO 4217 format) for the shop currency
#   - name: total_weight
#     type: integer
#     description: The sum of all line item weights in grams
#   - name: total_line_items_price
#     type: number
#     description: The sum of all line item prices in the shop currency
#   - name: total_discounts
#     type: number
#     description: The total discounts applied to the price of the order in the shop currency
#   - name: subtotal_price
#     type: number
#     description: The price of the order in the shop currency after discounts but before shipping, taxes, and tips
#   - name: total_shipping
#     type: number
#     description: The sum of all the shipping amoutns in the order in the shop currency
#   - name: total_tip_received
#     type: number
#     description: The sum of all the tips in the order in the shop currency
#   - name: total_tax
#     type: number
#     description: The sum of all line item prices, discounts, shipping, taxes, and tips in the shop currency
#   - name: total_price
#     type: number
#     description: The sum of all line item prices, discounts, shipping, taxes, and tips in the shop currency
# examples:
#   - '""'
#   - '"id, customer_id, created_at, total_price"'
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
    for data in get_data(flex.vars):
        flex.output.write(data)

def get_data(params):

    # get the api key and company domain from the variable input
    auth_token = dict(params).get('shopify_connection',{}).get('access_token')
    api_base_uri = dict(params).get('shopify_connection',{}).get('api_base_uri')

    # see here for more info:
    # https://shopify.dev/docs/admin-api/rest/reference/orders/order#index-2020-04
    # https://shopify.dev/tutorials/make-paginated-requests-to-rest-admin-api

    headers = {
        'X-Shopify-Access-Token': auth_token
    }
    url = api_base_uri + '/admin/api/2020-04/orders.json'

    # api call defaults to open orders, so use 'any' status to get everything; also note:
    # only last 60 days or orders are available with current oauth scope; additional oauth
    # scope and app approval required for orders past 60 days; see:
    # https://shopify.dev/tutorials/authenticate-a-public-app-with-oauth#orders-permissions
    page_size = 250
    url_query_params = {'limit': page_size, 'status': 'any'}
    url_query_str = urllib.parse.urlencode(url_query_params)
    page_url = url + '?' + url_query_str

    while True:

        response = requests_retry_session().get(page_url, headers=headers)
        response.raise_for_status()
        content = response.json()
        data = content.get('orders',[])

        if len(data) == 0: # sanity check in case there's an issue with cursor
            break

        buffer = ''
        for item in data:
            item = get_item_info(item)
            buffer = buffer + json.dumps(item, default=to_string) + "\n"
        yield buffer

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
    if value is None:
        return value
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
    info['app_id'] = item.get('app_id')
    info['customer_id'] = (item.get('customer') or {}).get('id')
    info['billing_address_first_name'] = (item.get('billing_address') or {}).get('first_name')
    info['billing_address_last_name'] = (item.get('billing_address') or {}).get('last_name')
    info['billing_address_name'] = (item.get('billing_address') or {}).get('name')
    info['billing_address_phone'] = (item.get('billing_address') or {}).get('phone')
    info['billing_address_company'] = (item.get('billing_address') or {}).get('company')
    info['billing_address_street1'] = (item.get('billing_address') or {}).get('address1')
    info['billing_address_street2'] = (item.get('billing_address') or {}).get('address2')
    info['billing_address_city'] = (item.get('billing_address') or {}).get('city')
    info['billing_address_province'] = (item.get('billing_address') or {}).get('province')
    info['billing_address_province_code'] = (item.get('billing_address') or {}).get('province_code')
    info['billing_address_zip'] = (item.get('billing_address') or {}).get('zip')
    info['billing_address_country'] = (item.get('billing_address') or {}).get('country')
    info['billing_address_country_code'] = (item.get('billing_address') or {}).get('country_code')
    info['billing_address_latitude'] = to_number((item.get('billing_address') or {}).get('latitude'))
    info['billing_address_longitude'] = to_number((item.get('billing_address') or {}).get('longitude'))
    info['shipping_address_first_name'] = (item.get('shipping_address') or {}).get('first_name')
    info['shipping_address_last_name'] = (item.get('shipping_address') or {}).get('last_name')
    info['shipping_address_name'] = (item.get('shipping_address') or {}).get('name')
    info['shipping_address_phone'] = (item.get('shipping_address') or {}).get('phone')
    info['shipping_address_company'] = (item.get('shipping_address') or {}).get('company')
    info['shipping_address_street1'] = (item.get('shipping_address') or {}).get('address1')
    info['shipping_address_street2'] = (item.get('shipping_address') or {}).get('address2')
    info['shipping_address_city'] = (item.get('shipping_address') or {}).get('city')
    info['shipping_address_province'] = (item.get('shipping_address') or {}).get('province')
    info['shipping_address_province_code'] = (item.get('shipping_address') or {}).get('province_code')
    info['shipping_address_zip'] = (item.get('shipping_address') or {}).get('zip')
    info['shipping_address_country'] = (item.get('shipping_address') or {}).get('country')
    info['shipping_address_country_code'] = (item.get('shipping_address') or {}).get('country_code')
    info['shipping_address_latitude'] = to_number((item.get('shipping_address') or {}).get('latitude'))
    info['shipping_address_longitude'] = to_number((item.get('shipping_address') or {}).get('longitude'))
    info['created_at'] = to_date(item.get('created_at'))
    info['updated_at'] = to_date(item.get('updated_at'))
    info['processed_at'] = to_date(item.get('processed_at'))
    info['cancelled_at'] = to_date(item.get('cancelled_at'))
    info['closed_at'] = to_date(item.get('closed_at'))
    info['currency'] = item.get('currency')
    info['total_weight'] = item.get('total_weight')
    info['total_line_items_price'] = to_number(item.get('total_line_items_price'))
    info['total_discounts'] = to_number(item.get('total_discounts'))
    info['subtotal_price'] = to_number(item.get('subtotal_price'))
    info['total_shipping'] = to_number((item.get('total_shipping_price_set') or {}).get('shop_money',{}).get('amount'))
    info['total_tip_received'] = to_number(item.get('total_tip_received'))
    info['total_tax'] = to_number(item.get('total_tax'))
    info['total_price'] = to_number(item.get('total_price'))

    return info
