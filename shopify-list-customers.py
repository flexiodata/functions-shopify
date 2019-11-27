
# ---
# name: shopify-list-customers
# deployed: true
# title: Shopify Products List
# description: Returns a list of customers from Shopify
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Notes" for a listing of the available properties.
#     required: false
# examples:
# notes: |
#   The following properties are available:
#     * `id`: A unique identifier for the customer.
#     * `email`: The unique email address of the customer.
#     * `accepts_marketing`: Whether the customer has consented to receive marketing material via email.
#     * `created_at`: The date and time (ISO 8601 format) when the customer was created.
#     * `updated_at`: The date and time (ISO 8601 format) when the customer information was last updated.
#     * `first_name`: The customer's first name.
#     * `last_name`: The customer's last name.
#     * `orders_count`: The number of orders associated with this customer.
#     * `state`: The state of the customer's account with a shop: **disabled**, **invited**, **enabled** or **declined**
#     * `total_spent`: The total amount of money that the customer has spent across their order history.
#     * `note`: A note about the customer.
#     * `verified_email`: Whether the customer has verified their email address.
#     * `tax_exempt`: Whether the customer is exempt from paying taxes on their order. If **true**, then taxes won't be applied to an order at checkout. If **false**, then taxes will be applied at checkout.
#     * `phone`: The unique phone number (E.164 format) for this customer.
#     * `tags`: Tags that the shop owner has attached to the customer, formatted as a string of comma-separated values.
#     * `last_order_id`: The ID of the customer's last order.
#     * `last_order_name`: The name of the customer's last order. This is directly related to the name field on the Order resource.
#     * `currency`: The three-letter code (ISO 4217 format) for the currency that the customer used when they paid for their last order.
#     * `accepts_marketing_updated_at`: The date and time (ISO 8601 format) when the customer consented or objected to receiving marketing material by email.
#     * `marketing_opt_in_level`: The marketing subscription opt-in level (as described by the M3AAWG best practices guideline) that the customer gave when they consented to receive marketing material by email: **single_opt_in**, **confirmed_opt_in** or **unknown**
#     * `tax_exemptions`: Whether the customer is exempt from paying specific taxes on their order. Canadian taxes only. See [Shopify customer properties](https://help.shopify.com/en/api/reference/customers/customer#index-2019-10) for a list of available values.
#     * `address1`: The first line of the customer's mailing address.
#     * `address2`: An additional field for the customer's mailing address.
#     * `city`: The customer's city, town, or village.
#     * `company`: The customer's company.
#     * `country_code`: The two-letter country code corresponding to the customer's country.
#     * `country_name`: The customer's normalized country name.
#     * `province`: The customer's region name. Typically a province, a state, or a prefecture.
#     * `province_code`: The two-letter code for the customer's region.
#     * `zip`: The customer's postal code, also known as zip, postcode, Eircode, etc.
# ---

import json
import requests
import urllib
import itertools
from datetime import *
from cerberus import Validator
from collections import OrderedDict

# main function entry point
def flexio_handler(flex):

    # get the api key from the variable input
    auth_token = dict(flex.vars).get('shopify_connection',{}).get('access_token')
    if auth_token is None:
        flex.output.content_type = "application/json"
        flex.output.write([[""]])
        return

    # get the company domain from the variable input
    api_base_uri = dict(flex.vars).get('shopify_connection',{}).get('api_base_uri')
    if api_base_uri is None:
        flex.output.content_type = "application/json"
        flex.output.write([[""]])
        return

    # get the input
    input = flex.input.read()
    try:
        input = json.loads(input)
        if not isinstance(input, list): raise ValueError
    except ValueError:
        raise ValueError

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': '*'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    # if the input is valid return an error
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    # map this function's property names to the API's property names
    property_map = OrderedDict()
    property_map['id'] = 'id'
    property_map['email'] = 'email'
    property_map['accepts_marketing'] = 'accepts_marketing'
    property_map['created_at'] = 'created_at'
    property_map['updated_at'] = 'updated_at'
    property_map['first_name'] = 'first_name'
    property_map['last_name'] = 'last_name'
    property_map['orders_count'] = 'orders_count'
    property_map['state'] = 'state'
    property_map['total_spent'] = 'total_spent'
    property_map['note'] = 'note'
    property_map['verified_email'] = 'verified_email'
    #property_map['multipass_identifier'] = 'multipass_identifier'
    property_map['tax_exempt'] = 'tax_exempt'
    property_map['phone'] = 'phone'
    property_map['tags'] = 'tags'
    property_map['last_order_id'] = 'last_order_id'
    property_map['last_order_name'] = 'last_order_name'
    property_map['currency'] = 'currency'
    property_map['accepts_marketing_updated_at'] = 'accepts_marketing_updated_at'
    property_map['marketing_opt_in_level'] = 'marketing_opt_in_level'
    property_map['tax_exemptions'] = 'tax_exemptions'

    # these all come from nested values in the 'default_address' object
    property_map['address1'] = 'default_address.address1'
    property_map['address2'] = 'default_address.address2'
    property_map['city'] = 'default_address.city'
    property_map['company'] = 'default_address.company'
    #property_map['country'] = 'default_address.country'
    property_map['country_code'] = 'default_address.country_code'
    property_map['country_name'] = 'default_address.country_name'
    property_map['province'] = 'default_address.province'
    property_map['province_code'] = 'default_address.province_code'
    property_map['zip'] = 'default_address.zip'

    try:

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # if we have a wildcard, get all the properties
        if len(properties) == 1 and properties[0] == '*':
            properties = list(property_map.keys())

        # list of the Shopify properties we'd like to query
        shopify_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://help.shopify.com/en/api/reference/customers/customer#index-2019-10
        url = api_base_uri + '/admin/api/2019-10/customers.json'
        headers = {
            'X-Shopify-Access-Token': auth_token
        }

        # get the response data as a JSON object
        response = requests.get(url, headers=headers)
        content = response.json()

        # return the info
        result = []
        result.append(properties)

        # build up each row and append it to the result
        customers = content.get('customers',[])
        for customer in customers:
            row = []
            for p in shopify_properties:
                row.append(deep_get(customer,p,'') or '')
            result.append(row)

        # return the results
        result = json.dumps(result, default=to_string)
        flex.output.content_type = "application/json"
        flex.output.write(result)

    except:
        raise RuntimeError

def safe_list_get(_list, idx, default):
    try:
        return _list[idx]
    except IndexError:
        return default

def deep_get(obj, path, default=None):
    try:
        keys = path.split('.')
        for key in keys:
            if key.find('[') >= 0:
                parts = key.split('[')
                key = parts[0]
                obj = obj.get(key, default)
                if isinstance(obj, list):
                    idx = int(parts[1].strip(']'))
                    obj = safe_list_get(obj, idx, default)
                else:
                    return default
            elif isinstance(obj, dict):
                obj = obj.get(key, default)
            else:
                return default
        return obj

    except:
        return default

def validator_list(field, value, error):
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                error(field, 'Must be a list with only string values')
        return
    error(field, 'Must be a string or a list of strings')

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (int)):
        return str(value)
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None
