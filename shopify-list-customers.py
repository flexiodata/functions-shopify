
# ---
# name: shopify-list-customers
# deployed: true
# title: Shopify Customers List
# description: Returns a list of customers from Shopify
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Returns" for a listing of the available properties.
#     required: false
# returns:
#   - name: id
#     type: string
#     description: A unique identifier for the customer.
#   - name: email
#     type: string
#     description: The unique email address of the customer.
#   - name: accepts_marketing
#     type: string
#     description: Whether the customer has consented to receive marketing material via email.
#   - name: created_at
#     type: string
#     description: The date and time (ISO 8601 format) when the customer was created.
#   - name: updated_at
#     type: string
#     description: The date and time (ISO 8601 format) when the customer information was last updated.
#   - name: first_name
#     type: string
#     description: The customer's first name.
#   - name: last_name
#     type: string
#     description: The customer's last name.
#   - name: orders_count
#     type: string
#     description: The number of orders associated with this customer.
#   - name: state
#     type: string
#     description: The state of the customer's account with a shop: **disabled**, **invited**, **enabled** or **declined**
#   - name: total_spent
#     type: string
#     description: The total amount of money that the customer has spent across their order history.
#   - name: note
#     type: string
#     description: A note about the customer.
#   - name: verified_email
#     type: string
#     description: Whether the customer has verified their email address.
#   - name: tax_exempt
#     type: string
#     description: Whether the customer is exempt from paying taxes on their order. If **true**, then taxes won't be applied to an order at checkout. If **false**, then taxes will be applied at checkout.
#   - name: phone
#     type: string
#     description: The unique phone number (E.164 format) for this customer.
#   - name: tags
#     type: string
#     description: Tags that the shop owner has attached to the customer, formatted as a string of comma-separated values.
#   - name: last_order_id
#     type: string
#     description: The ID of the customer's last order.
#   - name: last_order_name
#     type: string
#     description: The name of the customer's last order. This is directly related to the name field on the Order resource.
#   - name: currency
#     type: string
#     description: The three-letter code (ISO 4217 format) for the currency that the customer used when they paid for their last order.
#   - name: accepts_marketing_updated_at
#     type: string
#     description: The date and time (ISO 8601 format) when the customer consented or objected to receiving marketing material by email.
#   - name: marketing_opt_in_level
#     type: string
#     description: The marketing subscription opt-in level (as described by the M3AAWG best practices guideline) that the customer gave when they consented to receive marketing material by email: **single_opt_in**, **confirmed_opt_in** or **unknown**
#   - name: tax_exemptions
#     type: string
#     description: Whether the customer is exempt from paying specific taxes on their order. Canadian taxes only. See [Shopify customer properties](https://help.shopify.com/en/api/reference/customers/customer#index-2019-10) for a list of available values.
#   - name: address1
#     type: string
#     description: The first line of the customer's mailing address.
#   - name: address2
#     type: string
#     description: An additional field for the customer's mailing address.
#   - name: city
#     type: string
#     description: The customer's city, town, or village.
#   - name: company
#     type: string
#     description: The customer's company.
#   - name: country_code
#     type: string
#     description: The two-letter country code corresponding to the customer's country.
#   - name: country_name
#     type: string
#     description: The customer's normalized country name.
#   - name: province
#     type: string
#     description: The customer's region name. Typically a province, a state, or a prefecture.
#   - name: province_code
#     type: string
#     description: The two-letter code for the customer's region.
#   - name: zip
#     type: string
#     description: The customer's postal code, also known as zip, postcode, Eircode, etc.
# examples:
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
