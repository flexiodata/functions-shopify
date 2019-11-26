
# ---
# name: shopify-list-products
# deployed: true
# title: Shopify Products List
# description: Returns a list of products from Shopify
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Notes" for a listing of the available properties.
#     required: false
# examples:
# notes: |
#   The following properties are available:
#     * `id`: TODO
#     * `title`: TODO
#     * `body_html`: TODO
#     * `vendor`: TODO
#     * `product_type`: TODO
#     * `created_at`: TODO
#     * `handle`: TODO
#     * `updated_at`: TODO
#     * `published_at`: TODO
#     * `template_suffix`: TODO
#     * `tags`: TODO
#     * `published_scope`: TODO
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
    property_map['id']                   = 'id'
    property_map['title']                = 'title'
    property_map['body_html']            = 'body_html'
    property_map['vendor']               = 'vendor'
    property_map['product_type']         = 'product_type'
    property_map['created_at']           = 'created_at'
    property_map['handle']               = 'handle'
    property_map['updated_at']           = 'updated_at'
    property_map['published_at']         = 'published_at'
    property_map['template_suffix']      = 'template_suffix'
    property_map['tags']                 = 'tags'
    property_map['published_scope']      = 'published_scope'

    try:

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # if we have a wildcard, get all the properties
        if len(properties) == 1 and properties[0] == '*':
            properties = list(property_map.keys())

        # list of the Shopify properties we'd like to query
        shopify_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://help.shopify.com/en/api/reference/products/product#index-2019-10
        url = api_base_uri + '/admin/api/2019-10/products.json'
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
        products = content.get('products',[])
        for product in products:
            row = []
            for p in shopify_properties:
                row.append(product.get(p,'') or '')
            result.append(row)

        # return the results
        result = json.dumps(result, default=to_string)
        flex.output.content_type = "application/json"
        flex.output.write(result)

    except:
        raise RuntimeError

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
