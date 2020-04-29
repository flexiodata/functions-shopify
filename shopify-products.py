
# ---
# name: shopify-products
# deployed: true
# config: index
# title: Shopify Products
# description: Returns a list of products from Shopify
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
#     description: The unique identifer for the product
#   - name: title
#     type: string
#     description: The title for the product
#   - name: body_html
#     type: string
#     description: A description of the product
#   - name: handle
#     type: string
#     description: A human-friendly string for the product
#   - name: vendor
#     type: string
#     description: The name of the vendor of the product
#   - name: product_type
#     type: string
#     description: A categorization of the product used for filtering and searching products
#   - name: created_at
#     type: string
#     description: The date the product was created
#   - name: updated_at
#     type: string
#     description: The date the product was last updated
#   - name: published_at
#     type: string
#     description: The date the product was published
#   - name: published_scope
#     type: string
#     description: The channel to which the product is published
#   - name: template_suffix
#     type: string
#     description: The suffix of the Liquid tempate used for the product page
#   - name: tags
#     type: string
#     description: A comma-separated list of tags that are used for filtering and searching products
#   - name: variant_id
#     type: integer
#     description: The unique identifier for a product variant
#   - name: variant_title
#     type: string
#     description: The title of the product variant
#   - name: variant_option1
#     type: string
#     description: A custom property used to define product variants
#   - name: variant_option2
#     type: string
#     description: A custom property used to define product variants
#   - name: variant_option3
#     type: string
#     description: A custom property used to define product variants
#   - name: variant_created_at
#     type: string
#     description: The date the product variant was created
#   - name: variant_updated_at
#     type: string
#     description: The date the product variant was last updated
#   - name: sku
#     type: string
#     description: A unique identifier for the product variant in the shop
#   - name: barcode
#     type: string
#     description: The barcode, UPC, or ISBN number for a product variant
#   - name: price
#     type: number
#     description: The price of the product variant
#   - name: compare_at_price
#     type: number
#     description: The original price of the product variant before an adjustment or a sale
#   - name: inventory_policy
#     type: string
#     description: Whether or not customers are allowed to place an order for out-of-stock product variants
#   - name: inventory_management
#     type: string
#     description: The fullfillment service that tracks the number of items in tock for the product variant
#   - name: fulfillment_service
#     type: string
#     description: The fullfillment service associated with the product variant
#   - name: taxable
#     type: boolean
#     description: Whether or not a tax is charged when the product variant is sold
#   - name: grams
#     type: number
#     description: The weight of the product variant in grams
#   - name: weight
#     type: number
#     description: The weight of the product variant in the units given by weight_unit
#   - name: weight_unit
#     type: string
#     description: The unit of measurement that applies to the weight of the product variant
#   - name: inventory_item_id
#     type: integer
#     description: The unique identifier for the product variant in inventory
#   - name: inventory_quantity
#     type: integer
#     description: The total inventory for the product variant across all locations
#   - name: image_id
#     type: integer
#     description: The unique identifier for an image for the product
#   - name: image_created_at
#     type: string
#     description: The date the image for the product was created
#   - name: image_udpated_at
#     type: string
#     description: The date the image for the product was last updated
#   - name: image_width
#     type: integer
#     description: The width of the image for the product
#   - name: image_height
#     type: integer
#     description: The height of the image for the product
#   - name: image_src
#     type: string
#     description: A link to the image for the product
# examples:
#   - '""'
#   - '"id, title, sku, price"'
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
    # https://shopify.dev/docs/admin-api/rest/reference/products/product#index-2020-04
    # https://shopify.dev/tutorials/make-paginated-requests-to-rest-admin-api

    headers = {
        'X-Shopify-Access-Token': auth_token
    }
    url = api_base_uri + '/admin/api/2020-04/products.json'

    page_size = 250
    url_query_params = {'limit': page_size}
    url_query_str = urllib.parse.urlencode(url_query_params)
    page_url = url + '?' + url_query_str

    while True:

        response = requests_retry_session().get(page_url, headers=headers)
        response.raise_for_status()
        content = response.json()
        data = content.get('products',[])

        if len(data) == 0: # sanity check in case there's an issue with cursor
            break

        buffer = ''
        for header_item in data:
            detail_items_all =  header_item.get('variants',[])
            if len(detail_items_all) == 0:
                item = get_item_info(header_item, {}) # if we don't have any variants, make sure to return item header info
                buffer = buffer + json.dumps(item, default=to_string) + "\n"
            else:
                for detail_item in detail_items_all:
                    item = get_item_info(header_item, detail_item)
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

def get_item_info(header_item, detail_item):

    # map this function's property names to the API's property names
    info = OrderedDict()

    info['id'] = header_item.get('id')
    info['title'] = header_item.get('title')
    info['body_html'] = header_item.get('body_html')
    info['handle'] = header_item.get('handle')
    info['vendor'] = header_item.get('vendor')
    info['product_type'] = header_item.get('product_type')
    info['created_at'] = to_date(header_item.get('created_at'))
    info['updated_at'] = to_date(header_item.get('updated_at'))
    info['published_at'] = to_date(header_item.get('published_at'))
    info['published_scope'] = header_item.get('published_scope')
    info['template_suffix'] = header_item.get('template_suffix')
    info['tags'] = header_item.get('tags')
    info['variant_id'] = detail_item.get('variant_id')
    info['variant_title'] = detail_item.get('title')
    info['variant_option1'] = detail_item.get('option1')
    info['variant_option2'] = detail_item.get('option2')
    info['variant_option3'] = detail_item.get('option3')
    info['variant_created_at'] = to_date(detail_item.get('created_at'))
    info['variant_updated_at'] = to_date(detail_item.get('updated_at'))
    info['sku'] = detail_item.get('sku')
    info['barcode'] = detail_item.get('barcode')
    info['price'] = to_number(detail_item.get('price'))
    info['compare_at_price'] = to_number(detail_item.get('compare_at_price'))
    info['inventory_policy'] = detail_item.get('inventory_policy')
    info['inventory_management'] = detail_item.get('inventory_management')
    info['fulfillment_service'] = detail_item.get('fulfillment_service')
    info['taxable'] = detail_item.get('taxable')
    info['grams'] = detail_item.get('grams')
    info['weight'] = detail_item.get('weight')
    info['weight_unit'] = detail_item.get('weight_unit')
    info['inventory_item_id'] = detail_item.get('inventory_item_id')
    info['inventory_quantity'] = detail_item.get('inventory_quantity')
    info['image_id'] = header_item.get('image').get('id')
    info['image_created_at'] = to_date(header_item.get('image').get('created_at'))
    info['image_udpated_at'] = to_date(header_item.get('image').get('updated_at'))
    info['image_width'] = header_item.get('image').get('width')
    info['image_height'] = header_item.get('image').get('height')
    info['image_src'] = header_item.get('image').get('src')

    return info




