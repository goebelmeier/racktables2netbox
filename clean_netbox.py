#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import imp
import sys
import json
import requests
import urllib3
import logging
from pprint import pprint

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load config file into variable
conf = imp.load_source('conf', 'conf')

api_token = conf.NETBOX_TOKEN
api_url_base = conf.NETBOX_URL

# Define REST Headers
headers = {'Content-Type': 'application/json',
           'Authorization': 'Token {0}'.format(api_token)}

def api_request(method, url, headers):
    logger.debug(method + " - " + url)
    
    request = requests.Request(method, url, headers=headers)
    response = s.send(request.prepare(), verify=False)
    
    logger.debug(str(response.status_code) + " - " + response.reason)
    
    return response

def delete_sites():
    logger.info('Deleting Sites')
    # Get all sites
    api_url = '{0}/api/dcim/sites'.format(api_url_base)

    response = api_request('GET', api_url, headers)
    sites = json.loads(response.content.decode('utf-8'))

    # Delete every site you got
    for site in sites['results']:
        url = '{0}/{1}'.format(api_url, site['id'])
        response = api_request('DELETE', url, headers)

    return

def main():
    # We need to delete the items beginning from the most nested items to the top level items
    delete_sites()

if __name__ == '__main__':
    # Initialize logging platform
    logger = logging.getLogger('clean_netbox')
    logger.setLevel(logging.DEBUG)

    # Log to file
    fh = logging.FileHandler(conf.CLEAN_LOG)
    fh.setLevel(logging.DEBUG)

    # Log to stdout
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Format log output
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Attach handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    s = requests.Session()

    # Run the main function
    main()
    logger.info('[!] Done!')
    sys.exit()
