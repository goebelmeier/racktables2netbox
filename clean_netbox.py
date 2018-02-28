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
api_url_base = conf.NETBOX_URL

def api_request(method, url):
    logger.debug(method + " - " + url)
    
    request = requests.Request(method, url)
    prepared_request = s.prepare_request(request)
    response = s.send(prepared_request)

    logger.debug(str(response.status_code) + " - " + response.reason)
    
    return response

def delete_sites():
    logger.info('Deleting Sites')
    # Get all sites
    api_url = '{0}/dcim/sites'.format(api_url_base)

    response = api_request('GET', api_url)
    sites = json.loads(response.content.decode('utf-8'))

    # Delete every site you got
    for site in sites['results']:
        url = '{0}/{1}'.format(api_url, site['id'])
        response = api_request('DELETE', url)

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

    # Create HTTP connection pool
    s = requests.Session()

    # Disable SSL verification
    s.verify = False

    # Define REST Headers
    headers = {'Content-Type': 'application/json', 
        'Accept': 'application/json; indent=4',
        'Authorization': 'Token {0}'.format(conf.NETBOX_TOKEN)}

    s.headers.update(headers)

    # try:
    #     import http.client as http_client
    # except ImportError:
    #     # Python 2
    #     import httplib as http_client
    # http_client.HTTPConnection.debuglevel = 1

    # requests_log = logging.getLogger("requests.packages.urllib3")
    # requests_log.setLevel(logging.DEBUG)
    # requests_log.propagate = True

    # Run the main function
    main()
    logger.info('[!] Done!')
    sys.exit()
