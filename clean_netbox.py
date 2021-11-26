#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import imp
import sys
import json
import requests
import urllib3
import logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load config file into variable
#conf = imp.load_source('conf', 'conf')
configfile = 'conf'
config = configparser.ConfigParser()
config.read(configfile)

#api_url_base = "{}/api".format(conf.NETBOX_HOST)
api_url_base = "{}/api".format(config['NetBox']['NETBOX_HOST'])

def api_request(method, url):
    # Log which request we're trying to do
    logger.debug("HTTP Request: {} - {}".format(method, url))

    # Prepare request
    request = requests.Request(method, url)
    prepared_request = s.prepare_request(request)

    response = s.send(prepared_request)

    # Log HTTP Response
    logger.debug("HTTP Response: {!s} - {}".format(response.status_code, response.reason))

    return response

def delete_sites():
    logger.info('Deleting Sites')
    # Get all sites
    api_url = '{}/dcim/sites'.format(api_url_base)

    response = api_request('GET', api_url)
    sites = json.loads(response.content.decode('utf-8'))

    # Delete every site you got
    for site in sites['results']:
        url = '{}/{}'.format(api_url, site['id'])
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
    fh = logging.FileHandler(config['Log']['CLEAN_LOG'])
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
        #'Authorization': 'Token {0}'.format(conf.NETBOX_TOKEN)}
        'Authorization': 'Token {0}'.format(config['NetBox']['NETBOX_TOKEN'])}

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
