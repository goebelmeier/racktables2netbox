#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__version__ = 1.00

import configparser
import json
import logging
from os import replace
import pprint
import pymysql
import pynetbox
import requests
import slugify
import socket
import struct
import urllib3
import urllib.parse
import re
from time import sleep
import yaml
import copy
import datetime


class Migrator:
    def slugify(self, text):
        return slugify.slugify(text, max_length=50)

    def create_tenant_group(self, name):
        pass

    def create_tenant(self, name, tenant_group=None):
        logger.info("Creating tenant {}").format(name)

        tenant = {"name": name, "slug": self.slugify(name)}

        if tenant_group:
            tenant["tenant_group"] = netbox.tenancy.tenant_groups.all()

        return netbox.tenancy.tenants.create(tenant)

    def create_region(self, name, parent=None):
        netbox.dcim.regions.create()

        if not parent:
            pass
        pass

    def create_site(
        self,
        name,
        region,
        status,
        physical_address,
        facility,
        shipping_address,
        contact_phone,
        contact_email,
        contact_name,
        tenant,
        time_zone,
    ):
        slug = self.slugify(name)
        pass


# Re-Enabled SSL verification
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
class NETBOX(object):
    def __init__(self, pynetboxobj):
        self.base_url = "{}/api".format(config["NetBox"]["NETBOX_HOST"])
        self.py_netbox = pynetboxobj

        # Create HTTP connection pool
        self.s = requests.Session()

        # SSL verification
        self.s.verify = True

        # Define REST Headers
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json; indent=4",
            "Authorization": "Token {0}".format(config["NetBox"]["NETBOX_TOKEN"]),
        }

        self.s.headers.update(headers)
        self.device_types = None

    def uploader(self, data, url, method="POST"):

        logger.debug("HTTP Request: {} - {} - {}".format(method, url, data))

        try:
            request = requests.Request(method, url, data=json.dumps(data))
            prepared_request = self.s.prepare_request(request)
            r = self.s.send(prepared_request)
            logger.debug(f"HTTP Response: {r.status_code!s} - {r.reason}")
            if r.status_code not in [200, 201]:
                logger.debug(r.text)
            r.raise_for_status()
            r.close()
        except:
            logger.debug("POST attempt failed")
        try:
            if r:
                return_obj = r.json
        except:
            sleep(2)
            return {}
        return return_obj

    def uploader2(self, data, url, method="POST"):
        # ignores failures.
        method = "POST"

        logger.debug("HTTP Request: {} - {} - {}".format(method, url, data))

        request = requests.Request(method, url, data=json.dumps(data))
        prepared_request = self.s.prepare_request(request)
        r = self.s.send(prepared_request)
        logger.debug(f"HTTP Response: {r.status_code!s} - {r.reason}")
        r.close()
        logger.debug(r.text)

    def fetcher(self, url):
        method = "GET"

        logger.debug("HTTP Request: {} - {}".format(method, url))
        max_attempts = 3
        current_attempt = 0
        while current_attempt < max_attempts:

            try:
                request = requests.Request(method, url)
                prepared_request = self.s.prepare_request(request)
                r = self.s.send(prepared_request)

                logger.debug(f"HTTP Response: {r.status_code} - {r.reason}")
                r.raise_for_status()
                r.close()
            except:
                sleep(2)
                logger.debug("fetch attempt failed")
            try:
                if r:
                    if r.status_code == 200:
                        return r.text
            except:
                test = ""
            current_attempt = current_attempt + 1
        logger.debug("failed to get {} 3 times".format(url))
        exit(1)

    def post_subnet(self, data):
        url = self.base_url + "/ipam/prefixes/"
        exists = self.check_for_subnet(data)
        if exists[0]:
            logger.info("prefix/subnet: {} already exists, updating with Put".format(data["prefix"]))
            method = "PUT"
            url = "{}{}/".format(url, exists[1]["id"])
            self.uploader(data, url, method)
        else:
            logger.info("Posting data to {}".format(url))
            self.uploader(data, url)

    def check_for_subnet(self, data):
        url_safe_ip = urllib.parse.quote_plus(data["prefix"])
        url = self.base_url + "/ipam/prefixes/?prefix={}".format(url_safe_ip)
        logger.info("checking for existing prefix in netbox: {}".format(url))
        check = self.fetcher(url)
        json_obj = json.loads(check)
        # logger.debug("response: {}".format(check))
        if json_obj["count"] == 1:
            return True, json_obj["results"][0]
        elif json_obj["count"] > 1:
            logger.error("duplicate prefixes exist. cleanup!")
            exit(2)
        else:
            return False, False

    def check_for_ip(self, data):
        url_safe_ip = urllib.parse.quote_plus(data["address"])
        url = self.base_url + "/ipam/ip-addresses/?address={}".format(url_safe_ip)
        logger.info("checking for existing ip in netbox: {}".format(url))
        check = self.fetcher(url)
        json_obj = json.loads(check)
        # logger.debug("response: {}".format(check))
        if json_obj["count"] == 1:
            return True
        elif json_obj["count"] > 1:
            logger.error("duplicate ip's exist. cleanup!")
            exit(2)
        else:
            return False

    def post_ip(self, data):
        url = self.base_url + "/ipam/ip-addresses/"
        exists = self.check_for_ip(data)
        if exists:
            logger.info("ip: {} already exists, skipping".format(data["address"]))
        else:
            logger.info("Posting IP data to {}".format(url))
            self.uploader(data, url)

    def get_sites(self):
        url = self.base_url + "/dcim/sites/"
        resp = self.fetcher(url)
        return json.loads(resp)["results"]

    def get_sites_keyd_by_description(self):
        sites = self.get_sites()
        resp = {}
        for site in sites:
            if site["description"] == "":
                logger.debug("site: {} {} has no description set, skipping".format(site["display"], site["url"]))
            else:
                if not site["description"] in resp.keys():
                    resp[site["description"]] = site
                else:
                    logger.debug("duplicate description detected! {}".format(site["description"]))
        return resp

    def post_rack(self, data):
        url = self.base_url + "/dcim/racks/"
        exists = self.check_if_rack_exists(data)
        if exists[0]:
            logger.info("rack: {} already exists, updating".format(data["name"]))
            url = url + "{}/".format(exists[1])
            self.uploader(data, url, "PUT")
        else:
            logger.info("Posting rack data to {}".format(url))
            self.uploader(data, url)

    def check_if_rack_exists(self, data):
        url_safe_ip = urllib.parse.quote_plus(data["name"])
        url = self.base_url + "/dcim/racks/?name={}".format(url_safe_ip)
        logger.info("checking for existing rack in netbox: {}".format(url))
        check = self.fetcher(url)
        json_obj = json.loads(check)
        if json_obj["count"] == 0:
            return False, False
        else:
            for rack in json_obj["results"]:
                if rack["site"]["id"] == data["site"]:
                    return True, rack["id"]
        return False
        # elif json_obj["count"] > 1:
        #     logger.error("duplicate ip's exist. cleanup!")
        #     exit(2)
        # else:
        #     return False

    def post_tag(self, tag, description):
        url = self.base_url + "/extras/tags/"
        data = {}
        data["name"] = str(tag)
        data["slug"] = str(tag).lower().replace(" ", "_")
        if not description is None:
            data["description"] = description
        self.uploader2(data, url)

    def get_tags_key_by_name(self):
        url = self.base_url + "/extras/tags/?limit=10000"
        resp = json.loads(self.fetcher(url))
        tags = {}
        for tag in resp["results"]:
            tags[tag["name"]] = tag
        logger.debug(tags)
        return tags

    def check_for_vlan_group(self, group_name):
        url = self.base_url + "/ipam/vlan-groups/?name={}".format(group_name)
        logger.info("checking for vlan-group in netbox: {}".format(url))
        check = self.fetcher(url)
        json_obj = json.loads(check)
        # logger.debug("response: {}".format(check))
        if json_obj["count"] == 1:
            logger.debug("found matching group")
            return True, json_obj["results"][0]
        elif json_obj["count"] > 1:
            logger.debug("duplcate groups detected, fix this")
            logger.debug(json_obj)
            exit(1)
        else:
            return False, False

    def get_vlan_groups_by_name(self):
        url = self.base_url + "/ipam/vlan-groups/?limit=10000"
        resp = json.loads(self.fetcher(url))
        groups = {}
        for group in resp["results"]:
            if group["name"] in groups.keys():
                logger.debug("duplicate group name exists! fix this. group: {}".format(group["name"]))
                exit(1)
            groups[group["name"]] = group
        logger.debug(groups)
        return groups

    def post_vlan_group(self, group_name):
        url = self.base_url + "/ipam/vlan-groups/"
        data = {}
        data["name"] = str(group_name)
        data["description"] = str(group_name)
        data["slug"] = str(group_name).lower().replace(" ", "-").replace(":", "")
        if not self.check_for_vlan_group(group_name)[0]:
            self.uploader2(data, url)

    def check_for_vlan(self, data):
        url = self.base_url + "/ipam/vlans/?vid={}&group_id={}".format(data["vid"], data["group"])
        logger.info("checking for vlan in netbox: {}".format(url))
        check = self.fetcher(url)
        json_obj = json.loads(check)
        # logger.debug("response: {}".format(check))
        if json_obj["count"] == 1:
            logger.debug("matching vlan found")
            return True, json_obj["results"][0]
        elif json_obj["count"] > 1:
            logger.debug("duplcate vlans detected, fix this")
            logger.debug(json_obj)
            exit(1)
        else:
            return False, False

    def get_nb_vlans(self):
        vlans_by_netbox_id = {}
        url = self.base_url + "/ipam/vlans/?limit=10000"
        resp = json.loads(self.fetcher(url))
        for vlan in resp["results"]:
            vlans_by_netbox_id[vlan["id"]] = vlan
        return vlans_by_netbox_id

    def post_vlan(self, data):
        url = self.base_url + "/ipam/vlans/"
        exists = self.check_for_vlan(data)
        if exists[0]:
            logger.info("vlan: {} already exists, updating".format(data["name"]))
            url = url + "{}/".format(exists[1]["id"])
            self.uploader(data, url, "PUT")
        else:
            logger.info("Posting vlan data to {}".format(url))
            self.uploader(data, url)

    def post_device_type(self, device_type_key, device_type):
        logger.debug(device_type_key)
        logger.debug(device_type)
        try:
            filename = device_type["device_template_data"]["yaml_file"]
        except:
            filename = device_type["device_template_data"]["yaml_url"]
        data = {}
        if "yaml_file" in device_type["device_template_data"].keys():
            with open(filename, "r") as stream:
                try:
                    data = yaml.safe_load(stream)
                except yaml.YAMLError as exc:
                    logger.debug(exc)
        if "yaml_url" in device_type["device_template_data"].keys():
            try:
                resp = requests.get(device_type["device_template_data"]["yaml_url"])
                data = yaml.safe_load(resp.text)
            except:
                logger.debug(f"failed to load {device_type['device_template_data']['yaml_url']} for {device_type_key} template")

        pp.pprint(data)
        man_data = {"name": data["manufacturer"], "slug": self.slugFormat(data["manufacturer"])}
        self.createManufacturers([man_data], py_netbox)
        data["manufacturer"] = man_data
        self.createDeviceTypes([data], py_netbox)

    def post_device(self, data, py_netbox):
        needs_updating = False
        device_check = [str(item) for item in py_netbox.dcim.devices.filter(cf_rt_id=data["custom_fields"]["rt_id"])]

        if len(device_check) == 1:
            logger.debug("device already in netbox. sending to update checker")
            needs_updating = True
            matched_by = "cf_rt_id"
        if needs_updating:
            self.update_device(data, matched_by, py_netbox)
        else:
            try:
                py_netbox.dcim.devices.create(data)
            except pynetbox.RequestError as e:
                logger.debug("matched request error")
                pp.pprint(e.args)
                if "device with this Asset tag already exists" in str(e):
                    logger.debug("matched by asset tag")
                    matched_by = "asset_tag"
                    needs_updating = True
                elif "device with this name already exists" in str(e):
                    logger.debug("matched by name")
                    matched_by = "name"
                    needs_updating = True
            if needs_updating:  # update existing device
                self.update_device(data, matched_by, py_netbox)

    def update_device(self, data, match_type, py_netbox):

        if match_type == "cf_rt_id":
            device = py_netbox.dcim.devices.get(cf_rt_id=data["custom_fields"]["rt_id"])
        elif match_type == "asset_tag":
            device = py_netbox.dcim.devices.get(asset_tag=data["asset_tag"])
        elif match_type == "name":
            device = py_netbox.dcim.devices.get(name=data["name"])
        logger.debug("sending updates (if any) to nb")
        device.update(data)

    # def post_location(self, data):
    #     url = self.base_url + '/api/1.0/location/'
    #     logger.info('Posting location data to {}'.format(url))
    #     self.uploader(data, url)

    # def post_room(self, data):
    #     url = self.base_url + '/api/1.0/rooms/'
    #     logger.info('Posting room data to {}'.format(url))
    #     self.uploader(data, url)

    # def post_rack(self, data):
    #     url = self.base_url + '/api/1.0/racks/'
    #     logger.info('Posting rack data to {}'.format(url))
    #     response = self.uploader(data, url)
    #     return response

    # def post_pdu(self, data):
    #     url = self.base_url + '/api/1.0/pdus/'
    #     logger.info('Posting PDU data to {}'.format(url))
    #     response = self.uploader(data, url)
    #     return response

    # def post_pdu_model(self, data):
    #     url = self.base_url + '/api/1.0/pdu_models/'
    #     logger.info('Posting PDU model to {}'.format(url))
    #     response = self.uploader(data, url)
    #     return response

    # def post_pdu_to_rack(self, data, rack):
    #     url = self.base_url + '/api/1.0/pdus/rack/'
    #     logger.info('Posting PDU to rack {}'.format(rack))
    #     self.uploader(data, url)

    # def post_hardware(self, data, nb):
    #     all_device_types = {str(item): item for item in nb.dcim.device_types.all()}
    #     pp.pprint(all_device_types)
    #     pp.pprint(data)
    #     exit(2)

    # def post_device2rack(self, data):
    #     url = self.base_url + '/api/1.0/device/rack/'
    #     logger.info('Adding device to rack at {}'.format(url))
    #     self.uploader(data, url)

    def post_building(self, data):
        url = self.base_url + "/dcim/sites/"
        logger.info("Uploading building data to {}".format(url))
        self.uploader(data, url)

    # def post_switchport(self, data):
    #     url = self.base_url + '/api/1.0/switchports/'
    #     logger.info('Uploading switchports data to {}'.format(url))
    #     self.uploader(data, url)

    # def post_patch_panel(self, data):
    #     url = self.base_url + '/api/1.0/patch_panel_models/'
    #     logger.info('Uploading patch panels data to {}'.format(url))
    #     self.uploader(data, url)

    # def post_patch_panel_module_models(self, data):
    #     url = self.base_url + '/api/1.0/patch_panel_module_models/'
    #     logger.info('Uploading patch panels modules data to {}}'.format(url))
    #     self.uploader(data, url)

    # def get_pdu_models(self):
    #     url = self.base_url + '/api/1.0/pdu_models/'
    #     logger.info('Fetching PDU models from {}'.format(url))
    #     self.fetcher(url)

    # def get_racks(self):
    #     url = self.base_url + '/api/1.0/racks/'
    #     logger.info('Fetching racks from {}'.format(url))
    #     ata = self.fetcher(url)
    #     return data

    # def get_devices(self):
    #     url = self.base_url + '/api/1.0/devices/'
    #     logger.info('Fetching devices from {}'.format(url))
    #     data = self.fetcher(url)
    #     return data

    # def get_buildings(self):
    #     url = self.base_url + '/api/dcim/sites/'
    #     logger.info('Fetching buildings from {}'.format(url))
    #     data = self.fetcher(url)
    #     return data

    # def get_rooms(self):
    #     url = self.base_url + '/api/1.0/rooms/'
    #     logger.info('Fetching rooms from {}'.format(url))
    #     data = self.fetcher(url)
    #     return data

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def slugFormat(self, name):
        return re.sub("\W+", "-", name.lower())

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createManufacturers(self, vendors, nb):
        all_manufacturers = {str(item): item for item in nb.dcim.manufacturers.all()}
        need_manufacturers = []
        for vendor in vendors:
            try:
                manGet = all_manufacturers[vendor["name"]]
                logger.debug(f"Manufacturer Exists: {manGet.name} - {manGet.id}")
            except KeyError:
                need_manufacturers.append(vendor)

        if not need_manufacturers:
            return
        created = False
        count = 0
        while created == False and count < 3:
            try:
                manSuccess = nb.dcim.manufacturers.create(need_manufacturers)
                for man in manSuccess:
                    logger.debug(f"Manufacturer Created: {man.name} - " + f"{man.id}")
                    # counter.update({'manufacturer': 1})
                created = True
                count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createInterfaces(self, interfaces, deviceType, nb):
        all_interfaces = {str(item): item for item in nb.dcim.interface_templates.filter(devicetype_id=deviceType)}
        need_interfaces = []
        for interface in interfaces:
            try:
                ifGet = all_interfaces[interface["name"]]
                logger.debug(f"Interface Template Exists: {ifGet.name} - {ifGet.type}" + f" - {ifGet.device_type.id} - {ifGet.id}")
            except KeyError:
                interface["device_type"] = deviceType
                need_interfaces.append(interface)

        if not need_interfaces:
            return
        created = False
        count = 0
        while created == False and count < 3:
            try:
                ifSuccess = nb.dcim.interface_templates.create(need_interfaces)
                for intf in ifSuccess:
                    logger.debug(f"Interface Template Created: {intf.name} - " + f"{intf.type} - {intf.device_type.id} - " + f"{intf.id}")
                    # counter.update({'updated': 1})
                    created = True
                    count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createConsolePorts(self, consoleports, deviceType, nb):
        all_consoleports = {str(item): item for item in nb.dcim.console_port_templates.filter(devicetype_id=deviceType)}
        need_consoleports = []
        for consoleport in consoleports:
            try:
                cpGet = all_consoleports[consoleport["name"]]
                logger.debug(f"Console Port Template Exists: {cpGet.name} - " + f"{cpGet.type} - {cpGet.device_type.id} - {cpGet.id}")
            except KeyError:
                consoleport["device_type"] = deviceType
                need_consoleports.append(consoleport)

        if not need_consoleports:
            return
        created = False
        count = 0
        while created == False and count < 3:
            try:
                cpSuccess = nb.dcim.console_port_templates.create(need_consoleports)
                for port in cpSuccess:
                    logger.debug(f"Console Port Created: {port.name} - " + f"{port.type} - {port.device_type.id} - " + f"{port.id}")
                    # counter.update({'updated': 1})
                    created = True
                    count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createPowerPorts(self, powerports, deviceType, nb):
        all_power_ports = {str(item): item for item in nb.dcim.power_port_templates.filter(devicetype_id=deviceType)}
        need_power_ports = []
        for powerport in powerports:
            try:
                ppGet = all_power_ports[powerport["name"]]
                logger.debug(f"Power Port Template Exists: {ppGet.name} - " + f"{ppGet.type} - {ppGet.device_type.id} - {ppGet.id}")
            except KeyError:
                powerport["device_type"] = deviceType
                need_power_ports.append(powerport)

        if not need_power_ports:
            return
        created = False
        count = 0
        while created == False and count < 3:
            try:
                ppSuccess = nb.dcim.power_port_templates.create(need_power_ports)
                for pp in ppSuccess:
                    logger.debug(f"Interface Template Created: {pp.name} - " + f"{pp.type} - {pp.device_type.id} - " + f"{pp.id}")
                    # counter.update({'updated': 1})
                    created = True
                    count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createConsoleServerPorts(self, consoleserverports, deviceType, nb):
        all_consoleserverports = {str(item): item for item in nb.dcim.console_server_port_templates.filter(devicetype_id=deviceType)}
        need_consoleserverports = []
        for csport in consoleserverports:
            try:
                cspGet = all_consoleserverports[csport["name"]]
                logger.debug(f"Console Server Port Template Exists: {cspGet.name} - " + f"{cspGet.type} - {cspGet.device_type.id} - " + f"{cspGet.id}")
            except KeyError:
                csport["device_type"] = deviceType
                need_consoleserverports.append(csport)

        if not need_consoleserverports:
            return
        created = False
        count = 0
        while created == False and count < 3:
            try:
                cspSuccess = nb.dcim.console_server_port_templates.create(need_consoleserverports)
                for csp in cspSuccess:
                    logger.debug(f"Console Server Port Created: {csp.name} - " + f"{csp.type} - {csp.device_type.id} - " + f"{csp.id}")
                    # counter.update({'updated': 1})
                    created = True
                    count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createFrontPorts(self, frontports, deviceType, nb):
        all_frontports = {str(item): item for item in nb.dcim.front_port_templates.filter(devicetype_id=deviceType)}
        need_frontports = []
        for frontport in frontports:
            try:
                fpGet = all_frontports[frontport["name"]]
                logger.debug(f"Front Port Template Exists: {fpGet.name} - " + f"{fpGet.type} - {fpGet.device_type.id} - {fpGet.id}")
            except KeyError:
                frontport["device_type"] = deviceType
                need_frontports.append(frontport)

        if not need_frontports:
            return

        all_rearports = {str(item): item for item in nb.dcim.rear_port_templates.filter(devicetype_id=deviceType)}
        for port in need_frontports:
            try:
                rpGet = all_rearports[port["rear_port"]]
                port["rear_port"] = rpGet.id
            except KeyError:
                logger.debug(f'Could not find Rear Port for Front Port: {port["name"]} - ' + f'{port["type"]} - {deviceType}')
        created = False
        count = 0
        while created == False and count < 3:
            try:
                fpSuccess = nb.dcim.front_port_templates.create(need_frontports)
                for fp in fpSuccess:
                    logger.debug(f"Front Port Created: {fp.name} - " + f"{fp.type} - {fp.device_type.id} - " + f"{fp.id}")
                    # counter.update({'updated': 1})
                    created = True
                    count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createRearPorts(self, rearports, deviceType, nb):
        all_rearports = {str(item): item for item in nb.dcim.rear_port_templates.filter(devicetype_id=deviceType)}
        need_rearports = []
        for rearport in rearports:
            try:
                rpGet = all_rearports[rearport["name"]]
                logger.debug(f"Rear Port Template Exists: {rpGet.name} - {rpGet.type}" + f" - {rpGet.device_type.id} - {rpGet.id}")
            except KeyError:
                rearport["device_type"] = deviceType
                need_rearports.append(rearport)

        if not need_rearports:
            return
        created = False
        count = 0
        while created == False and count < 3:
            try:
                rpSuccess = nb.dcim.rear_port_templates.create(need_rearports)
                for rp in rpSuccess:
                    logger.debug(f"Rear Port Created: {rp.name} - {rp.type}" + f" - {rp.device_type.id} - {rp.id}")
                    # counter.update({'updated': 1})
                    created = True
                    count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createDeviceBays(self, devicebays, deviceType, nb):
        all_devicebays = {str(item): item for item in nb.dcim.device_bay_templates.filter(devicetype_id=deviceType)}
        need_devicebays = []
        for devicebay in devicebays:
            try:
                dbGet = all_devicebays[devicebay["name"]]
                logger.debug(f"Device Bay Template Exists: {dbGet.name} - " + f"{dbGet.device_type.id} - {dbGet.id}")
            except KeyError:
                devicebay["device_type"] = deviceType
                need_devicebays.append(devicebay)

        if not need_devicebays:
            return
        created = False
        count = 0
        while created == False and count < 3:
            try:
                dbSuccess = nb.dcim.device_bay_templates.create(need_devicebays)
                for db in dbSuccess:
                    logger.debug(f"Device Bay Created: {db.name} - " + f"{db.device_type.id} - {db.id}")
                    # counter.update({'updated': 1})
                created = True
                count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createPowerOutlets(self, poweroutlets, deviceType, nb):
        all_poweroutlets = {str(item): item for item in nb.dcim.power_outlet_templates.filter(devicetype_id=deviceType)}
        need_poweroutlets = []
        for poweroutlet in poweroutlets:
            try:
                poGet = all_poweroutlets[poweroutlet["name"]]
                logger.debug(f"Power Outlet Template Exists: {poGet.name} - " + f"{poGet.type} - {poGet.device_type.id} - {poGet.id}")
            except KeyError:
                poweroutlet["device_type"] = deviceType
                need_poweroutlets.append(poweroutlet)

        if not need_poweroutlets:
            return

        all_power_ports = {str(item): item for item in nb.dcim.power_port_templates.filter(devicetype_id=deviceType)}
        for outlet in need_poweroutlets:
            try:
                ppGet = all_power_ports[outlet["power_port"]]
                outlet["power_port"] = ppGet.id
            except KeyError:
                pass
        created = False
        count = 0
        while created == False and count < 3:
            try:
                poSuccess = nb.dcim.power_outlet_templates.create(need_poweroutlets)
                for po in poSuccess:
                    logger.debug(f"Power Outlet Created: {po.name} - " + f"{po.type} - {po.device_type.id} - " + f"{po.id}")
                    # counter.update({'updated': 1})
                    created = True
                    count = 3
            except Exception as e:
                logger.debug(e.error)
                created = False
                count = count + 1
                sleep(0.5 * count)

    # modified/sourced from from: https://github.com/minitriga/Netbox-Device-Type-Library-Import
    def createDeviceTypes(self, deviceTypes, nb=None):
        nb = self.py_netbox
        all_device_types = {str(item): item for item in nb.dcim.device_types.all()}
        for deviceType in deviceTypes:
            try:
                dt = all_device_types[deviceType["model"]]
                logger.debug(f"Device Type Exists: {dt.manufacturer.name} - " + f"{dt.model} - {dt.id}")
            except KeyError:
                try:
                    dt = nb.dcim.device_types.create(deviceType)
                    # counter.update({'added': 1})
                    logger.debug(f"Device Type Created: {dt.manufacturer.name} - " + f"{dt.model} - {dt.id}")
                except Exception as e:
                    logger.debug(e.error)

            if "interfaces" in deviceType:
                logger.debug("interfaces")
                self.createInterfaces(deviceType["interfaces"], dt.id, nb)
            if "power-ports" in deviceType:
                logger.debug("power-ports")
                self.createPowerPorts(deviceType["power-ports"], dt.id, nb)
            if "power-port" in deviceType:
                logger.debug("power-port")
                self.createPowerPorts(deviceType["power-port"], dt.id, nb)
            if "console-ports" in deviceType:
                logger.debug("console-port")
                self.createConsolePorts(deviceType["console-ports"], dt.id, nb)
            if "power-outlets" in deviceType:
                logger.debug("power-outlets")
                self.createPowerOutlets(deviceType["power-outlets"], dt.id, nb)
            if "console-server-ports" in deviceType:
                logger.debug("console-server-ports")
                self.createConsoleServerPorts(deviceType["console-server-ports"], dt.id, nb)
            if "rear-ports" in deviceType:
                logger.debug("rear-ports")
                self.createRearPorts(deviceType["rear-ports"], dt.id, nb)
            if "front-ports" in deviceType:
                logger.debug("front-ports")
                self.createFrontPorts(deviceType["front-ports"], dt.id, nb)
            if "device-bays" in deviceType:
                logger.debug("device-bays")
                self.createDeviceBays(deviceType["device-bays"], dt.id, nb)

    def change_attrib_type(self, attrib):
        if attrib in ["uint", "int", "float"]:
            attrib = "text"
        if attrib in ["bool"]:
            attrib = "boolean"
        if attrib in ["string", "dict"]:
            attrib = "text"
        return attrib

    def cleanup_attrib_value(self, attrib_val, attrib_type):
        if attrib_type in ["uint", "int", "float"]:
            return str(attrib_val)
        if attrib_type in ["bool"]:
            return bool(attrib_val)
        if attrib_type in ["string", "dict", "text"]:
            return str(attrib_val)
        if attrib_type == "date":
            datetime_time = datetime.datetime.fromtimestamp(int(attrib_val))
            return datetime_time.strftime("%Y-%m-%d")
        return str(attrib_val)

    def createCustomFields(self, attributes):
        logger.debug(attributes)
        nb = self.py_netbox
        all_custom_fields = {str(item): item for item in nb.extras.custom_fields.all()}
        logger.debug(all_custom_fields)
        for custom_field in attributes:
            try:
                # print(custom_field["name"])
                # print(all_custom_fields[custom_field["name"]])
                dt = all_custom_fields[custom_field["name"]]
                logger.debug(f"Custom Field Exists: {dt.name} - " + f"{dt.type}")
            except KeyError:
                try:
                    custom_field["type"] = self.change_attrib_type(custom_field["type"])
                    custom_field["content_types"] = [
                        "circuits.circuit",
                        "circuits.circuittype",
                        "circuits.provider",
                        "circuits.providernetwork",
                        "dcim.cable",
                        "dcim.consoleport",
                        "dcim.consoleserverport",
                        "dcim.device",
                        "dcim.devicebay",
                        "dcim.devicerole",
                        "dcim.devicetype",
                        "dcim.frontport",
                        "dcim.interface",
                        "dcim.inventoryitem",
                        "dcim.location",
                        "dcim.manufacturer",
                        "dcim.platform",
                        "dcim.powerfeed",
                        "dcim.poweroutlet",
                        "dcim.powerpanel",
                        "dcim.powerport",
                        "dcim.rack",
                        "dcim.rackreservation",
                        "dcim.rackrole",
                        "dcim.rearport",
                        "dcim.region",
                        "dcim.site",
                        "dcim.sitegroup",
                        "dcim.virtualchassis",
                        "ipam.aggregate",
                        "ipam.ipaddress",
                        "ipam.prefix",
                        "ipam.rir",
                        "ipam.role",
                        "ipam.routetarget",
                        "ipam.vrf",
                        "ipam.vlangroup",
                        "ipam.vlan",
                        "ipam.service",
                        "ipam.iprange",
                        "tenancy.tenantgroup",
                        "tenancy.tenant",
                        "virtualization.cluster",
                        "virtualization.clustergroup",
                        "virtualization.clustertype",
                        "virtualization.virtualmachine",
                        "virtualization.vminterface",
                    ]
                    dt = nb.extras.custom_fields.create(custom_field)
                    # counter.update({'added': 1})
                    logger.debug(f"Device Type Created: {dt.name} - " + f"{dt.type} ")
                    # print("test")
                except Exception as e:
                    logger.debug(e)


class DB(object):
    """
    Fetching data from Racktables and converting them to Device42 API format.
    """

    def __init__(self):
        self.con = None
        self.hardware = None
        self.tag_map = None
        self.vlan_group_map = None
        self.vlan_map = None
        self.tables = []
        self.rack_map = []
        self.vm_hosts = {}
        self.chassis = {}
        self.rack_id_map = {}
        self.container_map = {}
        self.building_room_map = {}
        self.skipped_devices = {}

    def connect(self):
        """
        Connection to RT database
        :return:
        """
        self.con = pymysql.connect(
            host=config["MySQL"]["DB_IP"],
            port=int(config["MySQL"]["DB_PORT"]),
            db=config["MySQL"]["DB_NAME"],
            user=config["MySQL"]["DB_USER"],
            passwd=config["MySQL"]["DB_PWD"],
        )

        self.con.query("SET SESSION interactive_timeout=60")
        # self.con.query('SET SESSION wait_timeout=3600')

    @staticmethod
    def convert_ip(ip_raw):
        """
        IP address conversion to human readable format
        :param ip_raw:
        :return:
        """
        ip = socket.inet_ntoa(struct.pack("!I", ip_raw))
        return ip

    @staticmethod
    def convert_ip_v6(ip_raw):
        ip = socket.inet_ntop(socket.AF_INET6, ip_raw)
        return ip

    def get_ips(self):
        """
        Fetch IPs from RT and send them to upload function
        :return:
        """
        adrese = []
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM IPv4Address;"
            cur.execute(q)
            ips = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("IPs", str(ips))
                logger.debug(msg)
            cur.close()
            cur2 = self.con.cursor()
            q2 = "SELECT object_id,ip FROM IPv4Allocation;"
            cur2.execute(q2)
            ip_by_allocation = cur2.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("IPs", str(ip_by_allocation))
                logger.debug(msg)
            cur2.close()
            self.con = None

        for line in ips:
            net = {}
            ip_raw, name, comment, reserved = line
            ip = self.convert_ip(ip_raw)
            adrese.append(ip)

            net.update({"address": ip})
            msg = "IP Address: %s" % ip
            logger.info(msg)

            desc = " ".join([name, comment]).strip()
            net.update({"description": desc})
            msg = "Label: %s" % desc
            logger.info(msg)
            if not desc in ["network", "broadcast"]:
                netbox.post_ip(net)

        for line in ip_by_allocation:
            net = {}
            object_id, allocationip_raw = line
            ip = self.convert_ip(allocationip_raw)
            if not ip in adrese:
                net.update({"address": ip})
                msg = "IP Address: %s" % ip
                logger.info(msg)
                netbox.post_ip(net)

    def get_ips_v6(self):
        """
        Fetch v6 IPs from RT and send them to upload function
        :return:
        """
        adrese = []
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM IPv6Address;"
            cur.execute(q)
            ips = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("IPs", str(ips))
                logger.debug(msg)
            cur.close()
            cur2 = self.con.cursor()
            q2 = "SELECT object_id,ip FROM IPv6Allocation;"
            cur2.execute(q2)
            ip_by_allocation = cur2.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("IPs", str(ip_by_allocation))
                logger.debug(msg)
            cur2.close()
            self.con = None

        for line in ips:
            net = {}
            ip_raw, name, comment, reserved = line
            ip = self.convert_ip_v6(ip_raw)
            adrese.append(ip)

            net.update({"address": ip})
            msg = "IP Address: %s" % ip
            logger.info(msg)

            desc = " ".join([name, comment]).strip()
            net.update({"description": desc})
            msg = "Label: %s" % desc
            logger.info(msg)
            netbox.post_ip(net)

        for line in ip_by_allocation:
            net = {}
            object_id, allocationip_raw = line
            ip = self.convert_ip_v6(allocationip_raw)
            if not ip in adrese:
                net.update({"address": ip})
                msg = "IP Address: %s" % ip
                logger.info(msg)
                netbox.post_ip(net)

    def create_tag_map(self):
        logger.debug("creating tag map")
        self.tag_map = netbox.get_tags_key_by_name()
        logger.debug("there are {} tags cached".format(len(self.tag_map)))
        logger.debug(self.tag_map.keys())

    def get_subnets(self):
        """
        Fetch subnets from RT and send them to upload function
        :return:
        """
        subs = {}
        if not self.vlan_group_map:
            self.create_vlan_domains_nb_group_map()
        if not self.vlan_map:
            self.create_vlan_nb_map()
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM IPv4Network LEFT JOIN VLANIPv4 on IPv4Network.id = VLANIPv4.ipv4net_id"
            cur.execute(q)
            subnets = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("Subnets", str(subnets))
                logger.debug(msg)
            cur.close()
            self.con = None

        for line in subnets:
            if not self.tag_map:
                self.create_tag_map()
            sid, raw_sub, mask, name, comment, vlan_domain_id, vlan_id, ipv4net_id = line
            subnet = self.convert_ip(raw_sub)
            rt_tags = self.get_tags_for_obj("ipv4net", sid)
            # print(rt_tags)
            tags = []
            # print (self.tag_map)
            # if not comment == None:
            #     name = "{} {}".format(name, comment)
            for tag in rt_tags:
                try:
                    # print(tag)
                    tags.append(self.tag_map[tag]["id"])
                except:
                    logger.debug("failed to find tag {} in lookup map".format(tag))
            if not vlan_id is None:
                try:
                    vlan = self.vlan_map["{}_{}".format(vlan_domain_id, vlan_id)]["id"]
                    subs.update({"vlan": vlan})
                except:
                    logger.debug("failed to find vlan for subnet {}".format(subnet))
            subs.update({"prefix": "/".join([subnet, str(mask)])})
            subs.update({"status": "active"})
            # subs.update({'mask_bits': str(mask)})
            subs.update({"description": name})
            subs.update({"tags": tags})
            netbox.post_subnet(subs)

    def get_subnets_v6(self):
        """
        Fetch subnets from RT and send them to upload function
        :return:
        """
        subs = {}
        if not self.vlan_group_map:
            self.create_vlan_domains_nb_group_map()
        if not self.vlan_map:
            self.create_vlan_nb_map()
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM IPv6Network LEFT JOIN VLANIPv6 on IPv6Network.id = VLANIPv6.ipv6net_id"
            cur.execute(q)
            subnets = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("Subnets", str(subnets))
                logger.debug(msg)
            cur.close()
            self.con = None

        for line in subnets:
            if not self.tag_map:
                self.create_tag_map()
            sid, raw_sub, mask, last_ip, name, comment, vlan_domain_id, vlan_id, ipv6net_id = line
            subnet = self.convert_ip_v6(raw_sub)
            rt_tags = self.get_tags_for_obj("ipv6net", sid)
            # print(rt_tags)
            tags = []
            # print (self.tag_map)
            if not comment == None:
                name = "{} {}".format(name, comment)
            for tag in rt_tags:
                try:
                    # print(tag)
                    tags.append(self.tag_map[tag]["id"])
                except:
                    logger.debug("failed to find tag {} in lookup map".format(tag))
            if not vlan_id is None:
                try:
                    vlan = self.vlan_map["{}_{}".format(vlan_domain_id, vlan_id)]["id"]
                    subs.update({"vlan": vlan})
                except:
                    logger.debug("failed to find vlan for subnet {}".format(subnet))
            subs.update({"prefix": "/".join([subnet, str(mask)])})
            subs.update({"status": "active"})
            # subs.update({'mask_bits': str(mask)})
            subs.update({"description": name})
            subs.update({"tags": tags})
            netbox.post_subnet(subs)

    def get_tags_for_obj(self, tag_type, object_id):
        subs = {}
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT tag FROM TagStorage
                LEFT JOIN TagTree ON TagStorage.tag_id = TagTree.id
                WHERE entity_realm = "{}" AND entity_id = "{}" """.format(
                tag_type, object_id
            )
            cur.execute(q)

            resp = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("tags", str(resp))
                logger.debug(msg)
            cur.close()
            self.con = None
        tags = []
        for tag in resp:
            tags.append(tag[0])
        if not self.tag_map:
            self.create_tag_map()
        return tags

    def get_tags(self):
        tags = []

        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = 'SELECT tag,description FROM TagTree where is_assignable = "yes";'
            cur.execute(q)
            tags = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("tags", str(tags))
                logger.debug(msg)
            cur.close()
            self.con = None

        for line in tags:
            tag, description = line
            netbox.post_tag(tag, description)

    def get_custom_attribs(self):
        attributes = []

        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT type,name FROM Attribute;"
            cur.execute(q)
            tags = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("attributes", str(tags))
                logger.debug(msg)
            cur.close()
            self.con = None

        for line in tags:
            attrib_type, attrib_name = line
            attributes.append({"name": attrib_name, "type": attrib_type})
        attributes.append({"name": "rt_id", "type": "text"})  # custom field for racktables source objid
        attributes.append({"name": "Visible label", "type": "text"})
        attributes.append({"name": "SW type", "type": "text"})
        attributes.append({"name": "Operating System", "type": "text"})

        netbox.createCustomFields(attributes)

    def get_vlan_domains(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM VLANDomain;"
            cur.execute(q)
            resp = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("vlan_domains", str(resp))
                logger.debug(msg)
            cur.close()
            self.con = None

        for line in resp:
            id, group_id, description = line
            netbox.post_vlan_group(description)

    def create_vlan_domains_nb_group_map(self):
        nb_groups = netbox.get_vlan_groups_by_name()
        # self.vlan_group_map
        groups_by_rt_id = {}
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM VLANDomain;"
            cur.execute(q)
            resp = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("vlan_domains", str(resp))
                logger.debug(msg)
            cur.close()
            self.con = None

        for line in resp:
            id, group_id, description = line
            groups_by_rt_id[id] = nb_groups[description]
        self.vlan_group_map = groups_by_rt_id

    def create_vlan_nb_map(self):
        if not self.vlan_group_map:
            self.create_vlan_domains_nb_group_map()
        rt_vlans = self.get_vlans_data()
        nb_vlans = netbox.get_nb_vlans()

        # pp.pprint(rt_vlans)
        rt_vlan_table = {}
        for line in rt_vlans:
            vlan_domain_id, vlan_id, vlan_type, vlan_descr = line
            vlan_domain_data = self.vlan_group_map[vlan_domain_id]

            found = False
            for nb_vlan_id, nb_vlan_data in nb_vlans.items():
                if nb_vlan_data["vid"] == vlan_id:
                    if nb_vlan_data["group"]["name"] == vlan_domain_data["name"]:
                        logger.debug(nb_vlan_data)
                        found = True
                        key = "{}_{}".format(vlan_domain_id, vlan_id)
                        rt_vlan_table[key] = nb_vlan_data
            if not found:
                logger.debug("unable to find a vlan. dying")
                logger.debug(line)
                exit(1)
        self.vlan_map = rt_vlan_table

    def get_vlans(self):
        resp = self.get_vlans_data()

        for line in resp:
            vlan_domain_id, vlan_id, vlan_type, vlan_descr = line
            vlan = {}
            vlan["group"] = self.vlan_group_map[vlan_domain_id]["id"]
            vlan["name"] = vlan_descr[: min(len(vlan_descr), 64)]  # limit char lenght
            vlan["vid"] = vlan_id
            vlan["description"] = vlan_descr
            logger.debug("adding vlan {}".format(vlan))
            netbox.post_vlan(vlan)

    def get_vlans_data(self):
        if not self.vlan_group_map:
            self.create_vlan_domains_nb_group_map()
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM VLANDescription order by domain_id desc;"
            cur.execute(q)
            resp = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("vlans", str(resp))
                logger.debug(msg)
            cur.close()
            self.con = None
        return resp

    def get_infrastructure(self, do_updates=True):
        """
        Get locations, rows and racks from RT, convert them to buildings and rooms and send to uploader.
        :return:
        """
        sites_map = {}
        rooms_map = {}
        rows_map = {}
        rackgroups = []
        racks = []

        # if not self.con:
        #     self.connect()

        # # ============ BUILDINGS AND ROOMS ============
        # with self.con:
        #     cur = self.con.cursor()
        #     q = """SELECT id, name, parent_id, parent_name FROM Location"""
        #     cur.execute(q)
        #     raw = cur.fetchall()

        #     for rec in raw:
        #         location_id, location_name, parent_id, parent_name = rec
        #         if not parent_name:
        #             sites_map.update({location_id: location_name})
        #         else:
        #             rooms_map.update({location_name: parent_name})
        #     cur.close()
        #     self.con = None
        # print("Sites:")
        # pp.pprint(sites_map)

        # pp.pprint(rooms_map)

        # print("Rack Groups:")
        # for room, parent in list(rooms_map.items()):
        #     if parent in sites_map.values():
        #         if room in rooms_map.values():
        #             continue

        #     rackgroup = {}

        #     if room not in sites_map.values():
        #         name = parent + "-" + room
        #         rackgroup.update({"site": rooms_map[parent]})
        #     else:
        #         name = room
        #         rackgroup.update({"site": parent})

        #     rackgroup.update({"name": name})

        #     rackgroups.append(rackgroup)

        # for site_id, site_name in list(sites_map.items()):
        #     if site_name not in rooms_map.values():
        #         rackgroup = {}
        #         rackgroup.update({"site": site_name})
        #         rackgroup.update({"name": site_name})

        #         rackgroups.append(rackgroup)

        # pp.pprint(rackgroups)

        # upload rooms
        # buildings = json.loads((netbox.get_buildings()))['buildings']

        #     for room, parent in list(rooms_map.items()):
        #         roomdata = {}
        #         roomdata.update({'name': room})
        #         roomdata.update({'building': parent})
        #         netbox.post_room(roomdata)

        # ============ ROWS AND RACKS ============
        netbox_sites_by_comment = netbox.get_sites_keyd_by_description()
        pp.pprint(netbox_sites_by_comment)
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT id, name ,height, row_id, row_name, location_id, location_name from Rack;"""
            cur.execute(q)
            raw = cur.fetchall()
            cur.close()
            self.con = None

        for rec in raw:
            rack_id, rack_name, height, row_id, row_name, location_id, location_name = rec

            rows_map.update({row_name: location_name})

            # prepare rack data. We will upload it a little bit later
            rack = {}
            rack.update({"name": rack_name})
            rack.update({"size": height})
            rack.update({"rt_id": rack_id})  # we will remove this later
            if config["Misc"]["ROW_AS_ROOM"]:
                rack.update({"room": row_name})
                rack.update({"building": location_name})
            else:
                row_name = row_name[:10]  # there is a 10char limit for row name
                rack.update({"row": row_name})
                if location_name in rooms_map:
                    rack.update({"room": location_name})
                    building_name = rooms_map[location_name]
                    rack.update({"building": building_name})
                else:
                    rack.update({"building": location_name})
            racks.append(rack)
        pprint.pprint(racks)

        # # upload rows as rooms
        # if config['Misc']['ROW_AS_ROOM']:
        #     if config['Log']['DEBUG']:
        #         msg = ('Rooms', str(rows_map))
        #         logger.debug(msg)
        #     for room, parent in list(rows_map.items()):
        #         roomdata = {}
        #         roomdata.update({'name': room})
        #         roomdata.update({'building': parent})
        #         netbox.post_room(roomdata)

        # upload racks
        if config["Log"]["DEBUG"]:
            msg = ("Racks", str(racks))
            # logger.debug(msg)
        for rack in racks:
            netbox_rack = {}
            netbox_rack["name"] = rack["name"]
            logger.debug("attempting to get site {} from netbox dict".format(rack["building"]))
            netbox_rack["site"] = netbox_sites_by_comment[rack["building"]]["id"]
            netbox_rack["comments"] = rack["room"]
            rt_tags = self.get_tags_for_obj("rack", rack["rt_id"])
            # print(rt_tags)
            tags = []
            # print (self.tag_map)
            for tag in rt_tags:
                try:
                    # print(tag)
                    tags.append(self.tag_map[tag]["id"])
                except:
                    logger.debug("failed to find tag {} in lookup map".format(tag))
            netbox_rack["tags"] = tags
            if rack["size"] == None:
                netbox_rack["u_height"] = 100
            else:
                netbox_rack["u_height"] = rack["size"]
            pp.pprint(netbox_rack)
            netbox.post_rack(netbox_rack)
            # response = netbox.post_rack(rack)

        #     self.rack_id_map.update({rt_rack_id: d42_rack_id})

        # self.all_ports = self.get_ports()

    def get_hardware(self):
        """
        Get hardware from RT
        :return:
        """
        if not self.con:
            self.connect()
        with self.con:
            # get hardware items (except PDU's)
            cur = self.con.cursor()
            q = (
                """SELECT
                    Object.id,Object.name as Description,
                    Object.label as Name,
                    Object.asset_no as Asset,
                    Dictionary.dict_value as Type,
                    Chapter.name
                    FROM Object
                    LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
                    LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
                    LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
                    LEFT JOIN Chapter on Dictionary.chapter_id = Chapter.id
                    WHERE 
                        Attribute.id=2 
                        AND Object.objtype_id != 2
                        """
                + config["Misc"]["hardware_data_filter"]
            )
            logger.debug(q)
            cur.execute(q)
        data = cur.fetchall()
        cur.close()
        self.con = None

        if config["Log"]["DEBUG"]:
            msg = ("Hardware", str(data))
            logger.debug(msg)

        # create map device_id:height
        # RT does not impose height for devices of the same hardware model so it might happen that -
        # two or more devices based on same HW model have different size in rack
        # here we try to find and set smallest U for device
        hwsize_map = {}
        logger.debug("about to get hardware sizes for existing services. this may take some time")
        for line in data:
            line = [0 if not x else x for x in line]
            data_id, description, name, asset, dtype, device_section = line
            size = self.get_hardware_size(data_id)
            if size:
                floor, height, depth, mount = size
                if data_id not in hwsize_map:
                    hwsize_map.update({data_id: height})
                else:
                    h = float(hwsize_map[data_id])
                    if float(height) < h:
                        hwsize_map.update({data_id: height})

        logger.debug(hwsize_map)
        hardware = {}
        for line in data:
            hwddata = {}
            line = [0 if not x else x for x in line]
            data_id, description, name, asset, dtype, device_section = line

            if "%GPASS%" in dtype:
                vendor, model = dtype.split("%GPASS%")
            elif len(dtype.split()) > 1:
                venmod = dtype.split()
                vendor = venmod[0]
                model = " ".join(venmod[1:])
            else:
                vendor = dtype
                model = dtype
            if "[[" in vendor:
                vendor = vendor.replace("[[", "").strip()
                name = model[:48].split("|")[0].strip()
            else:
                name = model[:48].strip()
            device_section = device_section.strip()
            if "models" in device_section:
                device_section = device_section.replace("models", "").strip()

            size = self.get_hardware_size(data_id)
            if size:
                floor, height, depth, mount = size
                # patching height
                height = hwsize_map[data_id]
                hwddata.update({"description": description})
                hwddata.update({"type": 1})
                hwddata.update({"size": height})
                hwddata.update({"depth": depth})
                hwddata.update({"name": str(name)})
                hwddata.update({"manufacturer": str(vendor)})
                hwddata.update({"rt_device_section": device_section})
                hwddata.update({"rt_dev_id": data_id})
                hardware[data_id] = hwddata
        return hardware

    def get_hardware_size(self, data_id):
        """
        Calculate hardware size.
        :param data_id: hw id
        :return:
            floor   - starting U location for the device in the rack
            height  - height of the device
            depth   - depth of the device (full, half)
            mount   - orientation of the device in the rack. Can be front or back
        """
        if not self.con:
            self.connect()
        with self.con:
            # get hardware items
            cur = self.con.cursor()
            q = """SELECT unit_no,atom FROM RackSpace WHERE object_id = %s""" % data_id
            cur.execute(q)
        data = cur.fetchall()
        cur.close()
        self.con = None
        if data != ():
            front = 0
            interior = 0
            rear = 0
            floor = 0
            depth = 1  # 1 for full depth (default) and 2 for half depth
            mount = "front"  # can be [front | rear]
            i = 1

            for line in data:
                flr, tag = line

                floor = int(flr)

                i += 1
                if tag == "front":
                    front += 1
                elif tag == "interior":
                    interior += 1
                elif tag == "rear":
                    rear += 1
            

            if front and interior and rear:  # full depth
                height = front
                if height > 1:
                    floor = floor - (height - 1)
                return floor, height, depth, mount

            elif front and interior and not rear:  # half depth, front mounted
                height = front
                depth = 2
                if height > 1:
                    floor = floor - (height - 1)
                return floor, height, depth, mount

            elif interior and rear and not front:  # half depth,  rear mounted
                height = rear
                depth = 2
                mount = "rear"
                if height > 1:
                    floor = floor - (height - 1)
                return floor, height, depth, mount

            # for devices that look like less than half depth:
            elif front and not interior and not rear:
                height = front
                depth = 2
                if height > 1:
                    floor = floor - (height - 1)
                return floor, height, depth, mount
            elif rear and not interior and not front:
                height = rear
                depth = 2
                mount = "rear"
                if height > 1:
                    floor = floor - (height - 1)
                return floor, height, depth, mount
            elif interior and not rear and not front:
                logger.warn("interior only mounted device. this is not nb compatible")
                return None, None, None, None
            else:
                return None, None, None, None
        else:
            return None, None, None, None

    def remove_links(self, item):
        if "[[" in item and "|" in item:
            item = item.replace("[[", "").strip()
            item = item.split("|")[0].strip()
        return item

    def get_device_types(self):
        if not self.hardware:
            self.hardware = self.get_hardware()
        rt_hardware = self.hardware
        rt_device_types = {}

        for device_id, device in rt_hardware.items():
            logger.debug(device)
            if device["name"] == device["manufacturer"]:
                key = device["name"]
            else:
                key = "{} {}".format(device["manufacturer"], device["name"])
            if not key in rt_device_types.keys():
                device_type = copy.deepcopy(device)
                if "description" in device_type.keys():
                    del device_type["description"]
                    del device_type["rt_dev_id"]
                rt_device_types[key] = device_type
        device_templates = self.match_device_types_to_netbox_templates(rt_device_types)
        # pp.pprint(device_templates)
        for device_type in device_templates["matched"].keys():
            # print(device_type)
            netbox.post_device_type(device_type, device_templates["matched"][device_type])

    def match_device_types_to_netbox_templates(self, device_types):
        unmatched = {}
        matched = {}

        for device_type_key in device_types.keys():
            if device_type_key in device_type_map_preseed["by_key_name"].keys():
                # print("found device type for {}".format(device_type_key))
                matched[device_type_key] = device_types[device_type_key]
                matched[device_type_key]["device_template_data"] = device_type_map_preseed["by_key_name"][device_type_key]
            else:
                # print("did not find device type {} in hardware_map.yaml".format(device_type_key))
                unmatched[device_type_key] = device_types[device_type_key]
        logger.debug("device templates found for importing: ")
        pp.pprint(matched)

        logger.debug("the following device types have no matching device templates:")
        for unmatched_device_type in sorted(unmatched.keys()):
            logger.debug(unmatched_device_type)
        if not config["Misc"]["SKIP_DEVICES_WITHOUT_TEMPLATE"] == "True":
            if len(unmatched) > 0:
                logger.debug("")
                logger.debug(
                    "please update hardware_map.yml with device maps or set SKIP_DEVICES_WITHOUT_TEMPLATE to True in conf file to skip devices without a matching template"
                )
                exit(22)
        return {"matched": matched, "unmatched": unmatched}

    @staticmethod
    def add_hardware(height, depth, name):
        """

        :rtype : object
        """
        hwddata = {}
        hwddata.update({"type": 1})
        if height:
            hwddata.update({"size": height})
        if depth:
            hwddata.update({"depth": depth})
        if name:
            hwddata.update({"name": name[:48]})
        logger.debug(hwddata)
        # netbox.post_hardware(hwddata)

    def get_vmhosts(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT id, name FROM Object WHERE objtype_id='1505'"""
            cur.execute(q)
            raw = cur.fetchall()
        cur.close()
        self.con = None
        dev = {}
        for rec in raw:
            host_id = int(rec[0])
            try:
                name = rec[1].strip()
            except AttributeError:
                continue
            self.vm_hosts.update({host_id: name})
            dev.update({"name": name})
            dev.update({"is_it_virtual_host": "yes"})

    def get_chassis(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT id, name FROM Object WHERE objtype_id='1502'"""
            cur.execute(q)
            raw = cur.fetchall()
        cur.close()
        self.con = None
        dev = {}
        for rec in raw:
            host_id = int(rec[0])
            try:
                name = rec[1].strip()
            except AttributeError:
                continue
            self.chassis.update({host_id: name})
            dev.update({"name": name})
            dev.update({"is_it_blade_host": "yes"})

    def get_container_map(self):
        """
        Which VM goes into which VM host?
        Which Blade goes into which Chassis ?
        :return:
        """
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT parent_entity_id AS container_id, child_entity_id AS object_id
                    FROM EntityLink WHERE child_entity_type='object' AND parent_entity_type = 'object'"""
            cur.execute(q)
            raw = cur.fetchall()
        cur.close()
        self.con = None
        for rec in raw:
            container_id, object_id = rec
            self.container_map.update({object_id: container_id})

    def get_devices(self):

        self.get_vmhosts()
        self.get_chassis()
        if not self.tag_map:
            self.create_tag_map()
        self.all_ports = self.get_ports()
        if not netbox.device_types:
            netbox.device_types = {str(item.slug): dict(item) for item in py_netbox.dcim.device_types.all()}
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            # get object IDs
            q = f"""SELECT id FROM Object WHERE  {config["Misc"]["device_data_filter_obj_only"]} """
            cur.execute(q)
            idsx = cur.fetchall()
        ids = [x[0] for x in idsx]
        cur.close()
        self.con = None

        for dev_id in ids:
            try:
                if not self.con:
                    self.connect()
                    cur = self.con.cursor()
                q = f"""Select
                            Object.id,
                            Object.objtype_id,
                            Object.name as Description,
                            Object.label as Name,
                            Object.asset_no as Asset,
                            Attribute.name as Name,
                            Dictionary.dict_value as Type,
                            Object.comment as Comment,
                            RackSpace.rack_id as RackID,
                            Rack.name as rack_name,
                            Rack.row_name,
                            Rack.location_id,
                            Rack.location_name,
                            Location.parent_name,
                            COALESCE(AttributeValue.string_value,AttributeValue.uint_value,AttributeValue.float_value,'') as attrib_value,
                            Attribute.type

                            FROM Object
                            left join Dictionary as Dictionary2 on Dictionary2.dict_key = Object.objtype_id
                            LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
                            LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
                            LEFT JOIN RackSpace ON Object.id = RackSpace.object_id
                            LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
                            LEFT JOIN Rack ON RackSpace.rack_id = Rack.id
                            LEFT JOIN Location ON Rack.location_id = Location.id
                            LEFT JOIN Chapter on Dictionary.chapter_id = Chapter.id
                            WHERE Object.id = {dev_id}
                            AND Object.objtype_id not in (2,9,1504,1505,1506,1507,1560,1561,1562,50275) 
                            {config["Misc"]["device_data_filter"]} """
                logger.debug(q)

                cur.execute(q)
                data = cur.fetchall()
                # print(json.dumps(data))
                cur.close()
                self.con = None
                if data:  # RT objects that do not have data are locations, racks, rows etc...
                    self.process_data(data, dev_id)
            except:
                sleep(2)
                if not self.con:
                    self.connect()
                    cur = self.con.cursor()
                q = f"""Select
                            Object.id,
                            Object.objtype_id,
                            Object.name as Description,
                            Object.label as Name,
                            Object.asset_no as Asset,
                            Attribute.name as Name,
                            Dictionary.dict_value as Type,
                            Object.comment as Comment,
                            RackSpace.rack_id as RackID,
                            Rack.name as rack_name,
                            Rack.row_name,
                            Rack.location_id,
                            Rack.location_name,
                            Location.parent_name,
                            COALESCE(AttributeValue.string_value,AttributeValue.uint_value,AttributeValue.float_value,'') as attrib_value,
                            Attribute.type

                            FROM Object
                            left join Dictionary as Dictionary2 on Dictionary2.dict_key = Object.objtype_id
                            LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
                            LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
                            LEFT JOIN RackSpace ON Object.id = RackSpace.object_id
                            LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
                            LEFT JOIN Rack ON RackSpace.rack_id = Rack.id
                            LEFT JOIN Location ON Rack.location_id = Location.id
                            LEFT JOIN Chapter on Dictionary.chapter_id = Chapter.id
                            WHERE Object.id = {dev_id}
                            AND Object.objtype_id not in (2,9,1504,1505,1506,1507,1560,1561,1562,50275) 
                            {config["Misc"]["device_data_filter"]} """
                logger.debug(q)

                cur.execute(q)
                data = cur.fetchall()
                # print(json.dumps(data))
                cur.close()
                self.con = None
                if data:  # RT objects that do not have data are locations, racks, rows etc...
                    self.process_data(data, dev_id)
        logger.debug("skipped devices:")
        pp.pprint(self.skipped_devices)

    def get_obj_location(self, obj_id):
        if not self.con:
            self.connect()

        cur = self.con.cursor()
        # get object IDs
        q = f"""
            SELECT Rack.id,Rack.name,Rack.row_id,Rack.row_name,Rack.location_id,Rack.location_name 
            FROM EntityLink 
            left join Rack on EntityLink.parent_entity_id = Rack.id 
            WHERE parent_entity_type = 'rack' 
            AND child_entity_type = 'object' 
            AND child_entity_id = {obj_id}
        """
        cur.execute(q)
        idsx = cur.fetchall()
        try:
            resp = [x for x in idsx][0]
        except:
            resp = [None, None, None, None, None, "Unknown"]

        cur.close()
        self.con = None
        return resp

    def process_data(self, data, dev_id):
        devicedata = {}
        devicedata["custom_fields"] = {}
        device2rack = {}
        name = None
        opsys = None
        hardware = None
        note = None
        rrack_id = None
        floor = None
        dev_type = 0
        process_object = True
        bad_tag = False

        if process_object:
            for x in data:
                (
                    rt_object_id,
                    dev_type,
                    rdesc,
                    rname,
                    rasset,
                    rattr_name,
                    dict_dictvalue,
                    rcomment,
                    rrack_id,
                    rrack_name,
                    rrow_name,
                    rlocation_id,
                    rlocation_name,
                    rparent_name,
                    attrib_value,
                    attrib_type,
                ) = x
                logger.debug(x)

                name = self.remove_links(rdesc)
                if rcomment:
                    try:
                        note = rname + "\n" + rcomment
                    except:
                        note = rcomment
                else:
                    note = rname

                if "Operating System" in x:
                    opsys = dict_dictvalue
                    opsys = self.remove_links(opsys)
                    if "%GSKIP%" in opsys:
                        opsys = opsys.replace("%GSKIP%", " ")
                    if "%GPASS%" in opsys:
                        opsys = opsys.replace("%GPASS%", " ")
                    devicedata["custom_fields"]["Operating System"] = str(opsys)
                elif "SW type" in x:
                    opsys = dict_dictvalue
                    opsys = self.remove_links(opsys)
                    if "%GSKIP%" in opsys:
                        opsys = opsys.replace("%GSKIP%", " ")
                    if "%GPASS%" in opsys:
                        opsys = opsys.replace("%GPASS%", " ")
                    devicedata["custom_fields"]["SW type"] = str(opsys)

                elif "Server Hardware" in x:
                    hardware = dict_dictvalue
                    hardware = self.remove_links(hardware)
                    if "%GSKIP%" in hardware:
                        hardware = hardware.replace("%GSKIP%", " ")
                    if "%GPASS%" in hardware:
                        hardware = hardware.replace("%GPASS%", " ")
                    if "\t" in hardware:
                        hardware = hardware.replace("\t", " ")

                elif "HW type" in x:
                    hardware = dict_dictvalue
                    hardware = self.remove_links(hardware)
                    if "%GSKIP%" in hardware:
                        hardware = hardware.replace("%GSKIP%", " ")
                    if "%GPASS%" in hardware:
                        hardware = hardware.replace("%GPASS%", " ")
                    if "\t" in hardware:
                        hardware = hardware.replace("\t", " ")
                elif "BiosRev" in x:
                    biosrev = self.remove_links(dict_dictvalue)
                    devicedata["custom_fields"]["BiosRev"] = biosrev
                else:
                    if not rattr_name == None:
                        if attrib_type == "dict":
                            attrib_value_unclean = dict_dictvalue
                        else:
                            attrib_value_unclean = attrib_value
                        cleaned_val = netbox.cleanup_attrib_value(attrib_value_unclean, attrib_type)
                        # print(cleaned_val)
                        devicedata["custom_fields"][rattr_name] = cleaned_val
                        config_cust_field_map = json.loads(config["Misc"]["CUSTOM_FIELD_MAPPER"])
                        if rattr_name in config_cust_field_map.keys():
                            devicedata[config_cust_field_map[rattr_name]] = cleaned_val
                if rasset:
                    devicedata["asset_tag"] = rasset
                devicedata["custom_fields"]["rt_id"] = str(rt_object_id)
                devicedata["custom_fields"]["Visible label"] = str(rname)

                if note:
                    note = note.replace("\n", "\n\n")  # markdown. all new lines need two new lines

            if not "tags" in devicedata.keys():
                rt_tags = self.get_tags_for_obj("object", int(devicedata["custom_fields"]["rt_id"]))
                tags = []
                # print (self.tag_map)

                for tag in rt_tags:
                    try:
                        # print(tag)
                        tags.append(self.tag_map[tag]["id"])
                    except:
                        logger.debug("failed to find tag {} in lookup map".format(tag))
                devicedata["tags"] = tags

            bad_tags = []
            for tag_check in config["Misc"]["SKIP_OBJECTS_WITH_TAGS"].strip().split(","):
                logger.debug(f"checking for tag '{tag_check}'")
                if self.tag_map[tag_check]["id"] in devicedata["tags"]:
                    logger.debug(f"tag matched by id")
                    bad_tag = True
                    bad_tags.append(tag_check)
            if bad_tag:
                process_object = False
                name = None
                logger.info(f"skipping object rt_id:{rt_object_id} as it has tags: {str(bad_tags)}")

            # 0u device logic
            zero_location_obj_data = None
            if rlocation_name == None:
                zero_location_obj_data = self.get_obj_location(rt_object_id)
                rlocation_name = zero_location_obj_data[5]
                rrack_id = zero_location_obj_data[0]
                rrack_name = zero_location_obj_data[1]
                print(zero_location_obj_data)
                print(f"obj location (probably 0u device): {rlocation_name}")

            if name:
                # set device data
                devicedata.update({"name": name})
                if hardware:
                    devicedata.update({"hardware": hardware[:48]})
                if opsys:
                    devicedata.update({"os": opsys})
                if note:
                    devicedata.update({"comments": note})
                if dev_id in self.vm_hosts:
                    devicedata.update({"is_it_virtual_host": "yes"})
                if dev_type == 8:
                    devicedata.update({"is_it_switch": "yes"})
                elif dev_type == 1502:
                    devicedata.update({"is_it_blade_host": "yes"})
                elif dev_type == 4:
                    try:
                        blade_host_id = self.container_map[dev_id]
                        blade_host_name = self.chassis[blade_host_id]
                        devicedata.update({"type": "blade"})
                        devicedata.update({"blade_host": blade_host_name})
                    except KeyError:
                        # print("ERROR: failed to track down blade info")
                        pass
                elif dev_type == 1504:
                    devicedata.update({"type": "virtual"})
                    devicedata.pop("hardware", None)
                    try:
                        vm_host_id = self.container_map[dev_id]
                        vm_host_name = self.vm_hosts[vm_host_id]
                        devicedata.update({"virtual_host": vm_host_name})
                    except KeyError:
                        logger.debug("ERROR: failed to track down virtual host info")
                        pass

                d42_rack_id = None
                # except VMs

                if dev_type != 1504:
                    if rrack_id:
                        rack_detail = dict(py_netbox.dcim.racks.get(name=rrack_name))
                        rack_id = rack_detail["id"]
                        devicedata.update({"rack": rack_id})
                        d42_rack_id = rack_id

                        # if the device is mounted in RT, we will try to add it to D42 hardwares.
                        position, height, depth, mount = self.get_hardware_size(dev_id)
                        devicedata.update({"position": position})
                        devicedata.update({"face": mount})
                        # 0u device logic
                        if height == None and not zero_location_obj_data == None:
                            height = 0
                            depth = 0
                    else:
                        height = 0
                        depth = 0

                netbox_sites_by_comment = netbox.get_sites_keyd_by_description()
                devicedata["site"] = netbox_sites_by_comment[rlocation_name]["id"]
                devicedata["device_role"] = 1
                # devicedata['device_type'] = 22
                if not "hardware" in devicedata.keys():
                    if height == None:
                        height = 0
                    generic_depth = ""

                    if depth:
                        print("depth:")
                        print(depth)
                        if depth == 2:
                            generic_depth = "short_"
                    devicedata["hardware"] = f"generic_{height}u_{generic_depth}device"
                logger.debug(devicedata["hardware"])
                if str(devicedata["hardware"]) in device_type_map_preseed["by_key_name"].keys():
                    logger.debug("hardware match")
                    # print(str(devicedata['hardware']))
                    nb_slug = device_type_map_preseed["by_key_name"][str(devicedata["hardware"])]["slug"]
                    if nb_slug in netbox.device_types:
                        logger.debug("found template in netbox")
                        devicedata["device_type"] = netbox.device_types[nb_slug]["id"]
                    else:
                        logger.debug("did not find matching device template in netbox")
                        if not config["Misc"]["SKIP_DEVICES_WITHOUT_TEMPLATE"] == "True":
                            logger.debug("device with no matching template by slugname {nb_slug} found")
                            exit(112)
                else:
                    if not devicedata["hardware"] in self.skipped_devices.keys():
                        self.skipped_devices[devicedata["hardware"]] = 1
                    else:
                        self.skipped_devices[devicedata["hardware"]] = self.skipped_devices[devicedata["hardware"]] + 1
                    logger.debug("hardware type missing: {}".format(devicedata["hardware"]))

                # upload device
                if devicedata:
                    if hardware and dev_type != 1504:
                        devicedata.update({"hardware": hardware[:48]})

                    # set default type for racked devices
                    if "type" not in devicedata and d42_rack_id and floor:
                        devicedata.update({"type": "physical"})

                    logger.debug(json.dumps(devicedata))
                    netbox.post_device(devicedata, py_netbox)

                    # update ports
                    if dev_type == 8 or dev_type == 4 or dev_type == 445 or dev_type == 1055:
                        # ports = self.get_ports_by_device(self.all_ports, dev_id)
                        ports = False
                        if ports:
                            for item in ports:
                                switchport_data = {
                                    "port": item[0],
                                    "switch": name,
                                    "label": item[1],
                                }

                                get_links = self.get_links(item[3])
                                if get_links:
                                    device_name = self.get_device_by_port(get_links[0])
                                    switchport_data.update({"device": device_name})
                                    switchport_data.update({"remote_device": device_name})
                                    # switchport_data.update({'remote_port': self.get_port_by_id(self.all_ports, get_links[0])})

                                    # netbox.post_switchport(switchport_data)

                                    # reverse connection
                                    device_name = self.get_device_by_port(get_links[0])
                                    switchport_data = {
                                        "port": self.get_port_by_id(self.all_ports, get_links[0]),
                                        "switch": device_name,
                                    }

                                    switchport_data.update({"device": name})
                                    switchport_data.update({"remote_device": name})
                                    switchport_data.update({"remote_port": item[0]})

                                    # netbox.post_switchport(switchport_data)
                                # else:
                                # netbox.post_switchport(switchport_data)

                    # # if there is a device, we can try to mount it to the rack
                    # if dev_type != 1504 and d42_rack_id and floor:  # rack_id is D42 rack id
                    #     device2rack.update({"device": name})
                    #     if hardware:
                    #         device2rack.update({"hw_model": hardware[:48]})
                    #     device2rack.update({"rack_id": d42_rack_id})
                    #     device2rack.update({"start_at": floor})
                    #     logger.debug(device2rack)
                    #     # netbox.post_device2rack(device2rack)
                    # else:
                    #     if dev_type != 1504 and d42_rack_id is not None:
                    #         msg = (
                    #             '\n-----------------------------------------------------------------------\
                    #         \n[!] INFO: Cannot mount device "%s" (RT id = %d) to the rack.\
                    #         \n\tFloor returned from "get_hardware_size" function was: %s'
                    #             % (name, dev_id, str(floor))
                    #         )
                    #         logger.info(msg)
                else:
                    msg = (
                        "\n-----------------------------------------------------------------------\
                    \n[!] INFO: Device %s (RT id = %d) cannot be uploaded. Data was: %s"
                        % (name, dev_id, str(devicedata))
                    )
                    logger.info(msg)

            else:
                # device has no name thus it cannot be migrated
                if bad_tag:
                    msg2 = f"Device with RT id={dev_id} cannot be migrated because it has bad tags."
                else:
                    msg2 = f"Device with RT id={dev_id} cannot be migrated because it has no name."
                msg = f"\n-----------------------------------------------------------------------\
                \n[!] INFO: {msg2} "
                logger.info(msg)

    def get_device_to_ip(self):
        if not self.con:
            self.connect()
        with self.con:
            # get hardware items (except PDU's)
            cur = self.con.cursor()
            q = (
                """SELECT
                    IPv4Allocation.ip,IPv4Allocation.name,
                    Object.name as hostname
                    FROM %s.`IPv4Allocation`
                    LEFT JOIN Object ON Object.id = object_id"""
                % config["MySQL"]["DB_NAME"]
            )
            cur.execute(q)
        data = cur.fetchall()
        cur.close()
        self.con = None

        if config["Log"]["DEBUG"]:
            msg = ("Device to IP", str(data))
            logger.debug(msg)

        for line in data:
            devmap = {}
            rawip, nic_name, hostname = line
            ip = self.convert_ip(rawip)
            devmap.update({"ipaddress": ip})
            devmap.update({"device": hostname})
            if nic_name:
                devmap.update({"tag": nic_name})
            netbox.post_ip(devmap)

    def get_pdus(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT
                    Object.id,Object.name as Name, Object.asset_no as Asset,
                    Object.comment as Comment, Dictionary.dict_value as Type, RackSpace.atom as Position,
                    (SELECT Object.id FROM Object WHERE Object.id = RackSpace.rack_id) as RackID
                    FROM Object
                    LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
                    LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
                    LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
                    LEFT JOIN RackSpace ON RackSpace.object_id = Object.id
                    WHERE Object.objtype_id = 2
                  """
            cur.execute(q)
        data = cur.fetchall()

        if config["Log"]["DEBUG"]:
            msg = ("PDUs", str(data))
            logger.debug(msg)

        rack_mounted = []
        pdumap = {}
        pdumodels = []
        pdu_rack_models = []

        for line in data:
            pdumodel = {}
            pdudata = {}
            line = ["" if x is None else x for x in line]
            pdu_id, name, asset, comment, pdu_type, position, rack_id = line

            if "%GPASS%" in pdu_type:
                pdu_type = pdu_type.replace("%GPASS%", " ")

            pdu_type = pdu_type[:64]
            pdudata.update({"name": name})
            pdudata.update({"notes": comment})
            pdudata.update({"pdu_model": pdu_type})
            pdumodel.update({"name": pdu_type})
            pdumodel.update({"pdu_model": pdu_type})
            if rack_id:
                floor, height, depth, mount = self.get_hardware_size(pdu_id)
                pdumodel.update({"size": height})
                pdumodel.update({"depth": depth})

            # post pdu models
            if pdu_type and name not in pdumodels:
                netbox.post_pdu_model(pdumodel)
                pdumodels.append(pdumodel)
            elif pdu_type and rack_id:
                if pdu_id not in pdu_rack_models:
                    netbox.post_pdu_model(pdumodel)
                    pdu_rack_models.append(pdu_id)

            # post pdus
            if pdu_id not in pdumap:
                response = netbox.post_pdu(pdudata)
                d42_pdu_id = response["msg"][1]
                pdumap.update({pdu_id: d42_pdu_id})

            # mount to rack
            if position:
                if pdu_id not in rack_mounted:
                    rack_mounted.append(pdu_id)
                    floor, height, depth, mount = self.get_hardware_size(pdu_id)
                    if floor is not None:
                        floor = int(floor) + 1
                    else:
                        floor = "auto"
                    try:
                        d42_rack_id = self.rack_id_map[rack_id]
                        if floor:
                            rdata = {}
                            rdata.update({"pdu_id": pdumap[pdu_id]})
                            rdata.update({"rack_id": d42_rack_id})
                            rdata.update({"pdu_model": pdu_type})
                            rdata.update({"where": "mounted"})
                            rdata.update({"start_at": floor})
                            rdata.update({"orientation": mount})
                            netbox.post_pdu_to_rack(rdata, d42_rack_id)
                    except TypeError:
                        msg = (
                            '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount pdu "%s" (RT id = %d) to the rack.\
                        \n\tFloor returned from "get_hardware_size" function was: %s'
                            % (name, pdu_id, str(floor))
                        )
                        logger.info(msg)
                    except KeyError:
                        msg = (
                            '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount pdu "%s" (RT id = %d) to the rack.\
                        \n\tWrong rack id map value: %s'
                            % (name, pdu_id, str(rack_id))
                        )
                        logger.info(msg)
            # It's Zero-U then
            else:
                rack_id = self.get_rack_id_for_zero_us(pdu_id)
                if rack_id:
                    try:
                        d42_rack_id = self.rack_id_map[rack_id]
                    except KeyError:
                        msg = (
                            '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount pdu "%s" (RT id = %d) to the rack.\
                        \n\tWrong rack id map value: %s'
                            % (name, pdu_id, str(rack_id))
                        )
                        logger.info(msg)
                    if config["Misc"]["PDU_MOUNT"].lower() in (
                        "left",
                        "right",
                        "above",
                        "below",
                    ):
                        where = config["Misc"]["PDU_MOUNT"].lower()
                    else:
                        where = "left"
                    if config["Misc"]["PDU_ORIENTATION"].lower() in ("front", "back"):
                        mount = config["Misc"]["PDU_ORIENTATION"].lower()
                    else:
                        mount = "front"
                    rdata = {}

                    try:
                        rdata.update({"pdu_id": pdumap[pdu_id]})
                        rdata.update({"rack_id": d42_rack_id})
                        rdata.update({"pdu_model": pdu_type})
                        rdata.update({"where": where})
                        rdata.update({"orientation": mount})
                        netbox.post_pdu_to_rack(rdata, d42_rack_id)
                    except UnboundLocalError:
                        msg = (
                            '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount pdu "%s" (RT id = %d) to the rack.\
                        \n\tWrong rack id: %s'
                            % (name, pdu_id, str(rack_id))
                        )
                        logger.info(msg)

    def get_patch_panels(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT
                   id,
                   name,
                   AttributeValue.uint_value
                   FROM Object
                   LEFT JOIN AttributeValue ON AttributeValue.object_id = id AND AttributeValue.attr_id = 6
                   WHERE Object.objtype_id = 9
                 """
            cur.execute(q)
        data = cur.fetchall()

        if config["Log"]["DEBUG"]:
            msg = ("PDUs", str(data))
            logger.debug(msg)

        for item in data:
            ports = self.get_ports_by_device(self.all_ports, item[0])
            patch_type = "singular"
            port_type = None

            if isinstance(ports, list) and len(ports) > 0:
                if len(ports) > 1:
                    types = []

                    # check patch_type
                    for port in ports:
                        if port[2][:12] not in types:
                            types.append(port[2][:12])

                    if len(types) > 1:
                        patch_type = "modular"
                        for port in ports:
                            netbox.post_patch_panel_module_models(
                                {
                                    "name": port[0],
                                    "port_type": port[2][:12],
                                    "number_of_ports": 1,
                                    "number_of_ports_in_row": 1,
                                }
                            )

                if patch_type == "singular":
                    port_type = ports[0][2][:12]

            payload = {
                "name": item[1],
                "type": patch_type,
                "number_of_ports": item[2],
                "number_of_ports_in_row": item[2],
            }

            if port_type is not None:
                payload.update({"port_type": port_type})

            netbox.post_patch_panel(payload)

    def get_ports(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT
                    name,
                    label,
                    PortOuterInterface.oif_name,
                    Port.id,
                    object_id
                    FROM Port
                    LEFT JOIN PortOuterInterface ON PortOuterInterface.id = type"""
            cur.execute(q)
        data = cur.fetchall()
        cur.close()
        self.con = None
        if data:
            return data
        else:
            return False

    @staticmethod
    def get_ports_by_device(ports, device_id):
        device_ports = []
        for port in ports:
            if port[4] == device_id:
                device_ports.append(port)

        return device_ports

    @staticmethod
    def get_port_by_id(ports, port_id):
        for port in ports:
            if port[3] == port_id:
                return port[0]

    def get_device_by_port(self, port_id):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = (
                """SELECT
                    name
                    FROM Object
                    WHERE id = ( SELECT object_id FROM Port WHERE id = %s )"""
                % port_id
            )
            cur.execute(q)
        data = cur.fetchone()
        cur.close()
        self.con = None
        if data:
            return data[0]
        else:
            return False

    def get_links(self, port_id):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = (
                """SELECT
                    porta,
                    portb
                    FROM Link
                    WHERE portb = %s"""
                % port_id
            )
            cur.execute(q)
        data = cur.fetchall()
        cur.close()
        self.con = None
        if data:
            return data[0]
        else:
            if not self.con:
                self.connect()
            with self.con:
                cur = self.con.cursor()
                q = (
                    """SELECT
                        portb,
                        porta
                        FROM Link
                        WHERE porta = %s"""
                    % port_id
                )
                cur.execute(q)
            data = cur.fetchall()
            cur.close()
            self.con = None
            if data:
                return data[0]
            else:
                return False

    def get_rack_id_for_zero_us(self, pdu_id):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = (
                """SELECT
                    EntityLink.parent_entity_id
                    FROM EntityLink
                    WHERE EntityLink.child_entity_id = %s
                    AND EntityLink.parent_entity_type = 'rack'"""
                % pdu_id
            )
            cur.execute(q)
        data = cur.fetchone()
        if data:
            return data[0]


if __name__ == "__main__":
    # Import config
    configfile = "conf"
    config = configparser.RawConfigParser()
    config.read(configfile)

    # Initialize Data pretty printer
    pp = pprint.PrettyPrinter(indent=4, width=100)

    # Initialize logging platform
    logger = logging.getLogger("racktables2netbox")
    logger.setLevel(logging.DEBUG)

    # Log to file
    fh = logging.FileHandler(config["Log"]["LOGFILE"])
    fh.setLevel(logging.DEBUG)

    # Log to stdout
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Format log output
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Attach handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    # Load lookup map of yaml data
    with open("hardware_map.yaml", "r") as stream:
        device_type_map_preseed = yaml.safe_load(stream)

    py_netbox = pynetbox.api(config["NetBox"]["NETBOX_HOST"], token=config["NetBox"]["NETBOX_TOKEN"])

    tenant_groups = py_netbox.tenancy.tenant_groups.all()

    netbox = NETBOX(py_netbox)
    racktables = DB()
    if config["Migrate"]["TAGS"] == "True":
        logger.debug("running get tags")
        racktables.get_tags()
    if config["Migrate"]["CUSTOM_ATTRIBUTES"] == "True":
        logger.debug("running get_custom_attribs")
        racktables.get_custom_attribs()
    if config["Migrate"]["INFRA"] == "True":
        logger.debug("running get infra")
        racktables.get_infrastructure()
    if config["Migrate"]["VLAN"] == "True":
        racktables.get_vlan_domains()
        racktables.get_vlans()
    if config["Migrate"]["SUBNETS"] == "True":
        logger.debug("running get subnets")
        racktables.get_subnets()
        racktables.get_subnets_v6()
    if config["Migrate"]["IPS"] == "True":
        logger.debug("running get ips")
        racktables.get_ips()
        racktables.get_ips_v6()
    if config["Migrate"]["HARDWARE"] == "True":
        # print("running device types")
        # racktables.get_device_types()
        logger.debug("running manage hardware")
        racktables.get_devices()
    # racktables.get_container_map()
    # racktables.get_chassis()
    # racktables.get_vmhosts()
    # racktables.get_device_to_ip()
    # racktables.get_pdus()
    # racktables.get_patch_panels()
    # racktables.get_devices()

    migrator = Migrator()

    logger.info("[!] Done!")
    # sys.exit()
