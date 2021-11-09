#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__version__ = 1.00

import configparser
import json
import logging
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
class REST(object):
    def __init__(self):
        self.base_url = "{}/api".format(config["NetBox"]["NETBOX_HOST"])

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

    def uploader(self, data, url, method="POST"):

        logger.debug("HTTP Request: {} - {} - {}".format(method, url, data))

        try:
            request = requests.Request(method, url, data=json.dumps(data))
            prepared_request = self.s.prepare_request(request)
            r = self.s.send(prepared_request)
            logger.debug(f"HTTP Response: {r.status_code!s} - {r.reason}")
            # print(r.text)
            r.raise_for_status()
            r.close()
        except:
            print("POST attempt failed")
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
        print(r.text)

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
                print("fetch attempt failed")
            try:
                if r:
                    if r.status_code == 200:
                        return r.text
            except:
                test = ""
            current_attempt = current_attempt + 1
        print("failed to get {} 3 times".format(url))
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
                print("site: {} {} has no description set, skipping".format(site["display"], site["url"]))
            else:
                if not site["description"] in resp.keys():
                    resp[site["description"]] = site
                else:
                    print("duplicate description detected! {}".format(site["description"]))
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
        print(tags)
        return tags

    def post_vlan_group(self, group_name):
        url = self.base_url + "/extras/tags/"
        data = {}
        data["name"] = str(group_name)
        data["description"] = str(group_name)
        data["slug"] = str(group_name).lower().replace(" ", "-").replace(":", "")
        self.uploader2(data, url)

    # def post_device(self, data):
    #     url = self.base_url + '/api/1.0/device/'
    #     logger.info('Posting device data to {}'.format(url))
    #     self.uploader(data, url)

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

    # def post_hardware(self, data):
    #     url = self.base_url + '/api/1.0/hardwares/'
    #     logger.info('Adding hardware data to {}'.format(url))
    #     self.uploader(data, url)

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


class DB(object):
    """
    Fetching data from Racktables and converting them to Device42 API format.
    """

    def __init__(self):
        self.con = None
        self.hardware = None
        self.tag_map = None
        self.tables = []
        self.rack_map = []
        self.vm_hosts = {}
        self.chassis = {}
        self.rack_id_map = {}
        self.container_map = {}
        self.building_room_map = {}

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
                rest.post_ip(net)

        for line in ip_by_allocation:
            net = {}
            object_id, allocationip_raw = line
            ip = self.convert_ip(allocationip_raw)
            if not ip in adrese:
                net.update({"address": ip})
                msg = "IP Address: %s" % ip
                logger.info(msg)
                rest.post_ip(net)

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
            rest.post_ip(net)

        for line in ip_by_allocation:
            net = {}
            object_id, allocationip_raw = line
            ip = self.convert_ip_v6(allocationip_raw)
            if not ip in adrese:
                net.update({"address": ip})
                msg = "IP Address: %s" % ip
                logger.info(msg)
                rest.post_ip(net)

    def create_tag_map(self):
        print("creating tag map")
        self.tag_map = rest.get_tags_key_by_name()
        print("there are {} tags cached".format(len(self.tag_map)))
        print(self.tag_map.keys())

    def get_subnets(self):
        """
        Fetch subnets from RT and send them to upload function
        :return:
        """
        subs = {}
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM IPv4Network"
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
            sid, raw_sub, mask, name, x = line
            subnet = self.convert_ip(raw_sub)
            rt_tags = self.get_tags_for_obj("ipv4net", sid)
            # print(rt_tags)
            tags = []
            # print (self.tag_map)
            for tag in rt_tags:
                try:
                    # print(tag)
                    tags.append(self.tag_map[tag]["id"])
                except:
                    print("failed to find tag {} in lookup map".format(tag))
            subs.update({"prefix": "/".join([subnet, str(mask)])})
            subs.update({"status": "active"})
            # subs.update({'mask_bits': str(mask)})
            subs.update({"description": name})
            subs.update({"tags": tags})
            rest.post_subnet(subs)

    def get_subnets_v6(self):
        """
        Fetch subnets from RT and send them to upload function
        :return:
        """
        subs = {}
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM IPv6Network"
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
            sid, raw_sub, mask, last_ip, name, comment = line
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
                    print("failed to find tag {} in lookup map".format(tag))
            subs.update({"prefix": "/".join([subnet, str(mask)])})
            subs.update({"status": "active"})
            # subs.update({'mask_bits': str(mask)})
            subs.update({"description": name})
            subs.update({"tags": tags})
            rest.post_subnet(subs)

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
            rest.post_tag(tag, description)

    def get_vlan_domains(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = 'SELECT * FROM VLANDomain;'
            cur.execute(q)
            resp = cur.fetchall()
            if config["Log"]["DEBUG"]:
                msg = ("vlan_domains", str(resp))
                logger.debug(msg)
            cur.close()
            self.con = None

        for line in resp:
            id, group_id, description = line
            rest.post_vlan_group(description)

    def get_infrastructure(self):
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
        # buildings = json.loads((rest.get_buildings()))['buildings']

        #     for room, parent in list(rooms_map.items()):
        #         roomdata = {}
        #         roomdata.update({'name': room})
        #         roomdata.update({'building': parent})
        #         rest.post_room(roomdata)

        # ============ ROWS AND RACKS ============
        netbox_sites_by_comment = rest.get_sites_keyd_by_description()
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
        #         rest.post_room(roomdata)

        # upload racks
        if config["Log"]["DEBUG"]:
            msg = ("Racks", str(racks))
            # logger.debug(msg)
        for rack in racks:
            netbox_rack = {}
            netbox_rack["name"] = rack["name"]
            print("attempting to get site {} from netbox dict".format(rack["building"]))
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
                    print("failed to find tag {} in lookup map".format(tag))
            netbox_rack["tags"] = tags
            if rack["size"] == None:
                netbox_rack["u_height"] = 100
            else:
                netbox_rack["u_height"] = rack["size"]
            pp.pprint(netbox_rack)
            rest.post_rack(netbox_rack)
            # response = rest.post_rack(rack)

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
                    Object.id,Object.name as Description, Object.label as Name,
                    Object.asset_no as Asset,Dictionary.dict_value as Type
                    FROM Object
                    LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
                    LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
                    LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
                    WHERE 
                        Attribute.id=2 
                        AND Object.objtype_id != 2
                        """
                + config["Misc"]["hardware_data_filter"]
            )
            print(q)
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
        for line in data:
            line = [0 if not x else x for x in line]
            data_id, description, name, asset, dtype = line
            print(line)
            size = self.get_hardware_size(data_id)
            if size:
                floor, height, depth, mount = size
                if data_id not in hwsize_map:
                    hwsize_map.update({data_id: height})
                else:
                    h = float(hwsize_map[data_id])
                    if float(height) < h:
                        hwsize_map.update({data_id: height})

        print(hwsize_map)
        hardware = {}
        for line in data:
            hwddata = {}
            line = [0 if not x else x for x in line]
            data_id, description, name, asset, dtype = line

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

                if i == 1:
                    floor = int(flr) - 1  # '-1' since RT rack starts at 1 and Device42 starts at 0.
                else:
                    if int(flr) < floor:
                        floor = int(flr) - 1
                i += 1
                if tag == "front":
                    front += 1
                elif tag == "interior":
                    interior += 1
                elif tag == "rear":
                    rear += 1

            if front and interior and rear:  # full depth
                height = front
                return floor, height, depth, mount

            elif front and interior and not rear:  # half depth, front mounted
                height = front
                depth = 2
                return floor, height, depth, mount

            elif interior and rear and not front:  # half depth,  rear mounted
                height = rear
                depth = 2
                mount = "rear"
                return floor, height, depth, mount

            # for devices that look like less than half depth:
            elif front and not interior and not rear:
                height = front
                depth = 2
                return floor, height, depth, mount
            elif rear and not interior and not front:
                height = rear
                depth = 2
                return floor, height, depth, mount
            else:
                return None, None, None, None
        else:
            return None, None, None, None

    def get_device_types(self):
        if not self.hardware:
            self.hardware = self.get_hardware()
        rt_hardware = self.hardware
        rt_device_types = {}

        for device_id, device in rt_hardware.items():
            print(device)
            if device["name"] == device["manufacturer"]:
                key = device["name"]
            else:
                key = "{}_{}"
            if not key in rt_device_types.keys():
                device_type = copy.deepcopy(device)
                if "description" in device_type.keys():
                    del device_type["description"]
                    del device_type["rt_dev_id"]
                rt_device_types[key] = device_type
        pp.pprint(rt_device_types)

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
            # rest.post_hardware(hwddata)

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
            # rest.post_device(dev)

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
            # rest.post_device(dev)

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
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            # get object IDs
            q = "SELECT id FROM Object"
            cur.execute(q)
            idsx = cur.fetchall()
        ids = [x[0] for x in idsx]

        with self.con:
            for dev_id in ids:
                q = (
                    """Select
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
                            Location.parent_name

                            FROM Object
                            LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
                            LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
                            LEFT JOIN RackSpace ON Object.id = RackSpace.object_id
                            LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
                            LEFT JOIN Rack ON RackSpace.rack_id = Rack.id
                            LEFT JOIN Location ON Rack.location_id = Location.id
                            WHERE Object.id = %s
                            AND Object.objtype_id not in (2,9,1505,1560,1561,1562,50275) """
                    + config["Misc"]["device_data_filter"] % dev_id
                )

                cur.execute(q)
                data = cur.fetchall()
                if data:  # RT objects that do not have data are locations, racks, rows etc...
                    self.process_data(data, dev_id)
        cur.close()
        self.con = None

    def process_data(self, data, dev_id):
        devicedata = {}
        device2rack = {}
        name = None
        opsys = None
        hardware = None
        note = None
        rrack_id = None
        floor = None
        dev_type = 0

        for x in data:
            (
                dev_type,
                rdesc,
                rname,
                rasset,
                rattr_name,
                rtype,
                rcomment,
                rrack_id,
                rrack_name,
                rrow_name,
                rlocation_id,
                rlocation_name,
                rparent_name,
            ) = x

            name = x[1]
            note = x[-7]

            if "Operating System" in x:
                opsys = x[-8]
                if "%GSKIP%" in opsys:
                    opsys = opsys.replace("%GSKIP%", " ")
                if "%GPASS%" in opsys:
                    opsys = opsys.replace("%GPASS%", " ")
            if "SW type" in x:
                opsys = x[-8]
                if "%GSKIP%" in opsys:
                    opsys = opsys.replace("%GSKIP%", " ")
                if "%GPASS%" in opsys:
                    opsys = opsys.replace("%GPASS%", " ")

            if "Server Hardware" in x:
                hardware = x[-8]
                if "%GSKIP%" in hardware:
                    hardware = hardware.replace("%GSKIP%", " ")
                if "%GPASS%" in hardware:
                    hardware = hardware.replace("%GPASS%", " ")
                if "\t" in hardware:
                    hardware = hardware.replace("\t", " ")

            if "HW type" in x:
                hardware = x[-8]
                if "%GSKIP%" in hardware:
                    hardware = hardware.replace("%GSKIP%", " ")
                if "%GPASS%" in hardware:
                    hardware = hardware.replace("%GPASS%", " ")
                if "\t" in hardware:
                    hardware = hardware.replace("\t", " ")
            if note:
                note = note.replace("\n", " ")
                if "&lt;" in note:
                    note = note.replace("&lt;", "")
                if "&gt;" in note:
                    note = note.replace("&gt;", "")

        if name:
            # set device data
            devicedata.update({"name": name})
            if hardware:
                devicedata.update({"hardware": hardware[:48]})
            if opsys:
                devicedata.update({"os": opsys})
            if note:
                devicedata.update({"notes": note})
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
                    pass
            elif dev_type == 1504:
                devicedata.update({"type": "virtual"})
                devicedata.pop("hardware", None)
                try:
                    vm_host_id = self.container_map[dev_id]
                    vm_host_name = self.vm_hosts[vm_host_id]
                    devicedata.update({"virtual_host": vm_host_name})
                except KeyError:
                    pass

            d42_rack_id = None
            # except VMs
            if dev_type != 1504:
                if rrack_id:
                    d42_rack_id = self.rack_id_map[rrack_id]

                # if the device is mounted in RT, we will try to add it to D42 hardwares.
                floor, height, depth, mount = self.get_hardware_size(dev_id)
                if floor is not None:
                    floor = int(floor) + 1
                else:
                    floor = "auto"
                if not hardware:
                    hardware = "generic" + str(height) + "U"
                self.add_hardware(height, depth, hardware)

            # upload device
            if devicedata:
                if hardware and dev_type != 1504:
                    devicedata.update({"hardware": hardware[:48]})

                # set default type for racked devices
                if "type" not in devicedata and d42_rack_id and floor:
                    devicedata.update({"type": "physical"})

                # rest.post_device(devicedata)
                print(devicedata)
                exit(1)

                # update ports
                if dev_type == 8 or dev_type == 4 or dev_type == 445 or dev_type == 1055:
                    ports = self.get_ports_by_device(self.all_ports, dev_id)
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

                                rest.post_switchport(switchport_data)

                                # reverse connection
                                device_name = self.get_device_by_port(get_links[0])
                                switchport_data = {
                                    "port": self.get_port_by_id(self.all_ports, get_links[0]),
                                    "switch": device_name,
                                }

                                switchport_data.update({"device": name})
                                switchport_data.update({"remote_device": name})
                                switchport_data.update({"remote_port": item[0]})

                                rest.post_switchport(switchport_data)
                            else:
                                rest.post_switchport(switchport_data)

                # if there is a device, we can try to mount it to the rack
                if dev_type != 1504 and d42_rack_id and floor:  # rack_id is D42 rack id
                    device2rack.update({"device": name})
                    if hardware:
                        device2rack.update({"hw_model": hardware[:48]})
                    device2rack.update({"rack_id": d42_rack_id})
                    device2rack.update({"start_at": floor})

                    rest.post_device2rack(device2rack)
                else:
                    if dev_type != 1504 and d42_rack_id is not None:
                        msg = (
                            '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount device "%s" (RT id = %d) to the rack.\
                        \n\tFloor returned from "get_hardware_size" function was: %s'
                            % (name, dev_id, str(floor))
                        )
                        logger.info(msg)
            else:
                msg = (
                    "\n-----------------------------------------------------------------------\
                \n[!] INFO: Device %s (RT id = %d) cannot be uploaded. Data was: %s"
                    % (name, dev_id, str(devicedata))
                )
                logger.info(msg)

        else:
            # device has no name thus it cannot be migrated
            msg = (
                "\n-----------------------------------------------------------------------\
            \n[!] INFO: Device with RT id=%d cannot be migrated because it has no name."
                % dev_id
            )
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
            rest.post_ip(devmap)

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
                rest.post_pdu_model(pdumodel)
                pdumodels.append(pdumodel)
            elif pdu_type and rack_id:
                if pdu_id not in pdu_rack_models:
                    rest.post_pdu_model(pdumodel)
                    pdu_rack_models.append(pdu_id)

            # post pdus
            if pdu_id not in pdumap:
                response = rest.post_pdu(pdudata)
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
                            rest.post_pdu_to_rack(rdata, d42_rack_id)
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
                        rest.post_pdu_to_rack(rdata, d42_rack_id)
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
                            rest.post_patch_panel_module_models(
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

            rest.post_patch_panel(payload)

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

        if data:
            return data[0]
        else:
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
    pp = pprint.PrettyPrinter(indent=4)

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

    netbox = pynetbox.api(config["NetBox"]["NETBOX_HOST"], token=config["NetBox"]["NETBOX_TOKEN"])

    tenant_groups = netbox.tenancy.tenant_groups.all()

    print()

    rest = REST()
    racktables = DB()
    if config["Migrate"]["TAGS"] == "True":
        print("running get tags")
        racktables.get_tags()
    if config["Migrate"]["INFRA"] == "True":
        print("running get infra")
        racktables.get_infrastructure()
    if config["Migrate"]["VLAN"] == "True":
        racktables.get_vlan_domains()
    if config["Migrate"]["SUBNETS"] == "True":
        print("running get subnets")
        racktables.get_subnets()
        racktables.get_subnets_v6()
    if config["Migrate"]["IPS"] == "True":
        print("running get ips")
        racktables.get_ips()
        racktables.get_ips_v6()
    if config["Migrate"]["HARDWARE"] == "True":
        print("running device types")
        # racktables.get_hardware()
        racktables.get_device_types()
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
