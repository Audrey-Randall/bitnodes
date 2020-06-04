#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# crawl.py - Greenlets-based Bitcoin network crawler.
#
# Copyright (c) Addy Yeow Chin Heng <ayeowch@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Greenlets-based Bitcoin network crawler.
"""

from gevent import monkey
monkey.patch_all()

import geoip2.database
import gevent
import json
import csv
import logging
import os
import redis
import redis.connection
import requests
import socket
import sys
import time
import statistics
import pickle
import bz2
from base64 import b32decode
from binascii import hexlify, unhexlify
from collections import Counter
from configparser import ConfigParser
from geoip2.errors import AddressNotFoundError
from ipaddress import ip_address, ip_network
from collections import defaultdict
from datetime import datetime, timezone


from protocol import (
    ONION_PREFIX,
    TO_SERVICES,
    Connection,
    ConnectionError,
    ProtocolError,
)
from utils import new_redis_conn, get_keys, ip_to_network
from repeated_timer import RepeatedTimer

redis.connection.socket = gevent.socket

REDIS_CONN = None
REDIS_CONN_NO_DECODE = None
CONF = {}
UP_SIZE = 0
UP_NODES_PER_SEC = []
UP_NODES_INDEX = 0

# MaxMind databases
ASN = geoip2.database.Reader("geoip/GeoLite2-ASN.mmdb")


def up_diff():
    global UP_SIZE
    global UP_NODES_INDEX
    up_card = REDIS_CONN.scard('up')
    up = up_card - UP_SIZE
    # logging.info(f"{up} new reachable nodes added")
    UP_NODES_PER_SEC.append([UP_NODES_INDEX, up])
    UP_NODES_INDEX += 5
    UP_SIZE = up_card


RT = RepeatedTimer(5, up_diff)


def enumerate_node(redis_pipe, addr_msgs, now):
    """
    Adds all peering nodes with age <= max. age into the crawl set.
    """
    peers_number = 0
    peers = set()
    excluded = 0

    if len(addr_msgs) == 0:
        return (None, -1, excluded)

    for addr_msg in addr_msgs:
        if 'addr_list' in addr_msg:
            for peer in addr_msg['addr_list']:
                age = now - peer['timestamp']  # seconds

                if age >= 0 and age <= CONF['max_age']:
                    address = peer['ipv4'] or peer['ipv6'] or peer['onion']
                    port = peer['port'] if peer['port'] > 0 else CONF['port']
                    services = peer['services']
                    if not address:
                        continue
                    if is_excluded(address):
                        logging.debug("Exclude: (%s, %d)", address, port)
                        excluded += 1
                        continue
                    redis_pipe.sadd('pending', (address, port, services))
                    redis_pipe.sadd('all_nodes_bis', f'node:{address}-{port}-{services}')
                    peers.add(f'{address}-{port}-{services}')
                    peers_number += 1
                    if peers_number >= CONF['peers_per_node']:
                        return (peers, peers_number, excluded)
    return (peers, peers_number, excluded)


def connect(redis_conn, key):
    """
    Establishes connection with a node to:
    1) Send version message
    2) Receive version and verack message
    3) Send getaddr message
    4) Receive addr message containing list of peering nodes
    Stores state and height for node in Redis.
    """
    handshake_msgs = []
    addr_msgs = []

    redis_conn.set(key, "")  # Set Redis key for a new node
    redis_conn.sadd('all_nodes', key)
    (address, port, services) = key[5:].split("-", 2)
    services = int(services)
    height = redis_conn.get('height')
    if height:
        height = int(height)

    proxy = None
    if address.endswith(".onion"):
        proxy = CONF['tor_proxy']

    conn = Connection((address, int(port)),
                      (CONF['source_address'], 0),
                      magic_number=CONF['magic_number'],
                      socket_timeout=CONF['socket_timeout'],
                      proxy=proxy,
                      protocol_version=CONF['protocol_version'],
                      to_services=services,
                      from_services=CONF['services'],
                      user_agent=CONF['user_agent'],
                      height=height,
                      relay=CONF['relay'])
    try:
        logging.debug("Connecting to %s", conn.to_addr)
        conn.open()
        handshake_msgs = conn.handshake()
    except (ProtocolError, ConnectionError, socket.error) as err:
        logging.debug("%s: %s", conn.to_addr, err)

    redis_pipe = redis_conn.pipeline()
    if len(handshake_msgs) > 0:
        try:
            conn.getaddr(block=False)
        except (ProtocolError, ConnectionError, socket.error) as err:
            logging.debug("%s: %s", conn.to_addr, err)
        else:
            addr_wait = 0
            while addr_wait < CONF['socket_timeout']:
                addr_wait += 1
                gevent.sleep(0.3)
                try:
                    msgs = conn.get_messages(commands=['addr'])
                except (ProtocolError, ConnectionError, socket.error) as err:
                    logging.debug("%s: %s", conn.to_addr, err)
                    break
                if msgs and any([msg['count'] > 1 for msg in msgs]):
                    addr_msgs = msgs
                    break

        version_msg = handshake_msgs[0]
        from_services = version_msg.get('services', 0)
        if from_services != services:
            logging.debug("%s Expected %d, got %d for services", conn.to_addr,
                          services, from_services)
            key = "node:{}-{}-{}".format(address, port, from_services)
        height_key = "height:{}-{}-{}".format(address, port, from_services)
        redis_pipe.setex(height_key, CONF['max_age'],
                         version_msg.get('height', 0))
        now = int(time.time())
        (peers, peers_number, excluded) = enumerate_node(redis_pipe, addr_msgs, now)
        REDIS_CONN_NO_DECODE.rpush('nodes_per_getaddr', pickle.dumps((key[5:], peers)))
        REDIS_CONN_NO_DECODE.rpush('nodes_per_getaddr_number', pickle.dumps((key[5:], peers_number)))
        logging.debug("%s Peers: %d (Excluded: %d)",
                      conn.to_addr, peers_number,
                      excluded)
        redis_pipe.set(key, "")
        redis_pipe.sadd('up', key)
    conn.close()
    redis_pipe.execute()


def dump(date, nodes):
    """
    Dumps data for reachable nodes into timestamp-prefixed JSON file and
    returns most common height from the nodes.
    """
    json_data = []

    logging.info('Building JSON data')
    for node in nodes:
        (address, port, services) = node[5:].split("-", 2)
        height_key = "height:{}-{}-{}".format(address, port, services)
        try:
            height = int(REDIS_CONN.get(height_key))
        except TypeError:
            logging.warning("%s missing", height_key)
            height = 0
        json_data.append([address, int(port), int(services), height])
    logging.info('Built JSON data: %d', len(json_data))

    if len(json_data) == 0:
        logging.warning("len(json_data): %d", len(json_data))
        return 0

    json_output = os.path.join(CONF['crawl_dir'], f"{date}.json")
    open(json_output, 'w').write(json.dumps(json_data))
    logging.info("Wrote %s", json_output)

    return Counter([node[-1] for node in json_data]).most_common(1)[0][0]


def dump_nodes_per_getaddr_number(nodes, date):
    """
    Dumps the number of nodes potential nodes retrieved from the GETADDR
    messages
    """
    output = os.path.join(CONF['crawl_dir'], f'nodes_per_getADDR_{date}.csv')
    nodes_per_getaddr = defaultdict(list)
    for nodes_addr_number in nodes:
        node, addr_number = pickle.loads(nodes_addr_number)
        nodes_per_getaddr[node].append(addr_number)
    with open(output, "w", newline='') as f:
        writer = csv.writer(f)
        for i, (k, v) in enumerate(nodes_per_getaddr.items()):
            writer.writerow([i+1, k, max(v), round(statistics.mean(v)), *v])
    logging.info(f"Wrote {output}")


def dump_nodes_per_getaddr(nodes, date):
    """
    Dumps the nodes retrieved from the GETADDR messages from all the crawled
    nodes
    """
    output = os.path.join(CONF['crawl_dir'], f'nodes_per_getADDR_{date}.pickle.bz2')
    nodes_per_getaddr = defaultdict(set)
    for nodes_addr_number in nodes:
        parent_node, nodes_set = pickle.loads(nodes_addr_number)
        if nodes_set is not None:
            nodes_per_getaddr[parent_node].update(nodes_set)
    with bz2.open(output, "wb") as f:
        pickle.dump(nodes_per_getaddr, f, pickle.HIGHEST_PROTOCOL)
    logging.info(f"Wrote {output}")


def dump_upnodes_per_second(date):
    """
    Dumps the number of up nodes crawled per second
    """
    global UP_NODES_PER_SEC
    global UP_SIZE
    global UP_NODES_INDEX
    logging.info('Building up nodes per second data')
    output = os.path.join(CONF['crawl_dir'], f"up_nodes_per_seconds_{date}.csv")
    with open(output, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(UP_NODES_PER_SEC)
    logging.info(f"Wrote {output}")
    UP_SIZE = 0
    UP_NODES_PER_SEC = []
    UP_NODES_INDEX = 0


def restart(timestamp):
    """
    Dumps data for the reachable nodes into a JSON file.
    Loads all reachable nodes from Redis into the crawl set.
    Removes keys for all nodes from current crawl.
    Updates excluded networks with current list of bogons.
    Updates number of reachable nodes and most common height in Redis.
    """
    redis_pipe = REDIS_CONN.pipeline()

    nodes = REDIS_CONN.smembers('up')  # Reachable nodes
    all_nodes = REDIS_CONN.scard('all_nodes')
    all_nodes_bis = REDIS_CONN.scard('all_nodes_bis')
    nodes_per_getaddr = REDIS_CONN_NO_DECODE.lrange('nodes_per_getaddr', 0, -1)
    nodes_per_getaddr_number = REDIS_CONN_NO_DECODE.lrange('nodes_per_getaddr_number', 0, -1)
    redis_pipe.delete('up')
    redis_pipe.delete('all_nodes')
    redis_pipe.delete('all_nodes_bis')
    redis_pipe.delete('nodes_per_getaddr')
    redis_pipe.delete('nodes_per_getaddr_number')

    for node in nodes:
        (address, port, services) = node[5:].split("-", 2)
        redis_pipe.sadd('pending', (address, int(port), int(services)))

    for key in get_keys(REDIS_CONN, 'node:*'):
        redis_pipe.delete(key)

    for key in get_keys(REDIS_CONN, 'crawl:cidr:*'):
        redis_pipe.delete(key)

    if CONF['include_checked']:
        checked_nodes = REDIS_CONN.zrangebyscore(
            'check', timestamp - CONF['max_age'], timestamp)
        for node in checked_nodes:
            (address, port, services) = eval(node)
            if is_excluded(address):
                logging.debug("Exclude: %s", address)
                continue
            redis_pipe.sadd('pending', (address, port, services))

    redis_pipe.execute()

    update_excluded_networks()

    reachable_nodes = len(nodes)
    logging.info("Reachable nodes: %d", reachable_nodes)
    logging.info(f"All nodes (not only reachable): {all_nodes}")
    logging.info(f"All nodes with IPv6 (not only reachable): {all_nodes_bis}")
    REDIS_CONN.lpush('nodes', (timestamp, reachable_nodes))

    RT.stop()
    date = datetime.utcfromtimestamp(timestamp).replace(tzinfo=timezone.utc).\
        astimezone(tz=None).strftime('%Y%m%d-%H:%M:%S')
    dump_upnodes_per_second(date)
    dump_nodes_per_getaddr_number(nodes_per_getaddr_number, date)
    dump_nodes_per_getaddr(nodes_per_getaddr, date)
    height = dump(date, nodes)
    REDIS_CONN.set('height', height)
    logging.info("Height: %d", height)


def cron():
    """
    Assigned to a worker to perform the following tasks periodically to
    maintain a continuous crawl:
    1) Reports the current number of nodes in crawl set
    2) Initiates a new crawl once the crawl set is empty
    """
    RT.start()
    start = int(time.time())

    while True:
        pending_nodes = REDIS_CONN.scard('pending')
        logging.info("Pending: %d", pending_nodes)

        if pending_nodes == 0:
            REDIS_CONN.set('crawl:master:state', "starting")
            gevent.sleep(30)  # gives workers some time to finish their jobs
            now = int(time.time())
            elapsed = now - start
            REDIS_CONN.set('elapsed', elapsed)
            logging.info("Elapsed: %d", elapsed)
            logging.info("Restarting")
            restart(now)
            while int(time.time()) - start < CONF['snapshot_delay']:
                gevent.sleep(1)
            RT.start()
            start = int(time.time())
            REDIS_CONN.set('crawl:master:state', "running")

        gevent.sleep(CONF['cron_delay'])


def task():
    """
    Assigned to a worker to retrieve (pop) a node from the crawl set and
    attempt to establish connection with a new node.
    """
    redis_conn = new_redis_conn(db=CONF['db'])

    while True:
        while REDIS_CONN.get('crawl:master:state') != "running":
            gevent.sleep(CONF['socket_timeout'])

        node = redis_conn.spop('pending')  # Pop random node from set
        if node is None:
            gevent.sleep(1)
            continue

        node = eval(node)  # Convert string from Redis to tuple

        # Skip IPv6 node
        if ":" in node[0] and not CONF['ipv6']:
            continue

        key = "node:{}-{}-{}".format(node[0], node[1], node[2])
        if REDIS_CONN.exists(key):
            if CONF['keep_duplication']:
                node_with_parent = "node:{}-{}-{}-{}".format(node[0], node[1], node[2],
                                                             node[3])
                REDIS_CONN.sadd('up', node_with_parent)
            continue

        # Check if prefix has hit its limit
        if ":" in node[0] and CONF['ipv6_prefix'] < 128:
            cidr = ip_to_network(node[0], CONF['ipv6_prefix'])
            nodes = redis_conn.incr('crawl:cidr:{}'.format(cidr))
            if nodes > CONF['nodes_per_ipv6_prefix']:
                logging.debug("CIDR %s: %d", cidr, nodes)
                continue

        connect(redis_conn, key)


def set_pending():
    """
    Initializes pending set in Redis with a list of reachable nodes from DNS
    seeders and hardcoded list of .onion nodes to bootstrap the crawler.
    """
    for seeder in CONF['seeders']:
        nodes = []

        try:
            ipv4_nodes = socket.getaddrinfo(seeder, None, socket.AF_INET)
        except socket.gaierror as err:
            logging.warning("%s", err)
        else:
            nodes.extend(ipv4_nodes)

        if CONF['ipv6']:
            try:
                ipv6_nodes = socket.getaddrinfo(seeder, None, socket.AF_INET6)
            except socket.gaierror as err:
                logging.warning("%s", err)
            else:
                nodes.extend(ipv6_nodes)

        for node in nodes:
            address = node[-1][0]
            if is_excluded(address):
                logging.debug("Exclude: %s", address)
                continue
            logging.debug("%s: %s", seeder, address)
            REDIS_CONN.sadd('pending', (address, CONF['port'], TO_SERVICES))

    if CONF['onion']:
        for address in CONF['onion_nodes']:
            REDIS_CONN.sadd('pending', (address, CONF['port'], TO_SERVICES))


def is_excluded(address):
    """
    Returns True if address is found in exclusion list, False if otherwise.
    """
    if address.endswith(".onion"):
        address = onion_to_ipv6(address)
    elif ip_address(str(address)).is_private:
        return True

    if ":" in address:
        address_family = socket.AF_INET6
        key = 'exclude_ipv6_networks'
    else:
        address_family = socket.AF_INET
        key = 'exclude_ipv4_networks'

    try:
        asn_record = ASN.asn(address)
    except AddressNotFoundError:
        asn = None
    else:
        asn = 'AS{}'.format(asn_record.autonomous_system_number)

    try:
        addr = int(hexlify(socket.inet_pton(address_family, address)), 16)
    except socket.error:
        logging.warning("Bad address: %s", address)
        return True

    if any([(addr & net[1] == net[0]) for net in CONF[key]]):
        return True

    if asn and asn in CONF['exclude_asns']:
        return True

    return False


def onion_to_ipv6(address):
    """
    Returns IPv6 equivalent of an .onion address.
    """
    ipv6_bytes = ONION_PREFIX + b32decode(address[:-6], True)
    return socket.inet_ntop(socket.AF_INET6, ipv6_bytes)


def list_excluded_networks(txt, networks=None):
    """
    Converts list of networks from configuration file into a list of tuples of
    network address and netmask to be excluded from the crawl.
    """
    if networks is None:
        networks = set()
    lines = txt.strip().split("\n")
    for line in lines:
        line = line.split('#')[0].strip()
        try:
            network = ip_network(str(line))
        except ValueError:
            continue
        else:
            networks.add((int(network.network_address), int(network.netmask)))
    return networks


def update_excluded_networks():
    """
    Adds bogons into the excluded IPv4 and IPv6 networks.
    """
    if CONF['exclude_ipv4_bogons']:
        urls = [
            "http://www.team-cymru.org/Services/Bogons/fullbogons-ipv4.txt",
        ]
        for url in urls:
            try:
                response = requests.get(url, timeout=15)
            except requests.exceptions.RequestException as err:
                logging.warning(err)
            else:
                if response.status_code == 200:
                    CONF['exclude_ipv4_networks'] = list_excluded_networks(
                        response.text,
                        networks=CONF['exclude_ipv4_networks'])
                    logging.info("IPv4: %d",
                                 len(CONF['exclude_ipv4_networks']))

    if CONF['exclude_ipv6_bogons']:
        urls = [
            "http://www.team-cymru.org/Services/Bogons/fullbogons-ipv6.txt",
        ]
        for url in urls:
            try:
                response = requests.get(url, timeout=15)
            except requests.exceptions.RequestException as err:
                logging.warning(err)
            else:
                if response.status_code == 200:
                    CONF['exclude_ipv6_networks'] = list_excluded_networks(
                        response.text,
                        networks=CONF['exclude_ipv6_networks'])
                    logging.info("IPv6: %d",
                                 len(CONF['exclude_ipv6_networks']))


def init_conf(argv):
    """
    Populates CONF with key-value pairs from configuration file.
    """
    conf = ConfigParser()
    conf.read(argv[1])
    CONF['logfile'] = conf.get('crawl', 'logfile')
    CONF['magic_number'] = unhexlify(conf.get('crawl', 'magic_number'))
    CONF['port'] = conf.getint('crawl', 'port')
    CONF['db'] = conf.getint('crawl', 'db')
    CONF['seeders'] = conf.get('crawl', 'seeders').strip().split("\n")
    CONF['workers'] = conf.getint('crawl', 'workers')
    CONF['debug'] = conf.getboolean('crawl', 'debug')
    CONF['source_address'] = conf.get('crawl', 'source_address')
    CONF['protocol_version'] = conf.getint('crawl', 'protocol_version')
    CONF['user_agent'] = conf.get('crawl', 'user_agent')
    CONF['services'] = conf.getint('crawl', 'services')
    CONF['relay'] = conf.getint('crawl', 'relay')
    CONF['socket_timeout'] = conf.getint('crawl', 'socket_timeout')
    CONF['cron_delay'] = conf.getint('crawl', 'cron_delay')
    CONF['snapshot_delay'] = conf.getint('crawl', 'snapshot_delay')
    CONF['max_age'] = conf.getint('crawl', 'max_age')
    CONF['peers_per_node'] = conf.getint('crawl', 'peers_per_node')
    CONF['ipv6'] = conf.getboolean('crawl', 'ipv6')
    CONF['ipv6_prefix'] = conf.getint('crawl', 'ipv6_prefix')
    CONF['nodes_per_ipv6_prefix'] = conf.getint('crawl',
                                                'nodes_per_ipv6_prefix')

    CONF['exclude_asns'] = conf.get('crawl',
                                    'exclude_asns').strip().split("\n")

    CONF['exclude_ipv4_networks'] = list_excluded_networks(
        conf.get('crawl', 'exclude_ipv4_networks'))
    CONF['exclude_ipv6_networks'] = list_excluded_networks(
        conf.get('crawl', 'exclude_ipv6_networks'))

    CONF['exclude_ipv4_bogons'] = conf.getboolean('crawl',
                                                  'exclude_ipv4_bogons')
    CONF['exclude_ipv6_bogons'] = conf.getboolean('crawl',
                                                  'exclude_ipv6_bogons')

    CONF['onion'] = conf.getboolean('crawl', 'onion')
    CONF['tor_proxy'] = None
    if CONF['onion']:
        tor_proxy = conf.get('crawl', 'tor_proxy').split(":")
        CONF['tor_proxy'] = (tor_proxy[0], int(tor_proxy[1]))
    CONF['onion_nodes'] = conf.get('crawl', 'onion_nodes').strip().split("\n")

    CONF['include_checked'] = conf.getboolean('crawl', 'include_checked')

    CONF['crawl_dir'] = conf.get('crawl', 'crawl_dir')
    if not os.path.exists(CONF['crawl_dir']):
        os.makedirs(CONF['crawl_dir'])

    # Set to True for master process
    CONF['master'] = argv[2] == "master"
    CONF['keep_duplication'] = conf.getboolean('crawl', 'keep_duplication')


def main(argv):
    if len(argv) < 3 or not os.path.exists(argv[1]):
        print("Usage: crawl.py [config] [master|slave]")
        return 1

    # Initialize global conf
    init_conf(argv)

    # Initialize logger
    loglevel = logging.INFO
    if CONF['debug']:
        loglevel = logging.DEBUG

    logformat = ("[%(process)d] %(asctime)s,%(msecs)05.1f %(levelname)s "
                 "(%(funcName)s) %(message)s")
    logging.basicConfig(level=loglevel,
                        format=logformat,
                        filename=CONF['logfile'],
                        filemode='a')
    print(("Log: {}, press CTRL+C to terminate..".format(CONF['logfile'])))

    global REDIS_CONN
    REDIS_CONN = new_redis_conn(db=CONF['db'])
    global REDIS_CONN_NO_DECODE
    REDIS_CONN_NO_DECODE = new_redis_conn(db=CONF['db'], decode=False)

    if CONF['master']:
        REDIS_CONN.set('crawl:master:state', "starting")
        logging.info("Removing all keys")
        redis_pipe = REDIS_CONN.pipeline()
        redis_pipe.delete('up')
        redis_pipe.delete('all_nodes')
        redis_pipe.delete('all_nodes_bis')
        redis_pipe.delete('nodes_per_getaddr')
        redis_pipe.delete('nodes_per_getaddr_number')
        for key in get_keys(REDIS_CONN, 'node:*'):
            redis_pipe.delete(key)
        for key in get_keys(REDIS_CONN, 'crawl:cidr:*'):
            redis_pipe.delete(key)
        redis_pipe.delete('pending')
        redis_pipe.execute()
        set_pending()
        update_excluded_networks()
        REDIS_CONN.set('crawl:master:state', "running")

    # Spawn workers (greenlets) including one worker reserved for cron tasks
    workers = []
    if CONF['master']:
        workers.append(gevent.spawn(cron))
    else:
        for _ in range(CONF['workers'] - len(workers)):
            workers.append(gevent.spawn(task))
    logging.info("Workers: %d", len(workers))
    gevent.joinall(workers)

    return 0


def sigterm_handler(signal, frame):
    # save the state here or do whatever you want
    RT.stop()
    print('Exit')
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
