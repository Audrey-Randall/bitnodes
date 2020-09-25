import socket
import sys
import time
from datetime import datetime, timezone
from collections import defaultdict
import os
import json
from binascii import unhexlify

from protocol import (
    Connection,
    ConnectionError,
    ProtocolError,
)


def enumerate_node(parent, addr_msgs, now):
    """
    Adds all peering nodes with age <= max. age into the crawl set.
    """
    peers_number = 0
    peers = set()
    excluded = 0

    if len(addr_msgs) == 0:
        return (None, -1, excluded)

    print(len(addr_msgs))
    nNodes = 0
    nNodes_old = 0

    for addr_msg in addr_msgs:
        if 'addr_list' in addr_msg:
            nNodes += len(addr_msg['addr_list'])
            print(len(addr_msg['addr_list']))
            for peer in addr_msg['addr_list']:
                age = now - peer['timestamp']  # seconds

                if age >= 0 and age <= 999999999:
                    address = peer['ipv4'] or peer['ipv6'] or peer['onion']
                    port = peer['port'] if peer['port'] > 0 else 18333
                    services = peer['services']
                    timestamp = peer['timestamp']
                    if not address:
                        continue
                    peers.add((f'{address}-{port}-{services}',f'{timestamp}'))
                    peers_number += 1
                    if peers_number >= 15000:
                        return (peers, peers_number, excluded)
                else:
                    nNodes_old += 1
    print(nNodes)
    print(nNodes_old)
    print(peers_number)
    return (peers, peers_number, excluded)


def task(key, dir="./test_topology/"):
    print(key)
    addr_msgs = []
    (address, port, services) = key[5:].split("-", 2)
    services = int(services)
    mainnet = unhexlify("f9beb4d9")
    testnet = unhexlify("0b110907")
    conn = Connection((address, int(port)),
                      ("0.0.0.0", 0),
                      magic_number=testnet,
                      socket_timeout=150,
                      proxy=None,
                      protocol_version=70015,
                      to_services=services,
                      from_services=0,
                      user_agent="skuld_localhost",
                      height=0,
                      relay=0)
    try:
        print("Connecting to %s", conn.to_addr)
        conn.open()
        handshake_msgs = conn.handshake()
    except (ProtocolError, ConnectionError, socket.error) as err:
        print("%s: %s", conn.to_addr, err)
    if len(handshake_msgs) > 0:
        try:
            conn.getaddr(block=False)
        except (ProtocolError, ConnectionError, socket.error) as err:
            print("%s: %s", conn.to_addr, err)
        else:
            addr_wait = 0
            while addr_wait < 15:
                addr_wait += 1
                print("addr wait + 1")
                time.sleep(0.3)
                try:
                    msgs = conn.get_messages(commands=['addr'])
                except (ProtocolError, ConnectionError, socket.error) as err:
                    print(f"{conn.to_addr} : {err}")
                    break
                if msgs and any([msg['count'] > 1 for msg in msgs]):
                    addr_msgs = msgs
                    break
        now = int(time.time())
        (peers, peers_number, excluded) = enumerate_node(address, addr_msgs, now)
        date = datetime.utcfromtimestamp(int(time.time())).replace(tzinfo=timezone.utc).\
            astimezone(tz=None).strftime('%Y%m%d-%H:%M:%S')
        print(f"Peers number : {peers_number}")
        output = os.path.join(dir, f'nodes_per_getADDR_{date}.json')
        nodes_per_getaddr = defaultdict(list)
        nodes_per_getaddr[address].extend(peers)
        with open(output, 'w') as f:
            json.dump(nodes_per_getaddr, f)
        print(f"Wrote {output}")
    conn.close()


def main():
    key = "node:127.0.0.1-18333-1"
    # key_walle = "node:152.81.8.87-18333-1"
    # key = "node:5.74.14.60-8333-1"

    try:
        while True:
            # task(key)
            task(key, "./test_topology_walle/")
    except KeyboardInterrupt:
        print("CTRL-C pressed")
        pass

    return 0


if __name__ == "__main__":
    main()
