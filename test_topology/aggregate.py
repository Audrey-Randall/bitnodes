import json
import os
import gzip
from typing import List
from collections import defaultdict
from ordered_set import OrderedSet


def serialize_sets(obj):
    if isinstance(obj, OrderedSet):
        return list(obj)
    return obj


def aggregate_timestamps(files: List[str]):
    aggregation = defaultdict(lambda: defaultdict(OrderedSet))
    for filename in files:
        # with gzip.GzipFile(filename, 'r') as f:
        #     data = json.loads(f.read().decode('utf8'))
        with open(filename, 'r') as f:
            data = json.load(f)
        for ip, bucket_entries in data.items():
            for entry in bucket_entries:
                entry_ip = entry[0]
                timestamp = int(entry[1])
                aggregation[ip][entry_ip].add(timestamp)
        print(f"{filename} finished processing : remaining : {len(files)}")
    return aggregation


def list_permanent_nodes(files: List[str]):
    permanent_nodes = set()
    with gzip.GzipFile(files[0], 'r') as f:
        data = json.loads(f.read().decode('utf8'))
        print(len(data))
        for ip in data.keys():
            permanent_nodes.add(ip)

    for filename in files[1:]:
        with gzip.GzipFile(filename, 'r') as f:
            data = json.loads(f.read().decode('utf8'))
        permanent_nodes = [perm_node for perm_node in permanent_nodes if perm_node in data.keys()]
        print(f"{filename} finished processing : remaining : {len(permanent_nodes)}")
    return permanent_nodes


def number_new_nodes_in_buckets(files: List[str], perm_nodes: set):
    buckets_content = defaultdict(set)
    number_new_nodes = defaultdict(list)
    for filename in files:
        # with gzip.GzipFile(filename, 'r') as f:
            # data = json.loads(f.read().decode('utf8'))
        with open(filename, 'r') as f:
            data = json.load(f)
        for ip, bucket_entries in data.items():
            if ip in perm_nodes:
                number_nodes = 0
                for ip_port_services, _ in bucket_entries:
                    if ip_port_services not in buckets_content[ip]:
                        buckets_content[ip].add(ip_port_services)
                        number_nodes += 1
                number_new_nodes[ip].append(number_nodes)
        print(f"{filename} finished processing")
    return number_new_nodes


if __name__ == "__main__":
    # filenames = [os.path.join(path, f) for f in sorted(os.listdir(path)) if f.endswith('.gz')]
    filenames = [os.path.join(path, f) for f in sorted(os.listdir(path)) if f.endswith('.json')]
    aggregation = aggregate_timestamps(filenames[:60])
    with open('result.json', "w") as f:
        json.dump(aggregation, f, default=serialize_sets)

    # permanent_nodes_filename = os.path.join(path, "permanent_nodes.json")
    # permanent_nodes = list_permanent_nodes(filenames)
    # print(len(permanent_nodes))
    # with open(os.path.join(path, 'permanent_nodes.json'), 'w') as f:
    #     json.dump(permanent_nodes, f)

    # with open(permanent_nodes_filename, 'r') as f:
    #     permanent_nodes = json.load(f)
    # permanent_nodes = set(permanent_nodes)
    # number_new_nodes = number_new_nodes_in_buckets(filenames, permanent_nodes)
    # with open("number_new_nodes.json", 'w') as f:
    #     json.dump(number_new_nodes, f)

    # aggregate ../data/crawl/20200920
