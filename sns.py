import seaborn as sns
import pandas as pd
import sys
import os
import json
import csv
import matplotlib.pyplot as plt
import random
from collections import Counter, OrderedDict
from enum import Enum


class ChurnPeriod(Enum):
    THIRTYMIN = 7
    ONEHOUR = 15
    TWOHOUR = 30
    FOURHOUR = 60
    EIGHTHOUR = 120
    ONEDAY = 500


def distinct_ip(json_dir: str):
    files = os.listdir(json_dir)
    all_nodes_set = set()
    for filename in files:
        with open(os.path.join(json_dir, filename), 'r') as f:
            nodes = json.load(f)
        for node_info in nodes:
            all_nodes_set.add(f'{node_info[0]}-{node_info[1]}-{node_info[5]}')
    print(f"Distinct IP in all the data files from the dir : {len(all_nodes_set)}")


def churn(json_dir: str, period: ChurnPeriod):
    files = sorted(os.listdir(json_dir), reverse=True)
    # print(len(files))
    files = files[:-(len(files) % period.value)] if len(files) % period.value != 0 else files
    # print(len(files))
    files_number = period.value
    nodes_sets = []
    nodes_set = set()
    while files:
        for _ in range(files_number):
            with open(os.path.join(json_dir, files.pop()), 'r') as f:
                nodes = json.load(f)
            for node_info in nodes:
                nodes_set.add(f'{node_info[0]}-{node_info[1]}-{node_info[5]}')
            # print(len(nodes_set))
        nodes_sets.append(nodes_set)
        nodes_set = set()
    print("Finished ", len(nodes_sets))
    nodes_set1 = nodes_sets[0]
    for nodes_set in nodes_sets[1:]:
        print(f'Present in set 1 but not in set 2 : {len(nodes_set1 - nodes_set)}')
        print(f'Nodes missing : {len(nodes_set1 - nodes_set)/len(nodes_set1)*100}')
        print(f'Present in set 2 but not in set 1 : {len(nodes_set - nodes_set1)}')
        print(f'New nodes : {len(nodes_set - nodes_set1)/len(nodes_set)*100}')
        print()
        nodes_set1 = nodes_set


def client_distribution(csv_file: str, export_json_file: str, min_addr: int,
                        max_addr: int, outfile: str):
    """
    Reads the nodes_per_getADDR csv file (crawl dir) and the export json file
    (export dir) and counts the different client used by the nodes which
    returned a number of ADDR between min_addr and max_addr. It outputs the
    distribution in the outfile (JSON).
    """
    nodes_addrnumber = {}
    with open(csv_file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if min_addr <= int(row[2]) <= max_addr:
                nodes_addrnumber[row[1]] = [int(row[2])]

    with open(export_json_file, 'r') as f:
        nodes_list = json.load(f)
    for node_info in nodes_list:
        node = f'{node_info[0]}-{node_info[1]}-{node_info[5]}'
        try:
            nodes_addrnumber[node].append(node_info[3])
        except KeyError:
            pass

    random_key = random.choice(list(nodes_addrnumber.keys()))
    print(f"Key : {random_key}, value : {nodes_addrnumber[random_key]}")

    client_counter = Counter()
    for node, addr_client in nodes_addrnumber.items():
        if len(addr_client) == 2:
            client_counter.update([addr_client[1]])

    result = [{'client': key, 'number': value} for key, value in
              OrderedDict(client_counter.most_common()).items()]
    with open(outfile, 'w') as f:
        json.dump(result, f, indent=4)


def addr_per_node(csv_file: str, export_json_file: str):
    # columns: index, node, max ADDR returned
    data = pd.read_csv(csv_file, names=["node_index", "node",
                       "number of ADDR returned"], usecols=[0, 1, 2])
    i = 0
    j = 0
    for node in data.values:
        i += 1 if node[2] == -1 else 0
        j += 1 if node[2] == 0 else 0
    print(f"In crawl csv file, size : {len(data.index)}, number of -1 : {i}, 0 : {j}")

    with open(export_json_file, 'r') as f:
        data_json = json.load(f)
    rows_list = []
    for crawl_node in data.values:
        for exported_node in data_json:
            exported_node_formatted = f"{exported_node[0]}-{exported_node[1]}-\
                                        {exported_node[5]}"
            if crawl_node[1] == exported_node_formatted:
                rows_list.append(pd.DataFrame([crawl_node],
                                 columns=["node_index", "node",
                                 "number of ADDR returned"]))
                break
    df = pd.concat(rows_list, ignore_index=True)

    i = 0
    j = 0
    for node in df.values:
        i += 1 if node[2] == -1 else 0
        j += 1 if node[2] == 0 else 0
    print(df.index)
    print(f"In crawl csv file filtered by export json file, size : {len(df.index)}, number of -1 : {i}, 0 : {j}")

    # ax = sns.relplot(x=data.index, y="number of ADDR returned", edgecolor='none', data=data)
    # ax.set(xlabel='node index', ylabel='number of ADDR')
    # plt.show()

    ax = sns.relplot(x=df.index, y="number of ADDR returned", edgecolor='none', data=df)
    ax.set(xlabel='node index', ylabel='number of ADDR')
    plt.show()


def up_nodes_per_sec(csv_files: list):
    # old version

    # d = defaultdict(list)

    # data = pd.read_csv(argv[1], header=None)
    # print(data.values)
    # for elt in data.values[0]:
    #     d['timeline'].append(eval(elt)[0])
    #     d['up nodes'].append(eval(elt)[1])

    # df = pd.DataFrame(data=d)
    # sns.lineplot(x="timeline", y="up nodes", data=df)

    # new version (old now)

    # data = pd.read_csv(argv[1], names=["timeline", "up nodes"])
    # # print(data.values)

    # ax = sns.lineplot(x="timeline", y="up nodes", data=data)
    # ax.set(xlabel='time (s)', ylabel='number of nodes')

    # plt.show()

    data = pd.read_csv(csv_files[0], names=["timeline", os.path.splitext(csv_files[0])[0].split('_')[-1]])
    for csv_file in csv_files[1:]:
        data_to_add = pd.read_csv(csv_file, names=["timeline", os.path.splitext(csv_file)[0].split('_')[-1]])
        data = pd.merge(data, data_to_add, how='outer')
    data = pd.melt(data, ['timeline'], var_name='date of crawl', value_name='up nodes')
    ax = sns.lineplot(x="timeline", y="up nodes", hue="date of crawl", data=data)
    ax.set(xlabel='time (s)', ylabel='number of nodes')
    plt.show()


def main(argv):
    sns.set()
    # up_nodes_per_sec(argv[1:])
    # addr_per_node(argv[1], argv[2])
    client_distribution(argv[1], argv[2], int(argv[3]), int(argv[4]), argv[5])
    # churn(argv[1], ChurnPeriod.THIRTYMIN)
    # distinct_ip(argv[1])


if __name__ == "__main__":
    main(sys.argv)
