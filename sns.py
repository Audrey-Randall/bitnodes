import seaborn as sns
import pandas as pd
import sys
import os
import json
import csv
import matplotlib.pyplot as plt
import random
import pytz
import datetime
import bisect
import statistics
from collections import Counter, OrderedDict
from enum import Enum
from timeit import default_timer as timer


def geo_distribution_per_hour(json_dir: str):
    europe = (-25., 45.)
    america = (-142, -25.0001)
    alaska = (-170., -142.0001, 45., 75.)
    hawai = (-170., -150, 15., 30.)
    asia = (45.0001, 180.)
    # other1 = (155.0001, 180.)
    # other2 = (-179.9999, -135.0001)
    result = {}
    cnt = Counter()
    local = pytz.timezone("Europe/Paris")
    for ind, filename in enumerate(os.listdir(json_dir)):
        naive_time = datetime.datetime.strptime(os.path.splitext(filename)[0],
                                                "%Y%m%d-%H:%M:%S")
        utc_time = local.localize(naive_time, is_dst=None).\
            astimezone(pytz.utc).strftime("%H:%M:%S")
        with open(os.path.join(json_dir, filename), 'r') as f:
            nodes = json.load(f)
        for node_info in nodes:
            longitude = float(node_info[11])
            latitude = float(node_info[10])
            if europe[0] <= longitude <= europe[1]:
                cnt['Europe / Africa / Middle East'] += 1
            elif america[0] <= longitude <= america[1]:
                cnt['Americas'] += 1
            elif asia[0] <= longitude <= asia[1]:
                cnt['Asia / Oceania'] += 1
            elif alaska[0] <= longitude <= alaska[1] and alaska[2] <= latitude <= alaska[3]:
                # Alaska
                cnt['Americas'] += 1
            elif hawai[0] <= longitude <= hawai[1] and hawai[2] <= latitude <= hawai[3]:
                # Hawaï
                cnt['Asia / Oceania'] += 1
            else:
                # if ind == 15:
                print(f"Not in europe, america, or asia : {longitude},"
                      f"{node_info[10]}, {node_info[8]}, {node_info[9]}")
                cnt['other'] += 1
        result[utc_time] = cnt
        cnt = Counter()
    df = pd.DataFrame.from_dict(result, orient='index')
    print(df)
    fig, ax = plt.subplots()
    sns.lineplot(data=df, ax=ax)
    ax.set(xlabel='Time (in UTC)', ylabel='Number of nodes')
    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 15 == 0:  # every 15th label is kept
            label.set_visible(True)
        else:
            label.set_visible(False)
    ax.legend(loc='lower left', bbox_to_anchor=(0.65, 0.7))
    ax.set_ylim(ymin=0)
    fig.autofmt_xdate()
    plt.show()


def distinct_ip(export_json_dirs: str):
    all_nodes_set = set()
    for export_json_dir in export_json_dirs:
        for filename in os.listdir(export_json_dir):
            path = os.path.join(export_json_dir, filename)
            with open(path, 'r') as f:
                nodes = json.load(f)
            for node_info in nodes:
                all_nodes_set.add(f'{node_info[0]}-{node_info[1]}-{node_info[5]}')
    print(f"Distinct IP in all the data files from the dir : {len(all_nodes_set)}")


class ChurnPeriod(Enum):
    THIRTYMIN = 7
    ONEHOUR = 15
    TWOHOUR = 30
    FOURHOUR = 60
    EIGHTHOUR = 120
    ONEDAY = 360


def churn(json_dir: str, period: ChurnPeriod):
    files = sorted(os.listdir(json_dir), reverse=True)
    print(len(files))
    files = files[:-(len(files) % period.value)] if len(files) % period.value != 0 else files
    print(len(files))
    files_number = period.value
    nodes_sets = []
    nodes_set = set()
    i = 1
    while files:
        for _ in range(files_number):
            filename = files.pop()
            with open(os.path.join(json_dir, filename), 'r') as f:
                nodes = json.load(f)
            for node_info in nodes:
                nodes_set.add(f'{node_info[0]}-{node_info[1]}-{node_info[5]}')
            # print(len(nodes_set))
        nodes_sets.append((f'jour {i}', nodes_set))
        i += 1
        nodes_set = set()
    print("Finished ", len(nodes_sets))
    missing_list = []
    new_list = []
    for (day_i, nodes_set_i), (day_j, nodes_set_j) in zip(nodes_sets, nodes_sets[1:]):
        print(f'Période : {day_i} / {day_j}')
        print(f'Nombre noeuds : {len(nodes_set_i)}/{len(nodes_set_j)}')
        missing = len(nodes_set_i - nodes_set_j)
        print(f'Présent dans le set1 et pas dans le set2 : {missing}')
        missing_pct = missing/len(nodes_set_i)*100
        missing_list.append(missing_pct)
        missing_pct = round(missing_pct, 2)
        print(f'Noeuds manquants : {missing_pct}%')
        new = len(nodes_set_j - nodes_set_i)
        print(f'Présent dans le set2 et pas dans le set1 : {new}')
        new_pct = new/len(nodes_set_j)*100
        new_list.append(new_pct)
        new_pct = round(new_pct, 2)
        print(f'Nouveaux noeuds: {new_pct}%\n')
    print(f'Min missing {min(missing_list)}%, '
          f'Max missing {max(missing_list)}% (or {sorted(missing_list)[-2]}), '
          f'Mean missing {round(statistics.mean(missing_list),2)}% (or '
          f'{round(statistics.mean(sorted(missing_list)[:-1]),2)})')
    print(f'Min new {min(new_list)}%, Max new {max(new_list)}%, '
          f'Mean new {round(statistics.mean(new_list),2)}%')


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
    result = {}
    i = 1
    # rows_list = []
    for crawl_node in data.values:
        for exported_node in data_json:
            exported_node_formatted = (
                f"{exported_node[0]}-{exported_node[1]}-{exported_node[5]}"
            )
            if crawl_node[1] == exported_node_formatted:
                result[i] = crawl_node[2]
                i += 1
                break
    df = pd.DataFrame.from_dict(result, orient='index', columns=[None])
    i = 0
    j = 0
    for node in df.values:
        i += 1 if node[0] == -1 else 0
        j += 1 if node[0] == 0 else 0
    print(f"In crawl csv file filtered by export json file, size : {len(df.index)},"
          f"number of -1 : {i}, 0 : {j}")

    ax = sns.relplot(edgecolor='none', data=df)
    ax.set(xlabel='Node index', ylabel='Number of addresses')
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

    data = pd.read_csv(csv_files[0], names=[os.path.splitext(csv_files[0])[0].split('_')[-1]])
    for csv_file in csv_files[1:]:
        data_to_add = pd.read_csv(csv_file, names=[os.path.splitext(csv_file)[0].split('_')[-1]])
        data = data.join(data_to_add, how='outer')
    ax = sns.lineplot(data=data)
    ax.set(xlabel='Elapsed time (in seconds)', ylabel='Number of nodes')
    plt.show()


def number_of_nodes(export_json_dirs):
    number_of_nodes = []
    minimum = (None, None)
    maximum = (None, None)
    for export_json_dir in export_json_dirs:
        for filename in os.listdir(export_json_dir):
            path = os.path.join(export_json_dir, filename)
            with open(path, 'r') as f:
                nodes = json.load(f)
            number = len(nodes)
            bisect.insort(number_of_nodes, number)
            if minimum[0] is None or minimum[0] > number:
                minimum = (number, path)
            if maximum[0] is None or maximum[0] < number:
                maximum = (number, path)
            # number_of_nodes.append(len(nodes))
    print(f"Min : {minimum}\nMax : {maximum}\n"
          f"Moy : {statistics.mean(number_of_nodes)}\n"
          f"Median : {statistics.median(number_of_nodes)}")


def main(argv):
    sns.set()
    # up_nodes_per_sec(argv[1:])
    # addr_per_node(argv[1], argv[2])
    # client_distribution(argv[1], argv[2], int(argv[3]), int(argv[4]), argv[5])
    # churn(argv[1], ChurnPeriod.ONEDAY)
    # distinct_ip(argv[1:])
    geo_distribution_per_hour(argv[1])
    # number_of_nodes(argv[1:])


if __name__ == "__main__":
    main(sys.argv)
