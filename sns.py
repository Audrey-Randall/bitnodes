import seaborn as sns
import pandas as pd
import sys
import os
import json
import csv
import matplotlib.pyplot as plt
import numpy as np
import random
import pytz
import datetime
import bisect
import statistics
import pickle
from typing import List
from collections import Counter, OrderedDict, defaultdict
from enum import Enum
import countries_codes
import packaging.version
from clients_metadata import vulnerable_client_versions, versions_date


def int_default_value():
    return int('-2')


def geo_distribution_by_continent_several_days(
        json_dirs: List[str],
        continent: countries_codes.Continent):
    average_list = []
    for d in json_dirs:
        average = geo_distribution_by_continent(d, continent)
        average_list.append(average)
    print("Total average number of nodes hosted in "
          f"continent {continent.name} : {statistics.mean(average_list)}")


def geo_distribution_by_continent(json_dir: str,
                                  continent: countries_codes.Continent):
    result = {}
    cnt = Counter()
    local = pytz.timezone("Europe/Paris")
    for filename in os.listdir(json_dir):
        naive_time = datetime.datetime.strptime(os.path.splitext(filename)[0],
                                                "%Y%m%d-%H:%M:%S")
        utc_time = local.localize(naive_time, is_dst=None).\
            astimezone(pytz.utc).strftime("%H:%M:%S")
        with open(os.path.join(json_dir, filename), 'r') as f:
            nodes = json.load(f)
        for node_info in nodes:
            country_code = node_info[9]
            if country_code in countries_codes.continents[continent.value]:
                cnt[continent.name] += 1
        result[utc_time] = cnt
        cnt = Counter()
    # print(result.values[0])
    all_nodes_list = [v[continent.name] for v in result.values()]
    average = statistics.mean(all_nodes_list)
    print(f"Average number of nodes hosted in "
          f"continent {continent.name} : {average}")
    return average


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
    last = len(ax.get_xticklabels()) - 1
    for ind, label in enumerate(ax.get_xticklabels()):
        if ind == last:
            label.set_visible(True)
        elif ind % 30 == 0:  # every nth label is kept
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
                all_nodes_set.add(f'{node_info[0]}')
    print(f"Distinct IP in all the data files from the dir : {len(all_nodes_set)}")


class ChurnPeriod(Enum):
    THIRTYMIN = 7
    ONEHOUR = 15
    TWOHOUR = 30
    FOURHOUR = 60
    EIGHTHOUR = 120
    ONEDAY = 360


def churn(json_dirs: List[str], period: ChurnPeriod, result_file: str):
    files = []
    for d in json_dirs:
        for f in os.listdir(d):
            files.append(os.path.join(d, f))
    files.sort(reverse=True)
    print(len(files))
    files = files[:-(len(files) % period.value)] if len(files) % period.value != 0 else files
    print(len(files))
    files_number = period.value
    nodes_sets = []
    nodes_set = set()
    while files:
        day = os.path.basename(files[-1])
        day = os.path.splitext(day)[0]
        if period == ChurnPeriod.ONEDAY:
            day = day.split('-')[0]
        else:
            day = day.replace('-', ' ')[:-3]
        # day = day[:4] + '-' + day[4:6] + '-' + day[6:]
        print(day)
        for _ in range(files_number):
            filename = files.pop()
            with open(filename, 'r') as f:
                nodes = json.load(f)
            for node_info in nodes:
                nodes_set.add(f'{node_info[0]}-{node_info[1]}-{node_info[5]}')
            # print(len(nodes_set))
        nodes_sets.append((f'{day}', nodes_set))
        nodes_set = set()
    print("Finished ", len(nodes_sets))
    missing_list = []
    new_list = []
    result = {}
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
        result[f'{day_i}–{day_j}'] = {
            'Nodes leaving the network': missing_pct,
            'Nodes (re)joining the network': new_pct
        }
    print(f'Min missing {round(min(missing_list),2)}%, '
          f'Max missing {round(max(missing_list),2)}% '
          f'(or {round(sorted(missing_list)[-2],2)}), '
          f'Mean missing {round(statistics.mean(missing_list),2)}% (or '
          f'{round(statistics.mean(sorted(missing_list)[:-1]),2)})')
    print(f'Min new {round(min(new_list),2)}%, Max new {round(max(new_list),2)}%, '
          f'Mean new {round(statistics.mean(new_list),2)}%')
    with open(result_file, 'w') as f:
        json.dump(result, f)


def display_churn(json_file: str, nth_x_axis=1, mondays=False):
    with open(json_file, 'r') as f:
        result = json.load(f)
    df = pd.DataFrame.from_dict(result, orient='index')
    df.columns = ['Nodes leaving the network', 'Nodes (re)joining the network']
    if mondays:
        df.rename(index=lambda s: s.replace('–', ', '), inplace=True)
    print(df)
    fig, ax = plt.subplots()
    sns.lineplot(data=df, ax=ax)
    ax.set(xlabel='Time', ylabel='Rate (%)')
    last = len(ax.get_xticklabels()) - 1
    # print(last)
    # for ind, label in enumerate(ax.get_xticklabels()):
    #     if ind == last:
    #         print(f"{ind} visible")
    #         label.set_visible(True)
    #     elif ind % nth_x_axis == 0:  # every nth label is kept
    #         print(f"{ind} visible")
    #         label.set_visible(True)
    #     else:
    #         print(f"{ind} NOT visible")
    #         label.set_visible(False)

    # ax.legend(loc='lower left', bbox_to_anchor=(0.65, 0.7))
    ax.set_ylim(ymin=0, ymax=20)
    rg = np.linspace(0, last, nth_x_axis).astype(int).tolist()
    print(rg)
    ax.set_xticks(rg)
    fig.autofmt_xdate()
    plt.show()


def client_distribution(export_json_file: str, result_filename=None):
    with open(export_json_file, 'r') as f:
        nodes_list = json.load(f)
    client_counter = Counter()
    for node_info in nodes_list:
        version = node_info[3]
        paren_start = version.find('(')
        paren_end = version.find(')')
        if paren_start != -1 and paren_end != -1:
            # remove text inside parenthesis in version
            version = version[:paren_start] + version[paren_end+1:]
        client_counter.update([version])
    result = OrderedDict(client_counter.most_common())
    df = pd.DataFrame.from_dict(result, orient='index', columns=["Number of nodes"])
    if result_filename is not None:
        df.to_csv(result_filename, index_label="Client version")

    satoshi_number = 0
    btcd_number = 0
    bcoin_number = 0
    bitcoin_unlimited_number = 0
    total_number = 0
    for key, value in result.items():
        if key.startswith("/Satoshi:"):
            satoshi_number += value
        elif "btcd" in key:
            btcd_number += value
        elif key.startswith("/bcoin:"):
            bcoin_number += value
        elif key.startswith("/BitcoinUnlimited:"):
            bitcoin_unlimited_number += value
        total_number += value
    print(f"Total Number of nodes : {total_number}")
    print(f"Satoshi number : {satoshi_number}")
    print(f"Percentage : {round((satoshi_number/total_number)*100, 3)}")
    print(f"btcd number : {btcd_number}")
    print(f"Percentage : {round((btcd_number/total_number)*100, 3)}")
    print(f"bcoin number : {bcoin_number}")
    print(f"Percentage : {round((bcoin_number/total_number)*100, 3)}")
    print(f"bitcoin_unlimited number : {bitcoin_unlimited_number}")
    print(f"Percentage : {round((bitcoin_unlimited_number/total_number)*100, 3)}")


def client_distribution_addr_per_node(csv_file: str, export_json_file: str,
                                      min_addr: int, max_addr: int,
                                      outfile: str):
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


def addr_per_node(export_json_file: str, csv_files: List[str], result_file=None):
    # columns: index, node, max ADDR returned
    # data = pd.read_csv(csv_file, names=["node_index", "node",
    #                    "number of ADDR returned"], usecols=[0, 1, 2])
    # i = 0
    # j = 0
    # for node in data.values:
    #     i += 1 if node[2] == -1 else 0
    #     j += 1 if node[2] == 0 else 0
    # print(f"In crawl csv file, size : {len(data.index)}, number of -1 : {i}, 0 : {j}")

    data_csv = pd.read_csv(csv_files[0], names=["node_index", "node",
                           "number of ADDR returned"], usecols=[0, 1, 2])
    for csv_file in csv_files[1:]:
        data_to_add = pd.read_csv(csv_file, names=["node_index", "node",
                                  "number of ADDR returned"], usecols=[0, 1, 2])
        data_csv = data_csv.append(data_to_add)
    print(data_csv)

    i = 0
    j = 0
    for node in data_csv.values:
        i += 1 if node[2] == -1 else 0
        j += 1 if node[2] == 0 else 0
    print(f"In crawl csv file, size : {len(data_csv.index)}, number of -1 : {i}, 0 : {j}")

    data = defaultdict(list)
    for crawl_node in data_csv.values:
        data[crawl_node[1]].append(crawl_node[2])

    with open(export_json_file, 'r') as f:
        data_json = json.load(f)
    result = defaultdict(int_default_value)

    i = 0
    for exported_node in data_json:
        added = False
        exported_node_formatted = (
            f"{exported_node[0]}-{exported_node[1]}-{exported_node[5]}"
        )
        if len(data[exported_node_formatted]) > 0:
            result[i] = max(data[exported_node_formatted])
            added = True
        if added:
            i += 1
    if result_file is not None:
        with open(result_file, 'wb') as f:
            pickle.dump(result, f, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        df = pd.DataFrame.from_dict(result, orient='index', columns=[None])
        print(df)
        h = i = j = k = m = n = 0
        for node in df.values:
            h += 1 if node[0] == -2 else 0
            i += 1 if node[0] == -1 else 0
            j += 1 if node[0] == 0 else 0
            k += 1 if node[0] <= 50 else 0
            m += 1 if node[0] <= 25 else 0
            n += 1 if node[0] <= 12 else 0
        print(f"In crawl csv file filtered by export json file, size : {len(df.index)},"
              f"number of -2 : {h}, -1 : {i}, 0 : {j}, <= 50 : {k}, <= 25 : {m}, <= 12 : {n}")

        ax = sns.relplot(edgecolor='none', data=df)
        ax.set(xlabel='Node index', ylabel='Number of addresses')
        plt.show()


def display_addr_per_node(pickle_file: str):
    with open(pickle_file, 'rb') as f:
        result = pickle.load(f)
    df = pd.DataFrame.from_dict(result, orient='index', columns=[None])
    print(df)
    i = 0
    j = 0
    k = 0
    m = 0
    n = 0
    for node in df.values:
        i += 1 if node[0] == -1 else 0
        j += 1 if node[0] == 0 else 0
        k += 1 if node[0] <= 50 else 0
        m += 1 if node[0] <= 25 else 0
        n += 1 if node[0] <= 12 else 0
    print(f"In crawl csv file filtered by export json file, size : {len(df.index)},"
          f"number of -1 : {i}, 0 : {j}, <= 50 : {k}, <= 25 : {m}, <= 12 : {n}")
    ax = sns.scatterplot(edgecolor='none', data=df)
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
    fig, ax = plt.subplots()
    ax.set(xlabel='Elapsed time (in seconds)', ylabel='Number of nodes')
    sns.lineplot(data=data, ax=ax)
    # fig.autofmt_xdate()
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


def vulnerable_nodes_number(client_distrib_csv_filename, cve_name):
    version_counter = Counter()
    with open(client_distrib_csv_filename, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            version_counter.update(Counter({
                row['Client version']: int(row['Number of nodes'])
            }))
    vulnerable_versions_count = 0
    for client_types, vulnerable_version in vulnerable_client_versions[cve_name]:
        print(client_types, vulnerable_version)
        for client_version, count in version_counter.items():
            if "/Knots:" in client_version:
                client_version = client_version[:-10]
            start = client_types[0]
            end = client_types[1] if len(client_types) == 2 else '/'
            if client_version.startswith(start) and client_version.endswith(end):
                version = client_version.split(':')[1].split('/')[0]
                v_target = packaging.version.parse(version)
                if len(vulnerable_version) == 1:
                    v = packaging.version.parse(vulnerable_version[0])
                    print(f"{v_target} < {v} = {v_target < v}")
                    if v_target < v:
                        print(client_version, count)
                        vulnerable_versions_count += count
                elif len(vulnerable_version) == 2:
                    v1 = packaging.version.parse(vulnerable_version[0])
                    v2 = packaging.version.parse(vulnerable_version[1])
                    print(f"{v1} <= {v_target} < {v2} = {v1 <= v_target < v2}")
                    if v1 <= v_target < v2:
                        print(client_version, count)
                        vulnerable_versions_count += count
    print(vulnerable_versions_count)


def temporal_distribution_clients(client_distrib_csv_filename):
    version_counter = Counter()
    with open(client_distrib_csv_filename, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            version_counter.update(Counter({
                row['Client version']: int(row['Number of nodes'])
            }))
    total = 0
    result = Counter()
    for date, versions in versions_date.items():
        subtotal = 0
        print(f"{date}:")
        for version, count in version_counter.items():
            if version.startswith("/Satoshi:"):
                version = version.split(':')[1].split('/')[0]
                if version in versions:
                    result[date] += count
                    subtotal += count
                    print(f"\t{version}({count})")
                    print(f"\tadd {count}, subtotal = {subtotal}")
        total += subtotal
    print(f"Total: {total}")
    df = pd.DataFrame.from_dict(result, orient="index", columns=["Count"])
    df = df[::-1]
    print(df)
    fig, ax = plt.subplots()
    ax.set(xlabel='Date', ylabel='Count')
    sns.barplot(x=df.index, y="Count", data=df, ax=ax, color="cornflowerblue")
    fig.autofmt_xdate()
    plt.show()


def main(argv):
    sns.set()
    sns.set_style("darkgrid", {"xtick.major.size": 3})
    # up_nodes_per_sec(argv[1:])
    # addr_per_node(argv[1], argv[2:])#:-1]), argv[-1])
    # display_addr_per_node(argv[1])
    # client_distribution_addr_per_node(argv[1], argv[2], int(argv[3]), int(argv[4]), argv[5])
    # churn(argv[1:-1], ChurnPeriod.ONEHOUR, argv[-1])
    # display_churn(argv[1], 16, False)
    # distinct_ip(argv[1:])
    # geo_distribution_per_hour(argv[1])
    # geo_distribution_by_continent(argv[1], countries_codes.Continent.EUROPE)
    # geo_distribution_by_continent_several_days(argv[1:], countries_codes.Continent.NORTH_AMERICA)
    # number_of_nodes(argv[1:]) 
    # client_distribution(argv[1], argv[2])
    # vulnerable_nodes_number(argv[1], argv[2])
    temporal_distribution_clients(argv[1])


if __name__ == "__main__":
    main(sys.argv)
