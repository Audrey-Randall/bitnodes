import seaborn as sns
import pandas as pd
import sys
import os
import matplotlib.pyplot as plt


def addr_per_node(csv_file: str):
    # colone index, noeud, max ADDR returned
    data = pd.read_csv(csv_file, names=["node_index", "node", "number of ADDR returned"], usecols=[0, 1, 2])
    print(data.values[2])
    i = 0
    j = 0
    for elt in data.values:
        i += 1 if elt[2] == -1 else 0
        j += 1 if elt[2] == 0 else 0

    print(i, j)
    # old version
    # child_nodes number distplot
    # d = [n[2] for n in data.values]
    # d = pd.Series(d, name="number of connections")
    # sns.distplot(d)

    ax = sns.relplot(x="node_index", y="number of ADDR returned", edgecolor='none', data=data)
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
    addr_per_node(argv[1])


if __name__ == "__main__":
    main(sys.argv)
