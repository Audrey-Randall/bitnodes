import seaborn as sns
import pandas as pd
import sys
import matplotlib.pyplot as plt
from collections import defaultdict


def main(argv):
    sns.set()

    ############################### ADDR response ###############################
    data = pd.read_csv(argv[1], names=["node_index", "node", "number of ADDR returned"], usecols=[0, 1, 2])
    print(data.values[2])
    i = 0
    j = 0
    for elt in data.values:
        i += 1 if elt[2] == -1 else 0
        j += 1 if elt[2] == 0 else 0

    print(i, j)
    ############ old version #############
    # child_nodes number distplot
    # d = [n[2] for n in data.values]
    # d = pd.Series(d, name="number of connections")
    # sns.distplot(d)


    ########### new version ###########
    ax = sns.relplot(x="node_index", y="number of ADDR returned", edgecolor='none', data=data)
    ax.set(xlabel='node index', ylabel='number of ADDR')


    ############################## UP_NODES ##############################

    ########### old version ###########

    # d = defaultdict(list)

    # data = pd.read_csv(argv[1], header=None)
    # print(data.values)
    # for elt in data.values[0]:
    #     d['timeline'].append(eval(elt)[0])
    #     d['up nodes'].append(eval(elt)[1])

    # df = pd.DataFrame(data=d)
    # sns.lineplot(x="timeline", y="up nodes", data=df)

    ########### new version ###########

    # data = pd.read_csv(argv[1], names=["timeline", "up nodes"])
    # # print(data.values)

    # ax = sns.lineplot(x="timeline", y="up nodes", data=data)
    # ax.set(xlabel='time (s)', ylabel='number of nodes')

    plt.show()


if __name__ == "__main__":
    main(sys.argv)
