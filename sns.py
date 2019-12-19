import seaborn as sns
import pandas as pd
import sys
import matplotlib.pyplot as plt


def main(argv):
    sns.set()
    data = pd.read_csv(argv[1])
    print(data)
    d = [n[1] for n in data.values]

#   sns.lineplot(x="timeline", y="up nodes", data=data)

    # child_nodes number distplot
    d = pd.Series(d, name="number of connections")
    sns.distplot(d)
    ##
    plt.show()


if __name__ == "__main__":
    main(sys.argv)
