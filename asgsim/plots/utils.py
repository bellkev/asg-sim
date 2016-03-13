import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plt_title(title):
    plt.title(title, y=1.05)


def plt_save(path):
    plt.savefig(path + '.svg', format='svg')
    plt.close()
