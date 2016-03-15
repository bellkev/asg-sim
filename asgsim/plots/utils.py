import matplotlib
matplotlib.use('Agg')
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

from ..model import run_model

def plt_title(title):
    plt.title(title, y=1.05)


def plt_save(path):
    plt.savefig(path + '.svg', format='svg')
    plt.close()


def make_scaling_plot(params, title, path, axis=None):
    m = run_model(**params)
    purple = '#BBA4D1'
    blue = '#3399CC'
    plt_title(title)
    plt.stackplot([(tick * params['sec_per_tick']) / 60.0 for tick in range(m.ticks)], m.builders_in_use, m.builders_available,
                  colors=(purple, blue), linewidth=0)
    plt.legend([mpatches.Patch(color=purple),
                mpatches.Patch(color=blue)],
               ['Busy Builder Machines','Available Builder Machines'])
    plt.xlabel('Time (m)')
    plt.ylabel('Machines')
    if axis:
        plt.axis(axis)
    plt_save(path)
