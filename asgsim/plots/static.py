from multiprocessing import Pool

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean

from .. import cost
from ..cost import costs
from ..model import run_model


def merge(*dict_args):
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def plt_title(title):
    plt.title(title, y=1.05)


def plt_save(name):
    plt.savefig(name + '.svg', format='svg')
    plt.close()


def make_resolution_plot(resolution):
    theoretical_series = []
    measured_series = []
    run_times = range(1, 30)

    for run_time in run_times:
        per_hour = 60.0 / float(run_time) * 0.3
        m = run_model(ticks=1000000, build_run_time=(run_time * 60), builds_per_hour=per_hour, sec_per_tick=resolution)
        theoretical_series.append(m.theoretical_queue_time() / 60.0)
        measured_series.append(m.mean_queue_time() / 60.0)

    plt_title('Queue Times at 0.3X Capacity @ %d sec/tick' % resolution)
    plt.xlabel('Build Time (m)')
    plt.ylabel('Mean Queue Time (m)')
    t_handle, = plt.plot(run_times, theoretical_series, label='Theoretical')
    m_handle, = plt.plot(run_times, measured_series, 'ro', label='Model')
    plt.legend(handles=[t_handle, m_handle])
    plt_save('plots/%d_sec_per_tick' % resolution)


def make_resolution_plots():
    make_resolution_plot(60)
    make_resolution_plot(10)


def make_queue_time_v_utilization_plot():
    counts = range(84, 100)
    means = []
    p95s = []
    utilizations = []

    for count in counts:
        m = run_model(ticks=100000, build_run_time=300, builds_per_hour=1000.0, initial_builder_count=count, builder_boot_time=0)
        means.append(m.mean_queue_time() / 60.0)
        p95s.append(m.percentile_queue_time(95.0) / 60.0)
        utilizations.append(m.mean_percent_utilization())

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.set_title('Processing 1000 5min Builds / Hr')
    m_handle, = ax1.plot(counts, means, 'g-', label='Mean Queue Time (m)')
    p_handle, = ax1.plot(counts, p95s, 'r-', label='p95 Queue Time (m)')
    ax1.set_xlabel('Fleet Size')
    ax1.set_ylabel('Time (m)')

    ax2 = ax1.twinx()
    u_handle, = ax2.plot(counts, utilizations, 'b-', label='% Builder Utilization')
    ax2.set_ylabel('% Utilization')
    plt.legend(handles=[m_handle, p_handle, u_handle])
    plt_save('plots/queue_time_v_utilization')


def make_cost_curve_plot():
    sizes = range(100,150)
    opts = [{'initial_builder_count': size, 'builds_per_hour': 1000.0,
             'ticks': 1000000, 'sec_per_tick': 10, 'builder_boot_time': 0}
            for size in sizes]

    p = Pool(8)
    cost_data = map(mean, p.map(costs, opts))
    p.close()

    plt_title('Cost vs Fleet Size')
    plt.xlabel('Fleet Size')
    plt.ylabel('Cost ($)')
    plt.plot(sizes, cost_data)
    plt_save('plots/cost_curve')


def make_cost_plot(title, configs, axis, filename, **extra_opts):
    handles = []
    minima = []
    p = Pool(8)
    for config in configs:
        opts = [merge({'initial_builder_count': size, 'builder_boot_time': 0, 'trials': 10},
                      config['opts'],
                      extra_opts)
                for size in config['sizes']]
        cost_data = map(mean, p.map(costs, opts))
        handle, = plt.plot(config['sizes'], cost_data, config['color'] + '-', label=config['label'])
        handles.append(handle)
        min_cost = min(cost_data)
        min_size = config['sizes'][cost_data.index(min_cost)]
        minima.append((min_size, min_cost))
    p.close()
    plt_title(title)
    plt.xlabel('Fleet Size')
    plt.ylabel('Cost ($)')
    plt.axis(axis)
    plt.plot([m[0] for m in minima], [m[1] for m in minima], 'ko')
    plt.legend(handles=handles, loc='lower right')
    plt_save(filename)
    print title
    print 'Optimal fleet sizes:', minima


def make_cost_v_traffic_plots():
    configs = [
        {'sizes': range(1, 20), 'opts': {'builds_per_hour': 10.0}, 'label': '10 builds / hr', 'color': 'b'},
        {'sizes': range(5, 25), 'opts': {'builds_per_hour': 50.0}, 'label': '50 builds / hr', 'color': 'g'},
        {'sizes': range(10, 30), 'opts': {'builds_per_hour': 100.0}, 'label': '100 builds / hr', 'color': 'r'},
        {'sizes': range(20, 40), 'opts': {'builds_per_hour': 200.0}, 'label': '200 builds / hr', 'color': 'c'},
        {'sizes': range(50, 75), 'opts': {'builds_per_hour': 500.0}, 'label': '500 builds / hr', 'color': 'm'},
        {'sizes': range(95, 125), 'opts': {'builds_per_hour': 1000.0}, 'label': '1000 builds / hr', 'color': 'y'}
    ]
    slow_configs = [
        {'sizes': range(1, 20), 'opts': {'builds_per_hour': 1.0}, 'label': '1 builds / hr', 'color': 'b'},
        {'sizes': range(1, 20), 'opts': {'builds_per_hour': 2.0}, 'label': '2 builds / hr', 'color': 'g'},
        {'sizes': range(1, 20), 'opts': {'builds_per_hour': 5.0}, 'label': '5 builds / hr', 'color': 'r'},
        {'sizes': range(1, 20), 'opts': {'builds_per_hour': 10.0}, 'label': '10 builds / hr', 'color': 'c'},
        {'sizes': range(20, 30), 'opts': {'builds_per_hour': 20.0}, 'label': '20 builds / hr', 'color': 'm'},
        {'sizes': range(45, 55), 'opts': {'builds_per_hour': 50.0}, 'label': '50 builds / hr', 'color': 'y'}
    ]
    ticks = 100000
    make_cost_plot('Cost vs Fleet Size for Various Traffic Patterns (5 min builds, m4.large)', configs, [0, 150, 0, ticks * 15 / 1000], 'plots/cost_v_traffic_cheap',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR, build_run_time=300, ticks=ticks, sec_per_tick=10)
    make_cost_plot('Cost vs Fleet Size for Various Traffic Patterns (5 min builds, 2X m4.10xl)', configs, [0, 150, 0, ticks * 6 / 10], 'plots/cost_v_traffic_expensive',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR_EXPENSIVE, build_run_time=300, ticks=ticks, sec_per_tick=10)
    make_cost_plot('Cost vs Fleet Size for Various Traffic Patterns (40 min builds, m4.large)', slow_configs, [0, 60, 0, ticks * 5 / 100], 'plots/cost_v_traffic_slow',
                   cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR, build_run_time=2400, ticks=ticks, sec_per_tick=60)


def make_optimum_traffic_plot(title, traffics, minima, suffix=''):
    utilizations = []
    means = []

    for traffic, minimum in zip(traffics, minima):
        size, cost = minimum
        m = run_model(ticks=100000, builds_per_hour=traffic, build_run_time=300, builder_boot_time=0, sec_per_tick=10, initial_builder_count=size)
        utilizations.append(m.mean_percent_utilization())
        means.append(m.mean_queue_time())

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.set_title(title, y=1.05)
    m_handle, = ax1.plot(traffics, means, 'gs', label='Mean Queue Time (s)')
    ax1.axis([10, 1200, 0, 8])
    ax1.set_xlabel('Builds per Hour')
    ax1.set_ylabel('Time (s)')

    ax2 = ax1.twinx()
    u_handle, = ax2.plot(traffics, utilizations, 'bo', label='% Builder Utilization')
    ax2.axis([10, 1200, 0, 100])
    ax2.set_ylabel('% Utilization')
    plt.legend(handles=[m_handle, u_handle], loc='upper left')
    plt_save('plots/optimum_props_by_traffic' + suffix)


def make_optimum_traffic_plots():
    # Results from make_cost_v_traffic_plots
    traffics = [10.0, 50.0, 100.0, 200.0, 500.0, 1000.0]
    cheap_machine_minima = [(5, 156.90737777777775), (12, 280.16122222222214), (19, 389.0736),
                            (31, 537.01893333333339), (63, 815.00451111111101), (114, 1146.3129555555556)]
    expensive_machine_minima = [(3, 3587.621038888889), (9, 7239.3732538888889), (15, 9827.6235616666672),
                                (26, 13814.710410555557), (55, 21017.942717222224), (102, 29345.773465555558)]
    slow_minima = [(5, 900.67199999999991), (6, 1077.5042000000001), (10, 1572.2400666666667),
                   (17, 2157.8415999999997), (26, 2840.8574666666668), (53, 4335.4602666666669)]
    make_optimum_traffic_plot('Optimum Queue Time and Utilization (5 min builds, 1 m4.large / build) ', traffics, cheap_machine_minima, suffix='_cheap')
    make_optimum_traffic_plot('Optimum Queue Time and Utilization (5 min builds, 2 m4.10xls / build)', traffics, expensive_machine_minima, suffix='_expensive')


def make_cost_v_build_time_plots():
    configs = [
        {'sizes': range(1, 20), 'opts': {'build_run_time': 30}, 'label': '30 sec builds', 'color': 'b'},
        {'sizes': range(1, 20), 'opts': {'build_run_time': 60}, 'label': '1 min builds', 'color': 'g'},
        {'sizes': range(1, 20), 'opts': {'build_run_time': 120}, 'label': '2 min builds', 'color': 'r'},
        {'sizes': range(5, 25), 'opts': {'build_run_time': 300}, 'label': '5 min builds', 'color': 'c'},
        {'sizes': range(10, 35), 'opts': {'build_run_time': 600}, 'label': '10 min builds', 'color': 'm'},
        {'sizes': range(20, 45), 'opts': {'build_run_time': 1200}, 'label': '20 min builds', 'color': 'y'},
        {'sizes': range(40, 65), 'opts': {'build_run_time': 2400}, 'label': '40 min builds', 'color': 'k'}
    ]
    slow_configs = [
        {'sizes': range(1, 10), 'opts': {'build_run_time': 30}, 'label': '30 sec builds', 'color': 'b'},
        {'sizes': range(1, 10), 'opts': {'build_run_time': 60}, 'label': '1 min builds', 'color': 'g'},
        {'sizes': range(1, 10), 'opts': {'build_run_time': 120}, 'label': '2 min builds', 'color': 'r'},
        {'sizes': range(1, 10), 'opts': {'build_run_time': 300}, 'label': '5 min builds', 'color': 'c'},
        {'sizes': range(1, 10), 'opts': {'build_run_time': 600}, 'label': '10 min builds', 'color': 'm'},
        {'sizes': range(1, 10), 'opts': {'build_run_time': 1200}, 'label': '20 min builds', 'color': 'y'},
        {'sizes': range(1, 10), 'opts': {'build_run_time': 2400}, 'label': '40 min builds', 'color': 'k'}
    ]

    ticks = 100000
    make_cost_plot('Cost vs Fleet Size for Various Build Times (50 builds / hr, m4.large)', configs, [0, 60, 0, ticks * 1 / 100], 'plots/cost_v_build_time_cheap',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR, builds_per_hour=50.0, ticks=ticks, sec_per_tick=10)
    make_cost_plot('Cost vs Fleet Size for Various Build Times (50 builds / hr, 2X m4.10xl)', configs, [0, 60, 0, ticks * 25 / 100], 'plots/cost_v_build_time_expensive',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR_EXPENSIVE, builds_per_hour=50.0, ticks=ticks, sec_per_tick=10)
    make_cost_plot('Cost vs Fleet Size for Various Build Times (2 builds / hr, m4.large)', slow_configs, [0, 10, 0, ticks * 3 / 1000], 'plots/cost_v_build_time_slow',
                   cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR, builds_per_hour=2.0, ticks=ticks, sec_per_tick=10)


def make_optimum_build_time_plot(title, build_times, minima, suffix=''):
    plot_times = [bt / 60.0 for bt in build_times]
    utilizations = []
    means = []

    for build_time, minimum in zip(build_times, minima):
        size, cost = minimum
        m = run_model(ticks=100000, builds_per_hour=50.0, build_run_time=build_time, builder_boot_time=0, sec_per_tick=10, initial_builder_count=size)
        utilizations.append(m.mean_percent_utilization())
        means.append(m.mean_queue_time())

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.set_title(title, y=1.05)
    m_handle, = ax1.plot(plot_times, means, 'gs', label='Mean Queue Time (s)')
    ax1.axis([0, 60, 0, 8])
    ax1.set_xlabel('Build Time (m)')
    ax1.set_ylabel('Time (s)')

    ax2 = ax1.twinx()
    u_handle, = ax2.plot(plot_times, utilizations, 'bo', label='% Builder Utilization')
    ax2.axis([0, 60, 0, 100])
    ax2.set_ylabel('% Utilization')
    plt.legend(handles=[m_handle, u_handle], loc='upper left')
    plt_save('plots/optimum_props_by_build_time' + suffix)


def make_optimum_build_time_plots():
    # Results from make_cost_v_build_time_plots
    build_times = [30, 60, 120, 300, 600, 1200, 2400]
    cheap_machine_minima = [(4, 122.30462423397832), (5, 156.90400521723009), (7, 186.61573547738476),
                            (12, 266.18263968907178), (19, 391.88402948743175), (31, 497.54767044356112), (55, 739.6319964639514)]
    expensive_machine_minima = [(2, 2673.9492266666666), (3, 3603.7000955555559), (5, 4863.7484711111119),
                                (9, 7243.547856666667), (15, 9822.7221577777782), (25, 13419.815085555554), (45, 18624.54101444445)]
    slow_minima = [(1, 40.895044444444444), (2, 66.097422222222207), (2, 66.289766666666679),
                   (3, 94.816888888888883), (3, 110.61755555555555), (5, 147.17408888888889), (6, 176.68764444444443)]
    make_optimum_build_time_plot('Optimum Queue Time and Utilization (50 builds / hour, 1 m4.large / build) ', build_times,  cheap_machine_minima, suffix='_cheap')
    make_optimum_build_time_plot('Optimum Queue Time and Utilization (50 builds / hour, 2 m4.10xls / build)', build_times, expensive_machine_minima, suffix='_expensive')


if __name__ == '__main__':
    print 'Resolution'
    make_resolution_plots()
    print 'Queue Time v Utilization'
    make_queue_time_v_utilization_plot()
    print 'Cost Curve'
    make_cost_curve_plot()
    print 'Cost v Traffic'
    make_cost_v_traffic_plots()
    print 'Optimum Fleets Traffic'
    make_optimum_traffic_plots()
    print 'Cost v Build Time'
    make_cost_v_build_time_plots()
    print 'Optimum Fleets Build Time'
    make_optimum_build_time_plots()
