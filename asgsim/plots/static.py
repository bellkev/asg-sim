from math import log
from multiprocessing import Pool

from numpy import mean

from ..batches import STATIC_MINIMA_LIMITED
from .. import cost
from ..cost import costs
from ..model import run_model
from .utils import plt, plt_title, plt_save, make_scaling_plot


def merge(*dict_args):
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


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

    plt_title('Cost vs Fleet Size (1000 builds / hr, 5 min / build)')
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
        handle, = plt.plot(config['sizes'], cost_data, '-', color=config['color'], label=config['label'])
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


def make_optimum_plot(title, i_var, label, vals, minima, axes, suffix='', transform=lambda x:x, log_scale=False, loc='upper left', **kwargs):
    utilizations = []
    means = []

    for val, minimum in zip(vals, minima):
        extra_kwargs = merge(kwargs, {i_var: val})
        size, cost = minimum
        m = run_model(sec_per_tick=10, initial_builder_count=size, **extra_kwargs)
        utilizations.append(m.mean_percent_utilization())
        means.append(m.mean_queue_time())

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.set_title(title, y=1.05)
    m_handle, = ax1.plot(map(transform, vals), map(lambda x: log(x, 10), means), 'gs', label='Mean Queue Time (s)')
    ax1.axis(axes[0])
    ax1.set_xlabel(label)
    if log_scale:
        ax1.set_ylabel('log_10(Time (s))')
    else:
        ax1.set_ylabel('Time (s)')

    ax2 = ax1.twinx()
    u_handle, = ax2.plot(map(transform, vals), utilizations, 'bo', label='% Builder Utilization')
    ax2.axis(axes[1])
    ax2.set_ylabel('% Utilization')
    plt.legend(handles=[m_handle, u_handle], loc=loc)
    plt_save('plots/optimum_props_by_%s%s' % (i_var, suffix))



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
    make_cost_plot('Cost vs Fleet Size for Various Traffic Patterns (5 min builds)', configs, [0, 150, 0, ticks * 15 / 1000], 'plots/cost_v_traffic_cheap',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR, build_run_time=300, ticks=ticks, sec_per_tick=10)
    make_cost_plot('Cost vs Fleet Size for Various Traffic Patterns (5 min builds, 2X m4.10xl)', configs, [0, 150, 0, ticks * 6 / 10], 'plots/cost_v_traffic_expensive',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR_EXPENSIVE, build_run_time=300, ticks=ticks, sec_per_tick=10)
    make_cost_plot('Cost vs Fleet Size for Various Traffic Patterns (40 min builds)', slow_configs, [0, 60, 0, ticks * 5 / 100], 'plots/cost_v_traffic_slow',
                   cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR, build_run_time=2400, ticks=ticks, sec_per_tick=60)


def make_optimum_traffic_plots():
    # Results from make_cost_v_traffic_plots
    traffics = [10.0, 50.0, 100.0, 200.0, 500.0, 1000.0]
    cheap_machine_minima = [(5, 156.90737777777775), (12, 280.16122222222214), (19, 389.0736),
                            (31, 537.01893333333339), (63, 815.00451111111101), (114, 1146.3129555555556)]
    expensive_machine_minima = [(3, 3587.621038888889), (9, 7239.3732538888889), (15, 9827.6235616666672),
                                (26, 13814.710410555557), (55, 21017.942717222224), (102, 29345.773465555558)]
    slow_minima = [(5, 900.67199999999991), (6, 1077.5042000000001), (10, 1572.2400666666667),
                   (17, 2157.8415999999997), (26, 2840.8574666666668), (53, 4335.4602666666669)]
    axes = ([10, 1200, 0, 8], [0, 1200, 0, 100])
    kwargs = dict(ticks=100000, build_run_time=300, builder_boot_time=0)
    make_optimum_plot('Optimum Queue Time and Utilization (5 min builds) ', 'builds_per_hour', 'Builds per hour',
                      traffics, cheap_machine_minima, axes, suffix='_cheap', **kwargs)
    make_optimum_plot('Optimum Queue Time and Utilization (5 min builds, 2 m4.10xls / build)', 'builds_per_hour', 'Builds per hour',
                      traffics, expensive_machine_minima, axes, suffix='_expensive', **kwargs)


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
    make_cost_plot('Cost vs Fleet Size for Various Build Times (50 builds / hr)', configs, [0, 60, 0, ticks * 1 / 100], 'plots/cost_v_build_time_cheap',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR, builds_per_hour=50.0, ticks=ticks, sec_per_tick=10)
    make_cost_plot('Cost vs Fleet Size for Various Build Times (50 builds / hr, 2X m4.10xl)', configs, [0, 60, 0, ticks * 25 / 100], 'plots/cost_v_build_time_expensive',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR_EXPENSIVE, builds_per_hour=50.0, ticks=ticks, sec_per_tick=10)
    make_cost_plot('Cost vs Fleet Size for Various Build Times (2 builds / hr)', slow_configs, [0, 10, 0, ticks * 3 / 1000], 'plots/cost_v_build_time_slow',
                   cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR, builds_per_hour=2.0, ticks=ticks, sec_per_tick=10)


def make_optimum_build_time_plots():
    # Results from make_cost_v_build_time_plots
    build_times = [30, 60, 120, 300, 600, 1200, 2400]
    cheap_machine_minima = [(4, 122.30462423397832), (5, 156.90400521723009), (7, 186.61573547738476),
                            (12, 266.18263968907178), (19, 391.88402948743175), (31, 497.54767044356112), (55, 739.6319964639514)]
    expensive_machine_minima = [(2, 2673.9492266666666), (3, 3603.7000955555559), (5, 4863.7484711111119),
                                (9, 7243.547856666667), (15, 9822.7221577777782), (25, 13419.815085555554), (45, 18624.54101444445)]
    slow_minima = [(1, 40.895044444444444), (2, 66.097422222222207), (2, 66.289766666666679),
                   (3, 94.816888888888883), (3, 110.61755555555555), (5, 147.17408888888889), (6, 176.68764444444443)]
    axes = ([0, 60, 0, 8], [0, 60, 0, 100])
    kwargs = dict(ticks=100000, builds_per_hour=50.0, builder_boot_time=0, transform=lambda x: x / 60.0)
    make_optimum_plot('Optimum Queue Time and Utilization (50 builds / hour) ', 'build_run_time', 'Build time (m)',
                      build_times,  cheap_machine_minima, axes, suffix='_cheap', **kwargs)
    make_optimum_plot('Optimum Queue Time and Utilization (50 builds / hour, 2 m4.10xls / build)', 'build_run_time', 'Build time (m)',
                      build_times, expensive_machine_minima, axes, suffix='_expensive', **kwargs)


def make_slow_expensive_plots():
    # A few remaining traffic patterns for comparison against autoscaling
    configs = [
        {'sizes': range(1, 10), 'opts': {'build_run_time': 2400, 'builds_per_hour': 1.0}, 'label': '2400, 1.0', 'color': 'b'},
        {'sizes': range(1, 10), 'opts': {'build_run_time': 2400, 'builds_per_hour': 2.0}, 'label': '2400, 2.0', 'color': 'g'},
        {'sizes': range(3, 20), 'opts': {'build_run_time': 2400, 'builds_per_hour': 5.0}, 'label': '2400, 5.0', 'color': 'r'},
        {'sizes': range(5, 20), 'opts': {'build_run_time': 2400, 'builds_per_hour': 10.0}, 'label': '2400, 10.0', 'color': 'c'},
        {'sizes': range(10, 35), 'opts': {'build_run_time': 2400, 'builds_per_hour': 20.0}, 'label': '2400, 20.0', 'color': 'm'},
        {'sizes': range(1, 5), 'opts': {'build_run_time': 60, 'builds_per_hour': 2.0}, 'label': '60, 2.0', 'color': 'y'},
        {'sizes': range(1, 5), 'opts': {'build_run_time': 120, 'builds_per_hour':2.0}, 'label': '120, 2.0', 'color': 'k'},
        {'sizes': range(1, 5), 'opts': {'build_run_time': 300, 'builds_per_hour':2.0}, 'label': '300, 2.0', 'color': '#3BC1ED'},
        {'sizes': range(1, 5), 'opts': {'build_run_time': 600, 'builds_per_hour':2.0}, 'label': '600, 2.0', 'color': '#DB3BED'},
        {'sizes': range(1, 10), 'opts': {'build_run_time': 1200, 'builds_per_hour':2.0}, 'label': '1200, 2.0', 'color': '#EDAC3B'}
    ]
    ticks = 100000
    make_cost_plot('Slow builds on expensive machines', configs, [0, 25, 0, ticks * 15 / 100], 'plots/slow_expensive',
                    cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR_EXPENSIVE, ticks=ticks, sec_per_tick=10)
    # Sample output: [(3, 3225.3252211111112), (4, 4438.0749250000008), (7, 6656.2780833333336), (12, 9168.3627800000013), (21, 12324.942074444445), (1, 1295.5906672222222), (1, 1349.7570200000002), (1, 2014.3006411111114), (2, 2482.265152777778), (3, 3310.3028344444442)]


def make_cheap_dev_plot():
    # What would the optimum be with a static fleet if developer time was dirt cheap?
    configs = [
        {'sizes': range(1, 25), 'opts': {'cost_per_dev_hour': 0.01}, 'label': '1 cent / hr', 'color': 'k'},
        {'sizes': range(1, 25), 'opts': {'cost_per_dev_hour': 0.1}, 'label': '10 cents / hr', 'color': 'b'},
        {'sizes': range(1, 25), 'opts': {'cost_per_dev_hour': 1}, 'label': '1 dollar / hr', 'color': 'g'},
        {'sizes': range(1, 25), 'opts': {'cost_per_dev_hour': 10}, 'label': '10 dollars / hr', 'color': 'r'},
    ]
    ticks = 100000
    make_cost_plot('Cheap Developers', configs, [0, 25, 0, ticks * 1 / 10], 'plots/cheap_devs',
                   cost_per_builder_hour=cost.COST_PER_BUILDER_HOUR_EXPENSIVE,
                   build_run_time=300, builds_per_hour=50.0, ticks=ticks, sec_per_tick=10)

def make_optimum_cheap_dev_plot():
    costs = [0.01, 0.1, 1, 10, 200]
    minima = [(4, 703.05473555555545), (5, 1114.5530394444445), (5, 1527.2476744444443), (6, 3525.6164516666663), (9, 7243.547856666667)]
    make_optimum_plot('Optimum Queue Time and Utilitization (50 builds / hr, 5 min / build)', 'cost_per_dev_hour',
                      'log_10($ / developer hr)',
                      costs, minima, ([-3, 3, 0, 6], [-3, 3, 0, 110]), transform=lambda x: log(x, 10),
                      ticks=100000, builds_per_hour=50.0, build_run_time=300, log_scale=True, loc='lower left')


def make_optimum_capacity_v_load_plot():
    loads = [ (build_time / 3600.0) * traffic for build_time, traffic, _ in STATIC_MINIMA_LIMITED]
    capacities = [ fleet_size for _, _, fleet_size in STATIC_MINIMA_LIMITED]
    plt.plot(loads, [capacity / load for capacity, load in zip(capacities, loads)], 'bo')
    plt_save('plots/optimum_capacity_v_load')


def make_capacity_plot():
    make_scaling_plot({'build_run_time': 300, 'builds_per_hour': 50.0,
                       'initial_builder_count': 12, 'ticks': 101, 'sec_per_tick': 60},
                      'Optimal Machine Usage for 5 min builds, 50 builds / hr',
                      'plots/static_capacity')



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
    print 'Slow and Expensive'
    make_slow_expensive_plots()
    print 'Cheap Devs'
    make_cheap_dev_plot()
    make_optimum_cheap_dev_plot()
    print 'Capacity Plots'
    make_optimum_capacity_v_load_plot()
    make_capacity_plot()
