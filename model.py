from collections import deque
from multiprocessing import Pool

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean, std, percentile
from numpy.random import poisson, rand


class Policy(object):

    def __init__(self, alarm, last_fired_time, cooldown):
        self.alarm = alarm
        self.last_fired_time = last_fired_time
        self.cooldown = cooldown


class Alarm(object):

    # Skip INSUFFICIENT_DATA because we'll always
    # have data and OK is equivalent to start with.
    # Also no need to track state transitions since
    # scaling policies re-fire on every ALARM period.
    OK = 0
    ALARM = 1

    def __init__(self, metric, threshold, comparison, period):
        self.metric = metric
        self.state = OK

    def state(self):
        recent_mean = mean(metric[-period:], threshold)
        # Apparently alarms return immediately (in one period) to OK
        if comparison(recent_mean, threshold) and comparison(metric[-1], threshold):
            return ALARM
        else:
            return OK


class Build(object):

    def __init__(self, queued_time, run_time):
        self.queued_time = queued_time
        self.run_time = run_time
        self.started_time = None


class Builder(object):

    def __init__(self, booted_time, boot_time):
        self.booted_time = booted_time
        self.boot_time = boot_time
        self.build = None

    def available(self, current_time):
        return not self.build and (self.booted_time + self.boot_time) <= current_time



class Model(object):
    """
    A simplified model of a CircleCI Enterprise builder ASG.

    builds_per_hour is a float of builds/hour
    build_run_time and builder_boot_time are integer numbers of seconds

    Current significant simplifications are:
    - No containers: Only one build at a time per builder
    - Only one type of build: Every build takes exactly the same integer number of seconds
    """
    def __init__(self, builds_per_hour=10.0, build_run_time=300, initial_builder_count=1, builder_boot_time=300, sec_per_tick=10):

        # Config
        self.sec_per_tick = sec_per_tick
        self.builds_per_hour = builds_per_hour
        self.builds_per_tick = self.builds_per_hour / 3600.0 * float(self.sec_per_tick)
        self.build_run_time_secs = build_run_time
        self.build_run_time_ticks = self.build_run_time_secs / self.sec_per_tick
        self.builder_boot_time_secs = builder_boot_time
        self.builder_boot_time_ticks = self.builder_boot_time_secs / self.sec_per_tick
        self.initial_builder_count = initial_builder_count

        # Core model state
        self.ticks = 0
        self.builders = set()
        self.build_queue = deque()
        self.finished_builds = []

        # Metrics
        self.builders_available = []
        self.builders_total = []
        self.build_queue_length = []

        # Boot initial builders
        for b in range(self.initial_builder_count):
            self.builders.add(self.make_builder())

    def theoretical_queue_time(self):
        """
        Theoretical mean queue time for an M/D/1 queue:
        https://en.wikipedia.org/wiki/M/D/1_queue
        In units of seconds.
        """
        assert self.initial_builder_count == 1

        u = 1.0 / float(self.build_run_time_ticks)
        l = self.builds_per_tick
        r = l / u
        return (1 / (2 * u)) * (r / (1 - r)) * self.sec_per_tick

    def queue_times(self):
        return [(b.started_time - b.queued_time) * self.sec_per_tick for b in self.finished_builds]

    def mean_queue_time(self):
        return mean(self.queue_times())

    def percentile_queue_time(self, ptile):
        return percentile(self.queue_times(), ptile)

    def mean_percent_utilization(self):
        return mean([float(t - a) / float(t) for a, t in zip(self.builders_available, self.builders_total)]) * 100.0

    def make_builder(self):
        return Builder(self.ticks, self.builder_boot_time_ticks)

    def queue_builds(self):
        n = poisson(self.builds_per_tick)
        for b in range(n):
            self.build_queue.append(Build(self.ticks, self.build_run_time_ticks))

    def start_builds(self):
        for builder in self.builders:
            if not self.build_queue:
                break
            elif builder.available(self.ticks):
                builder.build = self.build_queue.popleft()
                builder.build.started_time = self.ticks

    def finish_builds(self):
        for builder in self.builders:
            if builder.build and (builder.build.started_time + builder.build.run_time) <= self.ticks:
                self.finished_builds.append(builder.build)
                builder.build = None

    def update_metrics(self):
        self.builders_available.append(len([b for b in self.builders if b.available(self.ticks)]))
        self.builders_total.append(len(self.builders))
        self.build_queue_length.append(len(self.build_queue))

    def advance(self):
        self.queue_builds()
        self.finish_builds()
        self.start_builds()
        self.update_metrics()
        # scale
        self.ticks += 1

def run_model(ticks=0, **kwargs):
    m = Model(**kwargs)
    for i in range(ticks):
        m.advance()
    return m

def make_resolution_plot(resolution):
    theoretical_series = []
    measured_series = []
    run_times = range(1, 30)

    for run_time in run_times:
        per_hour = 60.0 / float(run_time) * 0.3
        m = run_model(ticks=1000000, build_run_time=(run_time * 60), builds_per_hour=per_hour, sec_per_tick=resolution)
        theoretical_series.append(m.theoretical_queue_time() / 60.0)
        measured_series.append(m.measured_queue_time() / 60.0)

    plt.title('Queue Times at 0.3X Capacity @ %d sec/tick' % resolution)
    plt.xlabel('Build Time (m)')
    plt.ylabel('Mean Queue Time (m)')
    t_handle, = plt.plot(run_times, theoretical_series, label='Theoretical')
    m_handle, = plt.plot(run_times, measured_series, 'ro', label='Model')
    plt.legend(handles=[t_handle, m_handle])
    plt.savefig('plots/%d_sec_per_tick' % resolution)
    plt.close()

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
    plt.savefig('plots/queue_time_v_utilization')
    plt.close()

def cost(opts):
    # Cost parameters
    sec_per_tick = 10
    cost_per_dev_hour = 100 # a reasonably average contractor rate
    adjusted_cost_per_dev_hour = cost_per_dev_hour * 2 # adjust for a bit of a "concentration loss factor"
    cost_per_builder_hour = 0.12 # m4.large on-demand price
    #cost_per_builder_hour = 4.698 # 2x m4.10xl on-demand price
    builds_per_hour = opts['builds_per_hour']
    ticks = opts['ticks']
    simulation_time_hours = ticks * sec_per_tick / 3600.0

    m = run_model(builder_boot_time=0, sec_per_tick=sec_per_tick, **opts)
    return simulation_time_hours * (mean(m.builders_available) * cost_per_builder_hour
                                    + builds_per_hour * m.mean_queue_time() / 3600.0 * adjusted_cost_per_dev_hour)

def make_cost_curve_plot():
    sizes = range(100,150,10)
    opts = [{'initial_builder_count': size, 'builds_per_hour': 1000.0, 'ticks': 100000} for size in sizes]

    p = Pool(8)
    costs = p.map(cost, opts)

    plt.plot(sizes, costs)
    plt.savefig('plots/cost_curve')

def make_cost_v_traffic_plot():
    configs = [
        {'sizes': range(1, 20), 'builds_per_hour': 10.0, 'color': 'b'},
        {'sizes': range(5, 25), 'builds_per_hour': 50.0, 'color': 'g'},
        {'sizes': range(10, 30), 'builds_per_hour': 100.0, 'color': 'r'},
        {'sizes': range(20, 40), 'builds_per_hour': 200.0, 'color': 'c'},
        {'sizes': range(55, 75), 'builds_per_hour': 500.0, 'color': 'm'},
        {'sizes': range(105, 125), 'builds_per_hour': 1000.0, 'color': 'y'}
    ]
    handles = []
    minima = []
    for config in configs:
        opts = [{'builds_per_hour': config['builds_per_hour'], 'initial_builder_count': size, 'ticks': 100000, 'build_run_time': 300} for size in config['sizes']]
        p = Pool(8)
        costs = p.map(cost, opts)
        handle, = plt.plot(config['sizes'], costs, config['color'] + '-', label='%.0f builds per hour' % config['builds_per_hour'])
        handles.append(handle)
        min_cost = min(costs)
        min_size = config['sizes'][costs.index(min_cost)]
        minima.append((min_size, min_cost))
    plt.axis([0, 150, 0, 3000])
    plt.plot([m[0] for m in minima], [m[1] for m in minima], 'ko')
    plt.legend(handles=handles)
    plt.savefig('plots/cost_v_traffic')
    plt.close()
    print 'Optimal fleet sizes:', minima

def make_optimum_traffic_plot():
    # Results from make_cost_v_traffic_plot
    traffics = [10.0, 50.0, 100.0, 200.0, 500.0, 1000.0]
    minima = [(5, 148.60742284519017), (12, 273.10872866896966), (19, 373.08614927744213), (31, 510.3652143839505), (62, 787.45517381445654), (113, 1065.6187933792928)]
    utilizations = []
    means = []

    for traffic, minimum in zip(traffics, minima):
        size, cost = minimum
        m = run_model(ticks=100000, builds_per_hour=traffic, build_run_time=300, builder_boot_time=0, sec_per_tick=10, initial_builder_count=size)
        utilizations.append(m.mean_percent_utilization())
        means.append(m.mean_queue_time())

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.set_title('Optimum Queue Time and Utilization (5 min builds)')
    m_handle, = ax1.plot(traffics, means, 'go', label='Mean Queue Time (s)')
    ax1.axis([10, 1200, 0, 2])
    ax1.set_xlabel('Builds per Hour')
    ax1.set_ylabel('Time (s)')

    ax2 = ax1.twinx()
    u_handle, = ax2.plot(traffics, utilizations, 'bo', label='% Builder Utilization')
    ax2.axis([10, 1200, 0, 100])
    ax2.set_ylabel('% Utilization')
    plt.legend(handles=[m_handle, u_handle])
    plt.savefig('plots/optimum_props_by_traffic')
    plt.close()

def make_cost_v_build_time_plot():
    configs = [
        {'sizes': range(1, 20), 'build_run_time': 30, 'color': 'b'},
        {'sizes': range(1, 20), 'build_run_time': 60, 'color': 'g'},
        {'sizes': range(1, 20), 'build_run_time': 120, 'color': 'r'},
        {'sizes': range(5, 25), 'build_run_time': 300, 'color': 'c'},
        {'sizes': range(15, 35), 'build_run_time': 600, 'color': 'm'},
        {'sizes': range(25, 45), 'build_run_time': 1200, 'color': 'y'},
        {'sizes': range(45, 65), 'build_run_time': 2400, 'color': 'k'}
    ]
    handles = []
    minima = []
    p = Pool(8)
    for config in configs:
        opts = [{'build_run_time': config['build_run_time'], 'initial_builder_count': size, 'ticks': 100000, 'builds_per_hour': 50.0} for size in config['sizes']]
        costs = p.map(cost, opts)
        if 'build_run_time' == 30:
            lab = '30 sec builds'
        else:
            lab = '%d min builds' % (config['build_run_time'] / 60)
        handle, = plt.plot(config['sizes'], costs, config['color'] + '-', label=lab)
        handles.append(handle)
        min_cost = min(costs)
        min_size = config['sizes'][costs.index(min_cost)]
        minima.append((min_size, min_cost))
    plt.axis([0, 90, 0, 1000])
    plt.plot([m[0] for m in minima], [m[1] for m in minima], 'ko')
    plt.legend(handles=handles)
    plt.savefig('plots/cost_v_build_time')
    plt.close()
    print 'Optimal fleet sizes:', minima

def make_optimum_build_time_plot():
    # Results from make_cost_v_build_time_plot
    build_times = [30, 60, 120, 300, 600, 1200, 2400]
    plot_times = [bt / 60.0 for bt in build_times]
    minima = [(4, 122.30462423397832), (5, 156.90400521723009), (7, 186.61573547738476), (12, 266.18263968907178), (19, 391.88402948743175), (31, 497.54767044356112), (55, 739.6319964639514)]
    utilizations = []
    means = []

    for build_time, minimum in zip(build_times, minima):
        size, cost = minimum
        m = run_model(ticks=100000, builds_per_hour=50.0, build_run_time=build_time, builder_boot_time=0, sec_per_tick=10, initial_builder_count=size)
        utilizations.append(m.mean_percent_utilization())
        means.append(m.mean_queue_time())

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.set_title('Optimum Queue Time and Utilization (50 builds / hour)')
    m_handle, = ax1.plot(plot_times, means, 'go', label='Mean Queue Time (s)')
    ax1.axis([0, 60, 0, 2])
    ax1.set_xlabel('Build Time (m)')
    ax1.set_ylabel('Time (s)')

    ax2 = ax1.twinx()
    u_handle, = ax2.plot(plot_times, utilizations, 'bo', label='% Builder Utilization')
    ax2.axis([0, 60, 0, 100])
    ax2.set_ylabel('% Utilization')
    plt.legend(handles=[m_handle, u_handle])
    plt.savefig('plots/optimum_props_by_build_time')
    plt.close()



if __name__ == '__main__':
    make_optimum_build_time_plot()
