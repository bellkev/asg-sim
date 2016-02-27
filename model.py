from collections import deque
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean, std, percentile
from numpy.random import poisson


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

def run_model(ticks, **kwargs):
    m = Model(**kwargs)
    for i in range(ticks):
        m.advance()
    return m

def make_resolution_plot(ticks, resolution):
    theoretical_series = []
    measured_series = []
    run_times = range(1, 30)

    for run_time in run_times:
        per_hour = 60.0 / float(run_time) * 0.3
        m = run_model(ticks, build_run_time=(run_time * 60), builds_per_hour=per_hour, sec_per_tick=resolution)
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
    make_resolution_plot(1000000, 60)
    make_resolution_plot(1000000, 10)

def make_queue_time_v_utilization_plot():
    counts = range(84,100)
    means = []
    p95s = []
    utilizations = []

    for count in counts:
        m = run_model(100000, build_run_time=300, builds_per_hour=1000.0, initial_builder_count=count, builder_boot_time=0)
        means.append(m.mean_queue_time() / 60.0)
        p95s.append(m.percentile_queue_time(95.0) / 60.0)
        utilizations.append(m.mean_percent_utilization())

    plt.title('Processing 1000 5min Builds / Hr')
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    m_handle, = ax1.plot(counts, means, 'g-', label='Mean Queue Time (m)')
    p_handle, = ax1.plot(counts, p95s, 'r-', label='p95 Queue Time (m)')
    ax1.set_xlabel('Fleet Size')
    ax1.set_ylabel('Time (m)')

    ax2 = ax1.twinx()
    u_handle, = ax2.plot(counts, utilizations, 'b-', label='% Builder Utilization')
    ax2.set_ylabel('% Utilization')
    plt.legend(handles=[m_handle, p_handle, u_handle])
    plt.savefig('plots/queue_time_v_utilization')
