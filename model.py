from collections import deque
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean, std
from numpy.random import poisson


class Policy(object):

    def __init__(self, alarm, last_fired_time, cooldown_time):
        self.alarm = alarm
        self.last_fired_time = last_fired_time
        self.cooldown_time = cooldown_time


class Alarm(object):

    # Skip INSUFFICIENT_DATA because we'll always
    # have data and OK is equivalent to start with.
    # Also no need to track state transitions since
    # scaling policies re-fire on every ALARM period.
    OK = 0
    ALARM = 1

    def __init__(self, metric, threshold, comparison, periods_time):
        self.metric = metric
        self.state = OK

    def state(self):
        recent_mean = mean(metric[-periods_time:], threshold)
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
    def __init__(self, builds_per_hour=10.0, build_run_time=300, initial_builder_count=1, builder_boot_time=300, sec_per_tick=5):

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

    def mean_queue_time(self):
        return mean([(b.started_time - b.queued_time) for b in self.finished_builds]) * self.sec_per_tick

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
        self.build_queue_length.append(len(self.build_queue))

    def advance(self):
        self.queue_builds()
        self.start_builds()
        self.finish_builds()
        self.update_metrics()
        # scale
        self.ticks += 1

def run_model(ticks, **kwargs):
    m = Model(**kwargs)
    for i in range(ticks):
        m.advance()
    return m.theoretical_queue_time() / 60.0, m.mean_queue_time() / 60.0

def make_resolution_plot(ticks, resolution):
    theoretical_series = []
    measured_series = []
    run_times = range(1, 30)

    for run_time in run_times:
        per_hour = 60.0 / float(run_time) * 0.3
        theoretical, measured = run_model(ticks, build_run_time=(run_time * 60), builds_per_hour=per_hour, sec_per_tick=resolution)
        theoretical_series.append(theoretical)
        measured_series.append(measured)

    plt.title('Queue Times at 0.3X Capacity')
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

make_resolution_plots()
