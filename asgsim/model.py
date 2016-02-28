from collections import deque

from numpy import mean, percentile
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
    # Only the average metric is implemented.

    LT=0
    GT=1
    OK=2
    ALARM=3

    def __init__(self, metric, threshold, comparison, period_duration, period_count):
        self.metric = metric
        self.threshold = threshold
        self.comparison = comparison
        self.period_duration = period_duration
        self.period_count = period_count

    def averaged_metric(self):
        unaveraged = self.metric[-(self.period_count * self.period_duration):]
        averaged = []
        for period in range(self.period_count):
            start = period * self.period_duration
            end = start + self.period_duration
            averaged.append(mean(unaveraged[start:end]))
        return averaged

    def value_not_ok(self, value):
        if self.comparison == self.LT:
            return value < self.threshold
        if self.comparison == self.GT:
            return value > self.threshold

    def state(self):
        # Apparently alarms return immediately (in one period) to OK
        if len(self.metric) < self.period_count * self.period_duration:
            return self.OK
        elif all([self.value_not_ok(v) for v in self.averaged_metric()]):
            return self.ALARM
        else:
            return self.OK


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
    - Only one type of build: Every build takes exactly the same integer number
      of seconds
    - Assumes traffic is random: Random traffic is generated according to a
      Poisson distribution, so things like end-of-day spikes in commits or strings
      of repeated builds to detect flaky tests will not be modeled
    """
    def __init__(self, builds_per_hour=10.0, build_run_time=300,
                 initial_builder_count=1, builder_boot_time=300, sec_per_tick=10):

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
        self.builders_in_use = []
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
        return mean([float(u) / float(t) for u, t in zip(self.builders_in_use, self.builders_total)]) * 100.0

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
        self.builders_in_use.append(len([b for b in self.builders if b.build]))
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
