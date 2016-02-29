from collections import deque

from numpy import mean, percentile
from numpy.random import poisson


class ScalingPolicy(object):

    # Only ChangeInCapacity is implemented

    def __init__(self, change, cooldown):
        self.change = change
        # both last_fired_time and cooldown are in "ticks"
        self.last_fired_time = 0
        self.cooldown = cooldown

    def maybe_scale(self, time):
        if (self.last_fired_time + self.cooldown) <= time:
            self.last_fired_time = time
            return self.change
        else:
            return 0


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
        self.period_duration = period_duration # in "ticks"
        self.period_count = period_count

    def averaged_metric(self):
        length = len(self.metric)
        phase = length % self.period_duration
        unaveraged = self.metric[-(self.period_count * self.period_duration + phase):(length - phase)]
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
        if len(self.metric) < (self.period_count * self.period_duration):
            return self.OK
        elif all([self.value_not_ok(v) for v in self.averaged_metric()]):
            return self.ALARM
        else:
            return self.OK


class Build(object):

    def __init__(self, queued_time, run_time):
        # All times in "ticks"
        self.queued_time = queued_time
        self.run_time = run_time
        self.started_time = None
        self.finished_time = None


class Builder(object):

    def __init__(self, booted_time, boot_time):
        # All times in "ticks"
        self.booted_time = booted_time
        self.boot_time = boot_time
        self.build = None
        self.shutting_down = False

    def available(self, current_time):
        return not self.shutting_down and not self.build and (self.booted_time + self.boot_time) <= current_time



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

    _defaults = dict(builds_per_hour=10.0, build_run_time=300, initial_builder_count=1,
                     builder_boot_time=300, sec_per_tick=10, autoscale=False)

    def __init__(self, **kwargs):

        # Config
        self.__dict__.update(self._defaults)
        self.__dict__.update(**kwargs)
        self.builds_per_tick = self.builds_per_hour / 3600.0 * float(self.sec_per_tick)
        self.build_run_time_ticks = self.build_run_time / self.sec_per_tick
        self.builder_boot_time_ticks = self.builder_boot_time / self.sec_per_tick

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

        # Autoscaling
        if self.autoscale:
            self.scale_up_alarm = Alarm(self.builders_available,
                                        self.scale_up_threshold, Alarm.LT,
                                        self.alarm_period_duration / self.sec_per_tick,
                                        self.alarm_period_count)
            self.scale_up_policy = ScalingPolicy(self.scale_up_change, self.builder_boot_time_ticks + self.alarm_period_duration)

        # Boot initial builders
        self.boot_builders(self.initial_builder_count)

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

    def boot_builders(self, count):
        for b in range(count):
            self.builders.add(Builder(self.ticks, self.builder_boot_time_ticks))

    def shutdown_builders(self, count):
        shutdown = 0
        for b in self.builders:
            if not b.shutting_down:
                b.shutting_down = True
                shutdown += 1
            if shutdown >= count:
                break

    def power_off_builders(self):
        to_power_off = [b for b in self.builders if b.shutting_down and not b.build]
        for b in to_power_off:
            self.builders.remove(b)

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
                builder.build.finished_time = self.ticks
                self.finished_builds.append(builder.build)
                builder.build = None

    def update_metrics(self):
        self.builders_available.append(len([b for b in self.builders if b.available(self.ticks)]))
        self.builders_in_use.append(len([b for b in self.builders if b.build]))
        self.builders_total.append(len(self.builders))
        self.build_queue_length.append(len(self.build_queue))

    def scale(self):
        if self.scale_up_alarm.state() == Alarm.ALARM:
            new_count = self.scale_up_policy.maybe_scale(self.ticks)
            self.boot_builders(new_count)

    def advance(self, ticks):
        for i in range(ticks):
            self.queue_builds()
            self.finish_builds()
            self.start_builds()
            self.update_metrics()
            if self.autoscale:
                self.scale()
            self.power_off_builders()
            self.ticks += 1

def run_model(ticks=0, **kwargs):
    m = Model(**kwargs)
    m.advance(ticks)
    return m
