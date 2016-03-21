from collections import deque
from math import sin, pi

from numpy import percentile
from numpy.random import poisson


# Seems to perform better than numpy.mean for averaging
# small list slices in Alarm, even if they are collected
# in numpy.arrays from the start
def mean(l):
    return sum(l) / float(len(l))


class ScalingPolicy(object):

    # Only ChangeInCapacity is implemented

    def __init__(self, change, cooldown):
        self.change = change
        # both last_fired_time and cooldown are in "ticks"
        self.last_fired_time = -cooldown
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
        self.averages = []
        self.threshold = threshold
        self.comparison = comparison
        self.period_duration = period_duration # in "ticks"
        self.period_count = period_count

    def averaged_metric(self):
        while len(self.averages) < len(self.metric) / self.period_duration:
            start = len(self.averages) * self.period_duration
            end = start + self.period_duration
            self.averages.append(mean(self.metric[start:end]))
        return self.averages[-self.period_count:]

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

    Available parameters:

    (all times are integer numbers of seconds unless noted otherwise)

    build_run_time: execution time of every build
    builder_boot_time: boot time of builders (except initial builders)
    builds_per_hour: float of average builds/hour
    builds_per_hour_fn: designates time-varying function with builds_per_hour is multiplied
                        options are Model.CONSTANT (always 1) or Model.SINE (a sinusoid
                        that starts at the minimum of 0 at t=0 and peaks at 1 at t=12hrs)
    initial_builder_count: number of builders to boot instantly at start
    sec_per_tick: number of seconds per "tick" on the model clock
    ticks: number of "ticks" (cycles) to run the model
    autoscale: boolean that enables autoscaling and supports these additional params:
        alarm_period_duration: duration of one (CloudWatch-style) alarm period
        scale_down_alarm_period_count: number of alarm periods metric must be under threshold
                                       before scale down alarm goes off
        scale_down_change: number of instances to turn off for each scale-down event
        scale_down_threshold: number of available instances above which scale-down events fire
        scale_up_alarm_period_count: number of alarm periods metric must be over threshold
                                       before scale down alarm goes off
        scale_up_change: number of instances to start for each scale-up event
        scale_up_threshold: number of available instances below which scale-up events fire

    Current significant simplifications are:
    - No containers: Only one build at a time per builder
    - Only one type of build: Every build takes exactly the same integer number
      of seconds
    - Assumes traffic is random: Random traffic is generated according to a
      Poisson distribution, optionally multiplied by build_per_hour_fn so things like
      repeated builds to detect flaky tests will not be modeled
    """

    CONSTANT = 0
    SINE = 1

    def __init__(self, **kwargs):

        # Config
        defaults = dict(builds_per_hour=10.0, build_run_time=300, initial_builder_count=1,
                        builder_boot_time=300, sec_per_tick=10, autoscale=False,
                        initial_build_count=0, builds_per_hour_fn=self.CONSTANT)
        self.__dict__.update(defaults)
        self.__dict__.update(**kwargs)
        self.build_run_time_ticks = self.build_run_time / self.sec_per_tick
        self.builder_boot_time_ticks = self.builder_boot_time / self.sec_per_tick
        if self.autoscale:
            self.alarm_period_duration_ticks = self.alarm_period_duration / self.sec_per_tick

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
                                        self.alarm_period_duration_ticks,
                                        self.scale_up_alarm_period_count)
            self.scale_down_alarm = Alarm(self.builders_available,
                                        self.scale_down_threshold, Alarm.GT,
                                        self.alarm_period_duration_ticks,
                                        self.scale_down_alarm_period_count)
            self.scale_up_policy = ScalingPolicy(self.scale_up_change, self.builder_boot_time_ticks + self.alarm_period_duration_ticks)
            self.scale_down_policy = ScalingPolicy(self.scale_down_change, self.build_run_time_ticks + self.alarm_period_duration_ticks)

        # Boot initial builders instantly
        self.boot_builders(self.initial_builder_count, instantly=True)

        # Load initial builds (for testing)
        for build in range(self.initial_build_count):
            self.build_queue.append(Build(self.ticks, self.build_run_time_ticks))

    def current_builds_per_hour(self):
        if self.builds_per_hour_fn == self.CONSTANT:
            return self.builds_per_hour
        elif self.builds_per_hour_fn == self.SINE:
            # A sinusoid with 24hr period that is 0 at t=0hr, 1 at t=12hr, 0 at t=24hr
            t = self.ticks * self.sec_per_tick / 3600.0
            w = (2.0 * pi) / 24.0
            p = pi / 2.0
            multiple = 0.5 * sin(w * t - p) + 0.5
            return multiple * self.builds_per_hour

    def builds_per_tick(self):
        return self.current_builds_per_hour() / 3600.0 * self.sec_per_tick

    def theoretical_queue_time(self):
        """
        Theoretical mean queue time for an M/D/1 queue:
        https://en.wikipedia.org/wiki/M/D/1_queue
        In units of seconds.
        """
        assert self.initial_builder_count == 1

        u = 1.0 / float(self.build_run_time_ticks)
        l = self.builds_per_tick()
        r = l / u
        return (1 / (2 * u)) * (r / (1 - r)) * self.sec_per_tick

    def queue_times(self):
        return [(b.started_time - b.queued_time) * self.sec_per_tick for b in self.finished_builds]

    def mean_queue_time(self):
        return mean(self.queue_times())

    def total_queue_time(self):
        return sum(self.queue_times())

    def percentile_queue_time(self, ptile):
        return percentile(self.queue_times(), ptile)

    def mean_percent_utilization(self):
        return mean([float(u) / float(t) for u, t in zip(self.builders_in_use, self.builders_total)]) * 100.0

    def mean_unused_builders(self):
        return mean([t - u for u, t in zip(self.builders_in_use, self.builders_total)])

    def boot_builders(self, count, instantly=False):
        if instantly:
            boot_time = 0
        else:
            boot_time = self.builder_boot_time_ticks
        for b in range(count):
            self.builders.add(Builder(self.ticks, boot_time))

    def shutdown_builders(self, count):
        shutdown = 0
        for b in self.builders:
            if shutdown >= count:
                break
            if not b.shutting_down:
                b.shutting_down = True
                shutdown += 1

    def power_off_builders(self):
        to_power_off = [b for b in self.builders if b.shutting_down and not b.build]
        for b in to_power_off:
            self.builders.remove(b)

    def queue_builds(self):
        n = poisson(self.builds_per_tick())
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
        # Alarm states can only change on new periods
        if self.ticks % self.alarm_period_duration_ticks == 0:
            if self.scale_up_alarm.state() == Alarm.ALARM:
                boot_count = self.scale_up_policy.maybe_scale(self.ticks)
                self.boot_builders(boot_count)
            if self.scale_down_alarm.state() == Alarm.ALARM:
                shutdown_count = self.scale_down_policy.maybe_scale(self.ticks)
                self.shutdown_builders(shutdown_count)

    def advance(self, ticks):
        for i in range(ticks):
            self.queue_builds()
            # finish eagerly to maximize throughput
            self.finish_builds()
            self.start_builds()
            if self.autoscale:
                self.scale()
            self.update_metrics()
            self.power_off_builders()
            self.ticks += 1

def run_model(ticks=0, **kwargs):
    """Takes all the same kwargs as Model constructor + 'ticks' to run."""
    m = Model(**kwargs)
    m.advance(ticks)
    return m
