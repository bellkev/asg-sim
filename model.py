from collections import deque
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean
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
    Current significant simplifications are:
    - No containers: Only one build at a time per builder
    - Only one type of build: Every build takes exactly the same integer number of minutes
    - Time resolution of minutes: Builds must take at least 1 minute, boot time, etc must be integer numbers of minutes
    """
    def __init__(self, builds_per_hour=10.0, build_run_time=5, initial_builder_count=1, builder_boot_time=5):

        # Config
        self.builds_per_hour = builds_per_hour
        self.builds_per_min = self.builds_per_hour / 60.0
        self.build_run_time = build_run_time
        self.initial_builder_count = initial_builder_count
        self.builder_boot_time = builder_boot_time

        # Core model state
        self.time = 0
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
        """
        assert self.initial_builder_count == 1

        u = 1.0 / float(self.build_run_time)
        l = self.builds_per_min
        r = l / u
        return (1 / (2 * u)) * (r / (1 - r))


    def make_builder(self):
        return Builder(self.time, self.builder_boot_time)

    def queue_builds(self):
        n = poisson(self.builds_per_min)
        for b in range(n):
            self.build_queue.append(Build(self.time, self.build_run_time))

    def start_builds(self):
        for builder in self.builders:
            if not self.build_queue:
                break
            elif builder.available(self.time):
                builder.build = self.build_queue.popleft()
                builder.build.started_time = self.time

    def finish_builds(self):
        for builder in self.builders:
            if builder.build and (builder.build.started_time + builder.build.run_time) <= self.time:
                self.finished_builds.append(builder.build)
                builder.build = None

    def update_metrics(self):
        self.builders_available.append(len([b for b in self.builders if b.available(self.time)]))
        self.build_queue_length.append(len(self.build_queue))

    def advance(self):
        self.queue_builds()
        self.start_builds()
        self.finish_builds()
        self.update_metrics()
        # scale
        self.time += 1

m = Model(builds_per_hour=0.25, build_run_time=120, builder_boot_time=0)
for i in range(20000000):
    if i % 1000000 == 0:
        print len(m.build_queue)
    m.advance()

print "Theoretical mean queue time:", m.theoretical_queue_time()
print "Measured mean queue time:", mean([(b.started_time - b.queued_time) for b in m.finished_builds])
