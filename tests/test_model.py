import unittest

from asgsim.model import Model, Builder, Build, Alarm, ScalingPolicy, run_model


def test_utilization():
    m = Model(build_run_time=100, builder_boot_time=100,
              builds_per_hour=0.0, sec_per_tick=1, initial_builder_count=2)
    m.advance(200)
    m.build_queue.append(Build(m.ticks, m.build_run_time))
    m.advance(200)
    assert m.mean_percent_utilization() == 12.5
    assert m.mean_unused_builders() == 1.75


def test_scale_up():
    m = Model(build_run_time=50, builder_boot_time=100,
              builds_per_hour=0.0, sec_per_tick=10,
              initial_builder_count=2, autoscale=True,
              alarm_period_duration=10, scale_up_alarm_period_count=3,
              scale_down_alarm_period_count=1000,
              scale_up_threshold=5, scale_up_change=2,
              scale_down_threshold=8, scale_down_change=1)
    assert len(m.builders) == 2
    m.advance(3)
    # No scaling until alarm has enough data
    assert len(m.builders) == 2
    m.advance(1)
    assert len(m.builders) == 4
    # Scale after one cooldown
    # (ideal cooldown of builder_boot_time + alarm_period_duration)
    m.advance(11)
    assert len(m.builders) == 6
    # But no more
    m.advance(11)
    assert len(m.builders) == 6


def test_graceful_shutdown():
    m = Model(build_run_time=10, builder_boot_time=0,
              builds_per_hour=0.0, sec_per_tick=1,
              initial_builder_count=2)
    assert len(m.builders) == 2
    m.build_queue.append(Build(m.ticks, m.build_run_time))
    m.advance(5)
    m.shutdown_builders(2)
    m.advance(6)
    assert len(m.builders) == 0
    finished = m.finished_builds[0]
    assert (finished.finished_time - finished.started_time) == m.build_run_time


def test_scale_down():
    m = Model(build_run_time=50, builder_boot_time=0,
              builds_per_hour=0.0, sec_per_tick=1,
              initial_builder_count=10, autoscale=True,
              alarm_period_duration=10, scale_up_alarm_period_count=1000,
              scale_down_alarm_period_count=3,
              scale_up_threshold=5, scale_up_change=2,
              scale_down_threshold=8, scale_down_change=1)
    # opposite of test_scale_up
    assert len(m.builders) == 10
    m.advance(30)
    assert len(m.builders) == 10
    m.advance(1)
    assert len(m.builders) == 9
    m.advance(60)
    assert len(m.builders) == 8
    m.advance(60)
    assert len(m.builders) == 8


def test_build_throughput():
    m = Model(builds_per_hour=0.0, build_run_time=10, sec_per_tick=1,
              initial_builder_count=1, builder_boot_time=0)
    builds = [Build(m.ticks, m.build_run_time), Build(m.ticks, m.build_run_time)]
    m.build_queue.extend(builds)
    m.advance(20)
    assert (builds[1].started_time - builds[1].queued_time) == m.build_run_time


def test_fixed_builds_per_hour():
    m = Model(ticks=3600, builds_per_hour=100.0, build_run_time=10, sec_per_tick=10,
              initial_builder_count=100, builder_boot_time=0)
    assert m.current_builds_per_hour() == 100.0


def test_fixed_builds_per_hour_e2e():
    # This test is not deterministic, but random fails should be extremely rare
    m = run_model(ticks=3600, builds_per_hour=100.0, build_run_time=10, sec_per_tick=10,
                  initial_builder_count=100, builder_boot_time=0)
    assert 900 < len(m.finished_builds) < 1100


def test_dynamic_builds_per_hour():
    m = Model(builds_per_hour=100.0, build_run_time=10, sec_per_tick=3600,
              initial_builder_count=100, builder_boot_time=0, builds_per_hour_fn=Model.SINE)
    assert m.current_builds_per_hour() == 0.0
    m.advance(12)
    print 'ticks:', m.ticks
    print 'per hour:', m.current_builds_per_hour()
    assert m.current_builds_per_hour() == 100.0
    m.advance(12)
    assert m.current_builds_per_hour() == 0.0


def test_dynamic_builds_per_hour_e2e():
    # This test is not deterministic, but random fails should be extremely rare
    m = Model(builds_per_hour=6000.0, build_run_time=10, sec_per_tick=60,
              initial_builder_count=100, builder_boot_time=0, builds_per_hour_fn=Model.SINE)
    m.advance(1)
    assert len(m.finished_builds) < 10
    m.advance(720) # 12 hrs
    initial =len(m.finished_builds)
    m.advance(1)
    final = len(m.finished_builds)
    assert 90 < final - initial < 110


def test_initial_boot():
    m = run_model(ticks=50, builds_per_hour=0.0, sec_per_tick=1,
                  initial_builder_count=1, builder_boot_time=100)
    assert m.builders.pop().available(m.ticks) == True


def test_run_model():
    m = run_model(ticks=100)
    assert m.ticks == 100


class TestBuilder(unittest.TestCase):

    def setUp(self):
        self.builder = Builder(0, 10)

    def test_available(self):
        assert self.builder.available(15)

    def test_not_available_if_booting(self):
        assert self.builder.available(5) == False

    def test_not_available_if_busy(self):
        self.builder.build = Build(5, 20)
        assert self.builder.available(15) == False

    def test_not_available_if_shutting_down(self):
        self.builder.shutting_down = True
        assert self.builder.available(15) == False


class TestAlarm(unittest.TestCase):

    def setUp(self):
        self.metric = []
        self.alarm = Alarm(self.metric, 5, Alarm.GT, 1, 3)

    def test_initial_state(self):
        assert self.alarm.state() == Alarm.OK
        self.metric.append(4)
        assert self.alarm.state() == Alarm.OK

    def test_ok(self):
        self.metric.extend([5,5,5,5])
        assert self.alarm.state() == Alarm.OK

    def test_alarm(self):
        self.metric.extend([6,6])
        assert self.alarm.state() == Alarm.OK
        self.metric.append(6)
        assert self.alarm.state() == Alarm.ALARM

    def test_continued_alarm(self):
        self.metric.extend([6,6,6,6,6,6,6,6])
        assert self.alarm.state() == Alarm.ALARM

    def test_reset(self):
        self.metric.extend([6,6,6,6,6,6,6,1])
        assert self.alarm.state() == Alarm.OK

    def test_comparisons_GT(self):
        self.alarm.metric = [6,6,6]
        assert self.alarm.state() == Alarm.ALARM

    def test_comparisons_LT(self):
        self.alarm.comparison = Alarm.LT
        self.alarm.metric = [4,4,4]
        assert self.alarm.state() == Alarm.ALARM

    def test_initial_averaged(self):
        self.alarm.period_duration = 3
        self.metric.extend([9,9,9,9,9])
        assert self.alarm.state() == Alarm.OK

    def test_ok_averaged(self):
        self.alarm.period_duration = 3
        # Each period mean is 5 == threshold
        self.metric.extend([0,5,10,0,5,10,0,5,10,0,5,10])
        assert self.alarm.state() == Alarm.OK

    def test_alarm_averaged(self):
        self.alarm.period_duration = 3
        # Each period mean is 5.33 (> threshold)
        self.metric.extend([0,5,11,0,5,11,0,5,11,0,5,11])
        assert self.alarm.state() == Alarm.ALARM

    def test_periodized_average(self):
        self.alarm.period_duration = 3
        self.metric.extend([6,6,6,6,6,6,6,6,6,6,6,6,0,0])
        assert self.alarm.state() == Alarm.ALARM


class TestScalingPolicy(unittest.TestCase):

    def setUp(self):
        self.policy = ScalingPolicy(2, 5)

    def test_cooldown(self):
        assert self.policy.maybe_scale(0) == 2
        assert self.policy.maybe_scale(4) == 0
        assert self.policy.maybe_scale(5) == 2
