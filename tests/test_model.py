import unittest

from asgsim.model import Model, Build, Alarm


def test_utilization():
    m = Model(build_run_time=100, builder_boot_time=100,
              builds_per_hour=0.0, sec_per_tick=1, initial_builder_count=2)
    for i in range(200): m.advance()
    m.build_queue.append(Build(m.ticks, 100))
    for i in range(200): m.advance()
    assert m.mean_percent_utilization() == 12.5


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

    def test_comparisons(self):
        self.alarm.metric = [6,6,6]
        assert self.alarm.state() == Alarm.ALARM
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
