from asgsim.model import Model, Build


def test_utilization():
    m = Model(build_run_time=100, builder_boot_time=100,
              builds_per_hour=0.0, sec_per_tick=1, initial_builder_count=2)
    for i in range(200): m.advance()
    m.build_queue.append(Build(m.ticks, 100))
    for i in range(200): m.advance()
    assert m.mean_percent_utilization() == 12.5
