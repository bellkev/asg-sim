from asgsim.cost import cost


def test_cost_machines():
    cost_per_builder_hour = 0.12 # m4.large on-demand price
    measured = cost({'builder_boot_time': 0,
                     'builds_per_hour': 0.0,
                     'initial_build_count': 1,
                     'build_run_time': 3600,
                     'initial_builder_count': 1,
                     'sec_per_tick': 3600,
                     'ticks': 10})
    # build running for one cycle, zero queue time
    expected = cost_per_builder_hour * 9
    print 'measured:', measured
    print 'expected:', expected
    assert measured == expected

def test_cost_queueing():
    # TODO: Look into using total queued time to compute cost
    cost_per_dev_hour = 200
    cost_per_builder_hour = 0.12 # m4.large on-demand price
    measured = cost({'builder_boot_time': 0,
                     'builds_per_hour': 1.0,
                     'initial_build_count': 10,
                     'build_run_time': 3600,
                     'initial_builder_count': 1,
                     'sec_per_tick': 3600,
                     'ticks': 4})
    # 3 builds will finish, no free machines, queue times are 0, 1, 2 hrs
    mean_queue_time = (0 + 1 + 2) / 3.0
    expected = cost_per_dev_hour * mean_queue_time * 4
    print 'measured:', measured
    print 'expected:', expected
    assert measured == expected
