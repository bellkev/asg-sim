import asgsim.cost
from asgsim.cost import costs_from_job_results, costs, cost_ci, compare_cis, compare_result_cis, compare_result_means


def test_cost_machines():
    measured = costs({'builder_boot_time': 0,
                      'builds_per_hour': 0.0,
                      'initial_build_count': 1,
                      'build_run_time': 3600,
                      'initial_builder_count': 1,
                      'sec_per_tick': 3600,
                      'ticks': 10})[0]
    # build running for one cycle, zero queue time
    expected = asgsim.cost.COST_PER_BUILDER_HOUR * 9
    assert measured == expected


def test_cost_per_builder_hour_override():
    measured = costs({'builder_boot_time': 0,
                      'builds_per_hour': 0.0,
                      'initial_build_count': 1,
                      'build_run_time': 3600,
                      'initial_builder_count': 1,
                      'sec_per_tick': 3600,
                      'ticks': 10,
                      'cost_per_builder_hour':
                      asgsim.cost.COST_PER_BUILDER_HOUR_EXPENSIVE})[0]
    # build running for one cycle, zero queue time
    expected = asgsim.cost.COST_PER_BUILDER_HOUR_EXPENSIVE * 9
    assert measured == expected


def test_cost_queueing():
    measured = costs({'builder_boot_time': 0,
                      'builds_per_hour': 1.0,
                      'initial_build_count': 10,
                      'build_run_time': 3600,
                      'initial_builder_count': 1,
                      'sec_per_tick': 3600,
                      'ticks': 4})[0]
    # 3 builds will finish, no free machines, queue times are 0, 1, 2 hrs
    total_queue_time = 0 + 1 + 2
    expected = asgsim.cost.COST_PER_DEV_HOUR * total_queue_time
    assert measured == expected


def test_costs_from_job_results_mean():
    results = {'input': {'sec_per_tick': 3600, 'builds_per_hour': 1, 'ticks': 1},
               'output': [{'mean_queue_time': 3600, 'mean_unused_builders': 1}]}
    expected = asgsim.cost.COST_PER_BUILDER_HOUR + asgsim.cost.COST_PER_DEV_HOUR
    assert costs_from_job_results(results)[0] == expected


def test_costs_from_job_results_total():
    results = {'input': {'sec_per_tick': 3600,  'ticks': 1},
               'output': [{'total_queue_time': 3600, 'mean_unused_builders': 1}]}
    expected = asgsim.cost.COST_PER_BUILDER_HOUR + asgsim.cost.COST_PER_DEV_HOUR
    assert costs_from_job_results(results)[0] == expected


def test_cost_ci():
    actual = cost_ci({'input': {'sec_per_tick': 3600, 'builds_per_hour': 1, 'ticks': 1},
                      'output': [{'mean_queue_time': 0, 'mean_unused_builders': 1},
                                 {'mean_queue_time': 0, 'mean_unused_builders': 2},
                                 {'mean_queue_time': 0, 'mean_unused_builders': 3},
                                 {'mean_queue_time': 0, 'mean_unused_builders': 4}]})
    # costs = 0.12 * [1, 2, 3, 4] = [0.12, 0.24, 0.36, 0.48]
    # m = mean([0.12, 0.24, 0.36, 0.48]) = 0.29999999999999999
    # s = std([0.12, 0.24, 0.36, 0.48]) = 0.13416407864998739
    # se = s / sqrt(4) = 0.13416407864998739 / 2 = 0.0670820393249937
    # z = 1.96
    # lower = 0.29999999999999999 - 0.0670820393249937 * 1.96 = 0.16851920292301234
    # upper = 0.29999999999999999 + 0.0670820393249937 * 1.96 = 0.4314807970769876
    expected = (0.16851920292301234, 0.4314807970769876)
    assert actual == expected


def test_compare_cis():
    ci_a = (1, 5)
    ci_b = (4, 6)
    ci_c = (6, 9)
    assert compare_cis(ci_a, ci_b) == 0
    assert compare_cis(ci_a, ci_c) == -1
    assert compare_cis(ci_c, ci_a) == 1


def test_compare_results():
    bad = {'input': {'sec_per_tick': 3600, 'builds_per_hour': 1, 'ticks': 1},
           'output': [{'total_queue_time': 0, 'mean_unused_builders': 5000},
                      {'total_queue_time': 0, 'mean_unused_builders': 5000}]}
    good = {'input': {'sec_per_tick': 3600, 'builds_per_hour': 1, 'ticks': 1},
           'output': [{'total_queue_time': 0, 'mean_unused_builders': 1},
                      {'total_queue_time': 0, 'mean_unused_builders': 1}]}
    assert sorted([bad, good], cmp=compare_result_cis) == [good, bad]
    assert sorted([bad, good], cmp=compare_result_means) == [good, bad]
    assert compare_result_means(good, bad, cost_per_builder_hour=0) == 0
