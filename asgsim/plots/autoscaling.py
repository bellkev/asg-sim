from collections import defaultdict
from math import log

from numpy import mean

from ..batches import generate_jobs, load_results, STATIC_MINIMA, STATIC_MINIMA_LIMITED, BOOT_TIMES, TRIAL_DURATION_SECS
from ..cost import costs_from_job_results, cost_ci, compare_result_means, COST_PER_BUILDER_HOUR_EXPENSIVE
from ..model import run_model
from .utils import plt, plt_title, plt_save, make_scaling_plot


def compare_result_means_expensive(a, b):
    return compare_result_means(a, b, cost_per_builder_hour=COST_PER_BUILDER_HOUR_EXPENSIVE)


def generate_candidate_jobs(sorted_auto, path, fraction=0.01, static_minima=STATIC_MINIMA, **kwargs):
    minima = defaultdict(list)
    candidates_per_key = max(1, int(len(sorted_auto) / float(len(static_minima) * len(BOOT_TIMES)) * fraction)) # take the best `fraction`
    for result in sorted_auto:
        params = result['input']
        result_key = (params['build_run_time'], params['builds_per_hour'], params['builder_boot_time'])
        if len(minima[result_key]) < candidates_per_key:
            minima[result_key].append(result)
    generate_jobs([result['input'] for key in minima for result in minima[key]], path, **kwargs)


def param_match(d1, result):
    d2 = result['input']
    return all(d1[key] == d2[key] for key in set(d1.keys()).intersection(set(d2.keys())))


def param_match_pred(d):
    return lambda x: param_match(d, x)


def min_auto_params(static, auto, **kwargs):
    """
    Given static and autoscaling results, return the parameters
    that minimize cost for each set of independent variables,
    with savings over static fleets added into the param dict.
    """
    auto_key_fn = lambda x: (x['build_run_time'], x['builds_per_hour'], x['builder_boot_time'])
    static_key_fn = lambda x: (x['build_run_time'], x['builds_per_hour'])
    static_costs = {}
    min_auto_costs = {}
    min_autos = {}
    for result in static:
        static_costs[static_key_fn(result['input'])] = mean(costs_from_job_results(result, **kwargs))
    for result in auto:
        key = auto_key_fn(result['input'])
        cost = mean(costs_from_job_results(result, **kwargs))
        if key not in min_autos or cost < min_auto_costs[key]:
            min_autos[key] = result
            min_auto_costs[key] = cost
    ret = []
    for key in min_autos:
        params = min_autos[key]['input']
        params['savings'] = 1 - min_auto_costs[key] / static_costs[key[:2]]
        ret.append(params)
    return ret


def one_min_auto_params(static, auto, param_filter, **kwargs):
    """
    Given static and autoscaling results and a parameter filter,
    ensure that the filter uniquely specifies a set of independent
    variables and return the corresponding minimum-cost parameters.
    """
    pred = param_match_pred(param_filter)
    params = min_auto_params(filter(pred, static), filter(pred, auto), **kwargs)
    assert len(params) == 1
    return params[0]


def dump_max_savings(static, auto):
    print 'build_run_time\tbuilds_per_hour\tbuilder_boot_time\tsavings'
    rows = min_auto_params(static, auto)
    for row in rows:
        print '\t'.join(map(str, [row['build_run_time'], row['builds_per_hour'], row['builder_boot_time'], row['savings']]))


def make_log_contour_plot(static, auto, path):
    rows = min_auto_params(static, auto)
    log_boot_build_times = [log(row['builder_boot_time'] / float(row['build_run_time']), 2) for row in rows]
    log_boot_sec_pers = [log(row['builder_boot_time'] / (3600.0 / row['builds_per_hour']), 2) for row in rows]
    savings = [row['savings'] for row in rows]
    plt.tricontourf(log_boot_build_times, log_boot_sec_pers, savings)
    plt.xlabel('log(boot_time/build_time)')
    plt.ylabel('log(boot_time/sec_per)')
    plt.colorbar()
    plt_save(path)


def make_linear_contour_plot_for_boot_time(static, auto, boot_time, path):
    rows = [row for row in min_auto_params(static, auto) if row['builder_boot_time'] == boot_time]
    plt.tricontourf([row['build_run_time'] for row in rows], [row['builds_per_hour'] for row in rows], [row['savings'] for row in rows], 20)
    plt.xlabel('build_run_time')
    plt.ylabel('builds_per_hour')
    plt.colorbar()
    plt_save(path)


def make_savings_v_boot_time_plot(static, auto):
    pred = param_match_pred({'builds_per_hour': 50.0, 'build_run_time': 600})
    rows = min_auto_params(filter(pred, static), filter(pred, auto))
    plt_title('Max Savings Over Static Fleet (50 builds / hr, 10 min / build)')
    plt.xlabel('Builder Boot Time (m)')
    plt.ylabel('Savings (%)')
    plt.axis([0, 11, 0, 35])
    plt.plot([params['builder_boot_time'] / 60.0 for params in rows], [params['savings'] * 100.0 for params in rows], 'bo')
    plt_save('plots/savings_v_boot_time')


def make_savings_v_build_time_plot(static, auto):
    boot_time = 300
    slow_pred = param_match_pred({'builder_boot_time': boot_time, 'builds_per_hour': 2.0})
    fast_pred = param_match_pred({'builder_boot_time': boot_time, 'builds_per_hour': 50.0})
    slow = min_auto_params(filter(slow_pred, static), filter(slow_pred, auto))
    fast = min_auto_params(filter(fast_pred, static), filter(fast_pred, auto))
    plt_title('Max Savings Over Static Fleet (5 min builder boot time)')
    plt.xlabel('Build Run Time (m)')
    plt.ylabel('Savings (%)')
    plt.axis([0, 41, 0, 50])
    s_handle, = plt.plot([params['build_run_time'] / 60.0 for params in slow], [params['savings'] * 100.0 for params in slow], 'bo', label='2 builds / hr')
    f_handle, = plt.plot([params['build_run_time'] / 60.0 for params in fast], [params['savings'] * 100.0 for params in fast], 'gs', label='50 builds / hr')
    plt.legend(handles=(s_handle, f_handle), loc='upper left')
    plt_save('plots/savings_v_build_time')


def make_savings_v_traffic_plot(static, auto):
    boot_time = 300
    slow_pred = param_match_pred({'builder_boot_time': boot_time, 'build_run_time': 2400})
    fast_pred = param_match_pred({'builder_boot_time': boot_time, 'build_run_time': 300})
    slow = min_auto_params(filter(slow_pred, static), filter(slow_pred, auto))
    fast = min_auto_params(filter(fast_pred, static), filter(fast_pred, auto))
    plt_title('Max Savings Over Static Fleet (5 min builder boot time)')
    plt.xlabel('Builds Per Hour')
    plt.ylabel('Savings (%)')
    plt.axis([0, 205, 0, 50])
    s_handle, = plt.plot([params['builds_per_hour'] for params in slow], [params['savings'] * 100.0 for params in slow], 'bo', label='40 min builds')
    f_handle, = plt.plot([params['builds_per_hour'] for params in fast], [params['savings'] * 100.0 for params in fast], 'gs', label='5 min builds')
    plt.legend(handles=(s_handle, f_handle), loc='upper right')
    plt_save('plots/savings_v_traffic')


def make_savings_v_dev_cost_plot(static, auto):
    dev_costs = [0.01, 0.1, 1, 10, 100]
    rows = [one_min_auto_params(static, auto,
                                {'builder_boot_time': 300, 'build_run_time': 300, 'builds_per_hour': 50.0},
                                cost_per_builder_hour=COST_PER_BUILDER_HOUR_EXPENSIVE,
                                cost_per_dev_hour=dev_cost)
            for dev_cost in dev_costs]
    plt.plot([log(dev_cost, 10) for dev_cost in dev_costs], [row['savings'] for row in rows], 'bo')
    plt_save('plots/savings_dev_cost_expensive')


def make_savings_v_traffic_plot_varying(static_const, auto_const, static_sine, auto_sine):
    boot_time = 300
    pred = param_match_pred({'builder_boot_time': boot_time, 'build_run_time': 300})
    const = min_auto_params(filter(pred, static_const), filter(pred, auto_const))
    sine = min_auto_params(filter(pred, static_sine), filter(pred, auto_sine))
    c_handle, = plt.plot([params['builds_per_hour'] for params in const], [params['savings'] for params in const], 'bo', label='Constant Traffic')
    s_handle, = plt.plot([params['builds_per_hour'] for params in sine], [params['savings'] for params in sine], 'gs', label='Sine-Varying Traffic')
    plt.legend(handles=(c_handle, s_handle), loc='upper left')
    plt_save('plots/savings_v_traffic_varying')


def make_constant_traffic_plots():
    make_savings_v_build_time_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'))
    make_savings_v_traffic_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'))
    make_savings_v_boot_time_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'))
    make_savings_v_dev_cost_plot(load_results('job-archives/2c517e8/static-expensive'), load_results('job-archives/2c517e8/auto'))


if __name__ == '__main__':
    make_constant_traffic_plots()
    params = one_min_auto_params(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'),
                                          {'build_run_time': 2400, 'builds_per_hour': 2, 'builder_boot_time': 300})
    params['ticks'] = 2000
    params2 = one_min_auto_params(load_results('jobs/static'), load_results('jobs/candidates1'),
                                          {'build_run_time': 300, 'builds_per_hour': 200.0, 'builder_boot_time': 300})
    params2['ticks'] = 1500 * 60
    print params2
    make_scaling_plot(params,
                      'Auto Scaling Fleet Capacity and Usage (40 min / build, 2 builds / hr)', 'plots/slow_auto_scaling', axis=[0, 20000 / 60, 0, 8])
    make_scaling_plot(params2,
                      'Auto Scaling Fleet Capacity and Usage (5 min / build, 200 builds / hr)', 'plots/sine_auto_scaling', axis=[0, 1500, 0, 35])

    # make_savings_v_traffic_plot_varying(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'),
    #                                     load_results('jobs/static'), load_results('jobs/candidates1'))
