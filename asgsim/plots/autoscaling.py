from collections import defaultdict
from math import log

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean

from ..batches import generate_jobs, load_results, STATIC_MINIMA, STATIC_MINIMA_LIMITED, BOOT_TIMES, TRIAL_DURATION_SECS
from ..cost import costs_from_job_results, cost_ci, compare_result_means, COST_PER_BUILDER_HOUR_EXPENSIVE
from ..model import run_model


def param_match(d1, result):
    d2 = result['input']
    return all(d1[key] == d2[key] for key in set(d1.keys()).intersection(set(d2.keys())))


def param_match_pred(d):
    return lambda x: param_match(d, x)


def max_savings(static, auto, param_filter, **kwargs):
    pred = param_match_pred(param_filter)
    static_cost = None
    min_auto = None
    min_auto_cost = None
    static_results = filter(pred, static)
    assert len(static_results) == 1 # param_filter should uniquely specify a static case
    static_cost = mean(costs_from_job_results(static_results[0], **kwargs))
    for result in filter(pred, auto):
        cost = mean(costs_from_job_results(result, **kwargs))
        if not min_auto_cost or cost < min_auto_cost:
            min_auto = result
            min_auto_cost = cost
    return min_auto, (1 - min_auto_cost / static_cost) * 100

def make_savings_v_dev_cost_plot(static, auto, param_filter, trial_duration=TRIAL_DURATION_SECS):
    dev_costs = [0.01, 0.1, 1, 5, 10, 100, 200]
    savings = []
    for dev_cost in dev_costs:
        result, saving = max_savings(static, auto, param_filter,
                                      cost_per_builder_hour=COST_PER_BUILDER_HOUR_EXPENSIVE,
                                      cost_per_dev_hour=dev_cost)
        savings.append(saving)
        print 'developer rate:', dev_cost
        print 'savings:', '%s%%' % saving
        print 'mean queue time:', mean([o['total_queue_time'] for o in result['output']]) / (result['input']['builds_per_hour'] * trial_duration / 3600.0), 'seconds'
        print 'params:', result['input']
    plt.plot(dev_costs, savings)
    plt.savefig('plots/savings_v_dev_cost')
    plt.close()


def make_savings_v_i_var_plot(static, auto, param_filter, i_var, transform=lambda x:x, suffix='', **kwargs):
    pred = param_match_pred(param_filter)
    static_costs = {}
    min_autos = {}
    min_auto_costs = {}
    for result in filter(pred, static):
        val = transform(result['input'][i_var])
        assert val not in static_costs # param_filter should uniquely specify a static case
        static_costs[val] = mean(costs_from_job_results(result, **kwargs))
    for result in filter(pred, auto):
        cost = mean(costs_from_job_results(result, **kwargs))
        val = transform(result['input'][i_var])
        if val not in min_autos.keys() or cost < min_auto_costs[val]:
            min_autos[val] = result
            min_auto_costs[val] = cost
    plt.plot([key for key in min_auto_costs], [1 - min_auto_costs[key] / static_costs[key] for key in min_auto_costs], 'bo')
    plt.savefig('plots/auto_savings_v_%s%s' % (i_var, suffix))
    plt.close()


def generate_candidate_jobs(sorted_auto, path, fraction=0.01, static_minima=STATIC_MINIMA, **kwargs):
    minima = defaultdict(list)
    candidates_per_key = max(1, int(len(sorted_auto) / float(len(static_minima) * len(BOOT_TIMES)) * fraction)) # take the best `fraction`
    for result in sorted_auto:
        params = result['input']
        result_key = (params['build_run_time'], params['builds_per_hour'], params['builder_boot_time'])
        if len(minima[result_key]) < candidates_per_key:
            minima[result_key].append(result)
    generate_jobs([result['input'] for key in minima for result in minima[key]], path, **kwargs)


def minima_from_sorted_coll(key_fn, sorted_coll):
    minima = {}
    for result in sorted_coll:
        params = result['input']
        result_key = key_fn(params)
        if result_key not in minima:
            minima[result_key] = result
    return minima


def auto_key_fn(x):
    return (x['build_run_time'], x['builds_per_hour'], x['builder_boot_time'])


def static_key_fn(x):
    return (x['build_run_time'], x['builds_per_hour'])


def make_savings_v_boot_time_plot(sorted_static, sorted_auto, scale_var, transform=lambda x:x, scale_var_label=None, suffix='', **kwargs):
    scale_var_label = scale_var_label or scale_var
    min_autos = minima_from_sorted_coll(auto_key_fn, sorted_auto)
    min_statics = minima_from_sorted_coll(static_key_fn, sorted_static)
    ratios = []
    savings = []
    for min_key in min_autos:
        static_cost = mean(costs_from_job_results(min_statics[min_key[:2]], **kwargs))
        auto_cost = mean(costs_from_job_results(min_autos[min_key], **kwargs))
        scale_var_transformed = transform(float(min_autos[min_key]['input'][scale_var]))
        boot_time = float(min_autos[min_key]['input']['builder_boot_time'])
        ratios.append(boot_time / scale_var_transformed)
        savings.append((1 - auto_cost / static_cost) * 100.0)
    max_savings = {}
    for ratio, saving in zip(ratios, savings):
        bucket = round(log(ratio, 2.0))
        # if bucket == 0.0:
        #     print "ratio:", ratio, "saving:", saving,
        if bucket not in max_savings or saving > max_savings[bucket]:
            # print "added"
            max_savings[bucket] = saving
    plt.title('Savings from Auto Scaling Groups', y=1.05)
    plt.ylabel('Maximum savings over fixed-size fleet (%)')
    plt.xlabel('Ratio of builder_boot_time:%s' % scale_var_label)
    plt.plot(max_savings.keys(), max_savings.values(), 'bo')
    ax = plt.subplot(111)
    ax.set_xticks([-6.0, -4.0, -2.0, 0.0, 2.0, 4.0, 6.0])
    ax.set_xticklabels(['1:64', '1:16', '1:4', '1:1', '4:1', '16:1', '64:1'])
    plt.savefig('plots/savings_v_boot_time_and_%s%s.svg' % (scale_var_label, suffix), format='svg')
    plt.close()


def min_auto_params(static, auto):
    # Return list of (build_run_time, builds_per_hour, builder_boot_time, max_savings)
    static_costs = {}
    min_auto_costs = {}
    min_autos = {}
    for result in static:
        static_costs[static_key_fn(result['input'])] = mean(costs_from_job_results(result))
    for result in auto:
        key = auto_key_fn(result['input'])
        cost = mean(costs_from_job_results(result))
        if key not in min_autos or cost < min_auto_costs[key]:
            min_autos[key] = result
            min_auto_costs[key] = cost
    ret = []
    for key in min_autos:
        params = min_autos[key]['input']
        params['savings'] = 1 - min_auto_costs[key] / static_costs[key[:2]]
        ret.append((params['build_run_time'], params['builds_per_hour'], params['builder_boot_time'], params['savings'], params))
    return ret


def dump_max_savings(static, auto):
    print 'build_run_time\tbuilds_per_hour\tbuilder_boot_time\tsavings'
    rows = min_auto_params(static, auto)
    for row in rows:
        print '\t'.join(map(str, row))


def make_log_contour_plot(static, auto, path):
    rows = min_auto_params(static, auto)
    log_boot_build_times = [log(row[2] / float(row[0]), 2) for row in rows]
    log_boot_sec_pers = [log(row[2] / (3600.0 / row[1]), 2) for row in rows]
    savings = [row[3] for row in rows]
    plt.tricontourf(log_boot_build_times, log_boot_sec_pers, savings)
    plt.xlabel('log(boot_time/build_time)')
    plt.ylabel('log(boot_time/sec_per)')
    plt.colorbar()
    plt.savefig(path)
    plt.close()


def make_linear_contour_plot_for_boot_time(static, auto, boot_time, path):
    rows = [row for row in min_auto_params(static, auto) if row[2] == boot_time]
    plt.tricontourf([row[0] for row in rows], [row[1] for row in rows], [row[3] for row in rows], 20)
    plt.xlabel('build_run_time')
    plt.ylabel('builds_per_hour')
    plt.colorbar()
    plt.savefig(path)
    plt.close()


def make_savings_v_build_time_plot(static, auto):
    boot_time = 300
    slow_pred = param_match_pred({'builder_boot_time': boot_time, 'builds_per_hour': 2.0})
    fast_pred = param_match_pred({'builder_boot_time': boot_time, 'builds_per_hour': 50.0})
    slow = [row[4] for row in min_auto_params(filter(slow_pred, static), filter(slow_pred, auto))]
    fast = [row[4] for row in min_auto_params(filter(fast_pred, static), filter(fast_pred, auto))]
    s_handle, = plt.plot([params['build_run_time'] for params in slow], [params['savings'] for params in slow], 'bo', label='2 builds / hr')
    f_handle, = plt.plot([params['build_run_time'] for params in fast], [params['savings'] for params in fast], 'gs', label='50 builds / hr')
    plt.legend(handles=(s_handle, f_handle), loc='upper left')
    plt.savefig('plots/savings_v_build_time')
    plt.close()


def make_savings_v_traffic_plot(static, auto):
    boot_time = 300
    slow_pred = param_match_pred({'builder_boot_time': boot_time, 'build_run_time': 2400})
    fast_pred = param_match_pred({'builder_boot_time': boot_time, 'build_run_time': 300})
    slow = [row[4] for row in min_auto_params(filter(slow_pred, static), filter(slow_pred, auto))]
    fast = [row[4] for row in min_auto_params(filter(fast_pred, static), filter(fast_pred, auto))]
    s_handle, = plt.plot([params['builds_per_hour'] for params in slow], [params['savings'] for params in slow], 'bo', label='40 min builds')
    f_handle, = plt.plot([params['builds_per_hour'] for params in fast], [params['savings'] for params in fast], 'gs', label='5 min builds')
    plt.legend(handles=(s_handle, f_handle), loc='upper right')
    plt.savefig('plots/savings_v_traffic')
    plt.close()


def make_savings_v_boot_time_plot(static, auto):
    pred = param_match_pred({'builds_per_hour': 50.0, 'build_run_time': 600})
    rows = [row[4] for row in min_auto_params(filter(pred, static), filter(pred, auto))]
    plt.plot([params['builder_boot_time'] for params in rows], [params['savings'] for params in rows], 'bo')
    plt.savefig('plots/savings_v_boot_time')
    plt.close()



def make_scaling_plot():
    params = [row[4] for row in min_auto_params(load_results('jobs/static'), load_results('jobs/candidates1')) if row[:3] == (300, 200.0, 300)][0]
    print 'params:', params
    m = run_model(**params)
    ax = plt.subplot(111)
    ax.stackplot(range(1240,1280), m.builders_in_use[1240:1280], m.builders_available[1240:1280], colors=('#BBA4D1', '#3399CC'), linewidth=0)
    plt.savefig('plots/fig.svg', format='svg')
    plt.close()


def compare_result_means_expensive(a, b):
    return compare_result_means(a, b, cost_per_builder_hour=COST_PER_BUILDER_HOUR_EXPENSIVE)


if __name__ == '__main__':
    # generate_candidate_jobs(sorted(load_results('jobs/auto'), cmp=compare_result_means), 'jobs/candidates1',
    #                         fraction=0.05, trials=10, static_minima=STATIC_MINIMA_LIMITED)
    # sorted_auto = sorted(load_results('jobs/candidates1'), cmp=compare_result_means)
    # sorted_static = sorted(load_results('jobs/static'), cmp=compare_result_means)
    # make_savings_v_boot_time_plot(sorted_static, sorted_auto, 'builds_per_hour',
    #                               transform=lambda x: 3600.0 / x, scale_var_label='mean_time_between_builds',
    #                               suffix='_sine')
    # make_savings_v_boot_time_plot(sorted_static, sorted_auto, 'build_run_time',
    #                               suffix='_sine')
    # make_savings_v_i_var_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'),
    #                           {'builder_boot_time': 600, 'build_run_time': 300}, 'builds_per_hour', suffix='_const')
    # make_savings_v_i_var_plot(load_results('jobs/static'), load_results('jobs/candidates1'),
    #                           {'builder_boot_time': 300, 'build_run_time': 300}, 'builds_per_hour', suffix='_sine')
    # make_savings_v_dev_cost_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/auto'),
    #                              {'builder_boot_time': 300, 'build_run_time': 300, 'builds_per_hour': 10.0})
    # dump_max_savings(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'))
    # linear_contour_plot_for_boot_time(load_results('jobs/static'), load_results('jobs/candidates1'), 600, 'plots/contour_sine')
    # linear_contour_plot_for_boot_time(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'), 600, 'plots/contour_const')
    make_scaling_plot()
    # make_savings_v_build_time_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'))
    # make_savings_v_traffic_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'))
    # make_savings_v_boot_time_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'))
