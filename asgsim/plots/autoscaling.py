from collections import defaultdict
from math import log

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean

from ..batches import generate_jobs, load_results, STATIC_MINIMA, BOOT_TIMES, TRIAL_DURATION_SECS
from ..cost import costs_from_job_results, cost_ci, compare_result_means, COST_PER_BUILDER_HOUR_EXPENSIVE


def i_vars(result):
    return (result['input']['build_run_time'], result['input']['builds_per_hour'])


def realistic(result):
    return result['input']['builder_boot_time'] >= 60 and result['input']['alarm_period_duration'] >= 60

def bucket_auto_results(static_path, auto_path, auto_result_filter=realistic):
    static_results = load_results(static_path)
    auto_results = load_results(auto_path)
    ivs_static_results = {}
    worse_autos = defaultdict(list)
    maybe_better_autos = defaultdict(list)
    min_auto_means = {}
    min_autos = {}
    for result in static_results:
        ivs_static_results[i_vars(result)] = result
    for result in filter(auto_result_filter, auto_results):
        ivs = i_vars(result)
        mean_cost = mean(costs_from_job_results(result))
        if ivs not in min_autos.keys() or mean_cost < min_auto_means[ivs]:
            min_auto_means[ivs] = mean_cost
            min_autos[ivs] = result
        comparison = compare_results(result, ivs_static_results[ivs])
        if comparison >= 0:
            maybe_better_autos[ivs].append(result)
        elif comparison < 0:
            worse_autos[ivs].append(result)
    return ivs_static_results, worse_autos, maybe_better_autos, min_autos


def param_sets(result_col):
    """
    Takes a dict, list, or dict of lists of
    results and returns the sets of parameters
    in that collection.
    """
    ret = defaultdict(set)
    results = []
    if isinstance(result_col, list):
        results.extend(result_col)
    elif isinstance(result_col, dict):
        vals = result_col.values()
        if isinstance(vals[0], list):
            for val in vals:
                results.extend(val)
        elif isinstance(vals[0], dict):
            for val in vals:
                results.append(val)
        else:
            raise TypeError('Dict entries must be result lists or result dicts')
    else:
        raise TypeError('Input must be a dict or list')
    params = results[0]['input'].keys()
    for result in results:
        for param in params:
            ret[param].add(result['input'][param])
    return ret


def print_param_sets(result_col):
    sets = param_sets(result_col)
    for key in sorted(sets.keys()):
        print '  ', key, ':', sorted(list(sets[key]))

def print_params(result):
    for key in sorted(result['input'].keys()):
        print key, ':', result['input'][key]


def print_summary(buckets):
    static, worse, better, mins = buckets
    print 'Min auto params:'
    print_param_sets(mins)
    print '(Maybe) better auto params:'
    print_param_sets(better)
    print 'By independent variables:'
    for key in better.keys():
        print key, ':'
        print_param_sets(better[key])


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


def make_savings_v_i_var_plot(static, auto, param_filter, i_var, transform=lambda x:x, **kwargs):
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
    plt.savefig('plots/auto_savings_v_' + i_var)
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


def dump_max_savings(static, auto):
    static_key = lambda x: (x['build_run_time'], x['builds_per_hour'])
    auto_key = lambda x: (x['build_run_time'], x['builds_per_hour'], x['builder_boot_time'])
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
    print 'build_run_time\tbuilds_per_hour\tbuilder_boot_time\tsavings'
    for key in min_autos:
        params = min_autos[key]['input']
        print '\t'.join(map(str, [params['build_run_time'], params['builds_per_hour'], params['builder_boot_time'], 1 - min_auto_costs[key] / static_costs[key[:2]]]))



if __name__ == '__main__':
    def compare_result_means_expensive(a, b):
        return compare_result_means(a, b, cost_per_builder_hour=COST_PER_BUILDER_HOUR_EXPENSIVE)
    # generate_candidate_jobs(sorted(load_results('jobs/auto'), cmp=compare_result_means), 'jobs/candidates1',
    #                         fraction=0.05, trials=10, static_minima=STATIC_MINIMA_LIMITED)
    sorted_auto = sorted(load_results('jobs/candidates1'), cmp=compare_result_means)
    sorted_static = sorted(load_results('jobs/static'), cmp=compare_result_means)
    make_savings_v_boot_time_plot(sorted_static, sorted_auto, 'builds_per_hour',
                                  transform=lambda x: 3600.0 / x, scale_var_label='mean_time_between_builds',
                                  suffix='_sine')
    make_savings_v_boot_time_plot(sorted_static, sorted_auto, 'build_run_time',
                                  suffix='_sine')
    # make_savings_v_i_var_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'),
    #                           {'builder_boot_time': 60, 'build_run_time': 300}, 'builds_per_hour')
    # make_savings_v_i_var_plot(load_results('jobs/static'), load_results('jobs/candidates1'),
    #                           {'build_run_time': 300}, 'builds_per_hour')
    # make_savings_v_dev_cost_plot(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/auto'),
    #                              {'builder_boot_time': 300, 'build_run_time': 300, 'builds_per_hour': 10.0})
    # dump_max_savings(load_results('job-archives/2c517e8/static'), load_results('job-archives/2c517e8/candidates2'))
