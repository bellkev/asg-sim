from collections import defaultdict
from math import log

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean

from ..batches import load_results, STATIC_MINIMA, BOOT_TIMES
from ..cost import costs_from_job_results, cost_ci, compare_result_means, COST_PER_BUILDER_HOUR_EXPENSIVE


def dep_vars(result):
    return (result['input']['build_run_time'], result['input']['builds_per_hour'])


def realistic(result):
    return result['input']['builder_boot_time'] >= 60 and result['input']['alarm_period_duration'] >= 60

def bucket_auto_results(static_path, auto_path, auto_result_filter=realistic):
    static_results = load_results(static_path)
    auto_results = load_results(auto_path)
    dvs_static_results = {}
    worse_autos = defaultdict(list)
    maybe_better_autos = defaultdict(list)
    min_auto_means = {}
    min_autos = {}
    for result in static_results:
        dvs_static_results[dep_vars(result)] = result
    for result in filter(auto_result_filter, auto_results):
        dvs = dep_vars(result)
        mean_cost = mean(costs_from_job_results(result))
        if dvs not in min_autos.keys() or mean_cost < min_auto_means[dvs]:
            min_auto_means[dvs] = mean_cost
            min_autos[dvs] = result
        comparison = compare_results(result, dvs_static_results[dvs])
        if comparison >= 0:
            maybe_better_autos[dvs].append(result)
        elif comparison < 0:
            worse_autos[dvs].append(result)
    return dvs_static_results, worse_autos, maybe_better_autos, min_autos


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
    print 'By dependent variables:'
    for key in better.keys():
        print key, ':'
        print_param_sets(better[key])


def param_match(d1, result):
    d2 = result['input']
    return all(d1[key] == d2[key] for key in set(d1.keys()).intersection(set(d2.keys())))


def param_match_pred(d):
    return lambda x: param_match(d, x)


def make_savings_v_build_time_plot(static, auto):
    pred = param_match_pred({'builds_per_hour': 2.0, 'builder_boot_time': 600})
    static_costs = {}
    min_autos = {}
    min_auto_costs = {}
    dep_var = 'build_run_time'
    for result in filter(pred, static):
        static_costs[result['input'][dep_var]] = mean(costs_from_job_results(result))
    for result in filter(pred, auto):
        cost = mean(costs_from_job_results(result))
        val = result['input'][dep_var]
        if val not in min_autos.keys() or cost < min_auto_costs[val]:
            min_autos[val] = result
            min_auto_costs[val] = cost
    plt.plot([key for key in min_auto_costs.keys()], [1 - min_auto_costs[key] / static_costs[key] for key in min_auto_costs.keys()], 'bo')
    plt.savefig('plots/min_auto_cost_v_runtime')
    plt.close()


def make_savings_plot(static, sorted_auto):
    mins = {} # defaultdict(list)
    xs = []
    ys = []
    for result in sorted_auto:
        params = result['input']
        result_key = (params['build_run_time'], params['builds_per_hour'], params['builder_boot_time'])
        if result_key not in mins:
            mins[result_key] = result
    for min_key in mins:
        static_result = filter(param_match_pred({'build_run_time': min_key[0], 'builds_per_hour': min_key[1], 'builder_boot_time': min_key[2]}), static)[0]
        static_cost = mean(costs_from_job_results(static_result))
        auto_cost = mean(costs_from_job_results(mins[min_key]))
        build_time = float(min_key[0])
        per_hour = float(min_key[1])
        boot_time = float(min_key[2])
        sec_per = 3600.0 / per_hour
        x = log(max(boot_time / sec_per, boot_time / build_time))
        R = auto_cost / static_cost
        y = 1 - R
        xs.append(x)
        ys.append(y)
    plt.plot(xs, ys, 'bo')
    plt.savefig('plots/auto_savings.svg', format='svg')
    plt.close()


if __name__ == '__main__':
    make_savings_plot(load_results('jobs/static'), sorted(load_results('jobs/auto'), cmp=compare_result_means))
