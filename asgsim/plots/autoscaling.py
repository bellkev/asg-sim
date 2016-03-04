from collections import defaultdict

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from numpy import mean

from ..batches import load_results, STATIC_MINIMA
from ..cost import cost_from_job_results, cost_ci, compare_results


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
        mean_cost = mean(cost_from_job_results(result))
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


def make_boot_time_plot(static, auto):
    dv_filter = lambda x: dep_vars(x) == (300, 50.0)
    static_results = filter(dv_filter, static)
    auto_results = filter(dv_filter, auto)
    static_cost = mean(cost_from_job_results(static_results[0]))
    min_auto_means = {}
    min_autos = {}
    for result in auto_results:
        mean_cost = mean(cost_from_job_results(result))
        boot_time = result['input']['builder_boot_time']
        if boot_time not in min_autos.keys() or mean_cost < min_auto_means[boot_time]:
            min_auto_means[boot_time] = mean_cost
            min_autos[boot_time] = result
    plt.plot(min_auto_means.keys(), min_auto_means.values(), 'bo')
    plt.plot(min_auto_means.keys(), [static_cost for k in min_auto_means.keys()], 'g-')
    plt.savefig('plots/fig')
    plt.close()





if __name__ == '__main__':
    make_boot_time_plot(load_results('jobs/static'), load_results('jobs/auto'))
