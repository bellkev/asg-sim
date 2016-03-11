import json
from math import sqrt
from multiprocessing import Pool

from numpy import mean, std

from .model import run_model


# a reasonably average contractor rate multiplied
# by a bit of a fudge factor for interruption
COST_PER_DEV_HOUR = 200
# m4.large on-demand price
COST_PER_BUILDER_HOUR = 0.12
# 2x m4.10xl on-demand price
COST_PER_BUILDER_HOUR_EXPENSIVE = 4.698


def _run_job(trials=1, **opts):
    return {'input': opts,
             'output': [{'total_queue_time': model.total_queue_time(),
                         'mean_unused_builders': model.mean_unused_builders()}
                        for model in [run_model(**opts) for t in range(trials)]]}


def run_job(opts):
   return  _run_job(**opts)


def costs_from_job_results(results, cost_per_builder_hour=COST_PER_BUILDER_HOUR,
                           cost_per_dev_hour=COST_PER_DEV_HOUR):
    """
    Returns a scalar cost for a single-run job, or a list
    of costs for a job with multiple trials.
    """
    opts = results['input']
    output = results['output']
    # Cost parameters
    sec_per_tick = opts['sec_per_tick']
    ticks = opts['ticks']
    simulation_time_hours = ticks * sec_per_tick / 3600.0
    # Prefer to use total queue time directly, but support older format
    if 'total_queue_time' in output[0].keys():
        def cost_of_output(output):
            builder_cost = output['mean_unused_builders'] * cost_per_builder_hour * simulation_time_hours
            queue_cost = output['total_queue_time'] / 3600.0 * cost_per_dev_hour
            return builder_cost + queue_cost
    else:
        def cost_of_output(output):
            cost_per_hour = (output['mean_unused_builders'] * cost_per_builder_hour
                             + opts['builds_per_hour'] * output['mean_queue_time'] / 3600.0 * cost_per_dev_hour)
            return simulation_time_hours * cost_per_hour
    return map(cost_of_output, output)


def costs(opts):
    # Bit of a hack to keep things easily pickleable
    machine_cost = opts.pop('cost_per_builder_hour',
                            COST_PER_BUILDER_HOUR)
    dev_cost = opts.pop('cost_per_dev_hour',
                            COST_PER_DEV_HOUR)
    results = run_job(opts)
    return costs_from_job_results(results, cost_per_builder_hour=machine_cost,
                                  cost_per_dev_hour=dev_cost)


def cost_ci(results, percent=95):
    """
    Returns 95 percent confidence interval for cost of `results`,
    assuming costs are normally distributed.
    """
    assert len(results) > 1
    costs = costs_from_job_results(results)
    z = {95: 1.96, 99: 2.58, 99.5: 2.81, 99.9: 3.29} # http://mathworld.wolfram.com/StandardDeviation.html
    m = mean(costs)
    s = std(costs)
    se = s / sqrt(len(costs))
    return (m - se * z[percent], m + se * z[percent])


def compare_cis(ci_a, ci_b):
    """
    Returns:
      1 if cost(a) < cost(b),
     -1 if cost(b) < cost(a),
      0 if confidence intervals overlap
    """
    if ci_a[1] < ci_b[0]:
        return -1
    elif ci_b[1] < ci_a[1]:
        return 1
    else:
        return 0


def compare_result_cis(a, b):
    ci_a = cost_ci(a)
    ci_b = cost_ci(b)
    return compare_cis(ci_a, ci_b)


def compare_result_means(a, b, **kwargs):
    mean_a = mean(costs_from_job_results(a, **kwargs))
    mean_b = mean(costs_from_job_results(b, **kwargs))
    if mean_a < mean_b:
        return -1
    elif mean_b < mean_a:
        return 1
    else:
        return 0
