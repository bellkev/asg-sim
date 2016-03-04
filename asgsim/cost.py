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
#COST_PER_BUILDER_HOUR = 4.698


def _run_job(trials=1, **opts):
    return {'input': opts,
             'output': [{'total_queue_time': model.total_queue_time(),
                         'mean_unused_builders': model.mean_unused_builders()}
                        for model in [run_model(**opts) for t in range(trials)]]}


def run_job(opts):
   return  _run_job(**opts)


def cost_from_job_results(results):
    """
    Returns a scalar cost for a single-run job, or a list
    of costs for a job with multiple trials.
    """
    opts = results['input']
    output = results['output']
    if isinstance(output, list):
        output_sample = output[0]
    else:
        output_sample = output
    # Cost parameters
    sec_per_tick = opts['sec_per_tick']
    cost_per_dev_hour = 100 # a reasonably average contractor rate
    adjusted_cost_per_dev_hour = cost_per_dev_hour * 2 # adjust for a bit of a "concentration loss factor"
    ticks = opts['ticks']
    simulation_time_hours = ticks * sec_per_tick / 3600.0
    # Prefer to use total queue time directly, but support older format
    if 'total_queue_time' in output_sample.keys():
        def cost_of_output(output):
            builder_cost = output['mean_unused_builders'] * COST_PER_BUILDER_HOUR * simulation_time_hours
            queue_cost = output['total_queue_time'] / 3600.0 * COST_PER_DEV_HOUR
            return builder_cost + queue_cost
    else:
        def cost_of_output(output):
            cost_per_hour = (output['mean_unused_builders'] * COST_PER_BUILDER_HOUR
                         + opts['builds_per_hour'] * output['mean_queue_time'] / 3600.0 * COST_PER_DEV_HOUR)
            return simulation_time_hours * cost_per_hour
    if isinstance(output, list):
        return map(cost_of_output, output)
    else:
        return cost_of_output(output)


def cost(opts):
    results = run_job(opts)
    costs = cost_from_job_results(results)
    if len(costs) == 1:
        return costs[0]
    else:
        return costs


def cost_ci(results, percent=95):
    """
    Returns 95 percent confidence interval for cost of `results`,
    assuming costs are normally distributed.
    """
    assert len(results) > 1
    costs = cost_from_job_results(results)
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
        return 1
    elif ci_b[1] < ci_a[1]:
        return -1
    else:
        return 0


def compare_results(a, b):
    ci_a = cost_ci(a)
    ci_b = cost_ci(b)
    return compare_cis(ci_a, ci_b)
