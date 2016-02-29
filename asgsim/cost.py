import json
from multiprocessing import Pool

from .model import run_model



def run_job(opts):
    m = run_model(**opts)
    return {'input': opts,
            'output': {'mean_queue_time': m.mean_queue_time(),
                       'mean_unused_builders': m.mean_unused_builders()}}


def cost_from_job_results(results):
    opts = results['input']
    mean_queue_time = results['output']['mean_queue_time']
    mean_unused_builders = results['output']['mean_unused_builders']

    # Cost parameters
    sec_per_tick = opts['sec_per_tick']
    cost_per_dev_hour = 100 # a reasonably average contractor rate
    adjusted_cost_per_dev_hour = cost_per_dev_hour * 2 # adjust for a bit of a "concentration loss factor"
    cost_per_builder_hour = 0.12 # m4.large on-demand price
    #cost_per_builder_hour = 4.698 # 2x m4.10xl on-demand price
    builds_per_hour = opts['builds_per_hour']
    ticks = opts['ticks']
    simulation_time_hours = ticks * sec_per_tick / 3600.0

    return simulation_time_hours * (mean_unused_builders * cost_per_builder_hour
                                    + builds_per_hour * mean_queue_time / 3600.0 * adjusted_cost_per_dev_hour)

def cost(opts):
    results = run_job(opts)
    return cost_from_job_results(results)
