import json
import os
import sys
from multiprocessing import Pool

from .cost import run_job
from .model import Model


HIGH_RESOLUTION = 10
LOW_RESOLUTION = 60
TRIAL_DURATION_SECS = 100000 # about a day
LONG_TRIAL_DURATION_SECS = 200000 # about two days

# Determined from asgsim.plots.static methods

STATIC_MINIMA = [(300, 10.0, 5), (300, 50.0, 12), (300, 200.0, 31),
                 (60, 50.0, 5), (120, 50.0, 7), (600, 50.0, 19), (1200, 50.0, 31),
                 (2400, 1.0, 5), (2400, 2.0, 6), (2400, 5.0, 10), (2400, 10.0, 17), (2400, 20.0, 26),
                 (60, 2.0, 2), (120, 2.0, 2), (300, 2.0, 3), (600, 2.0, 3), (1200, 2.0, 5)]

STATIC_MINIMA_LIMITED = STATIC_MINIMA[0:7] # Just the more realistic times

STATIC_MINIMA_EXPENSIVE = [(300, 10.0, 3), (300, 50.0, 9), (300, 200.0, 26),
                           (60, 50.0, 3), (120, 50.0, 5), (600, 50.0, 15), (1200, 50.0, 25),
                           (2400, 1.0, 3), (2400, 2.0, 4), (2400, 5.0, 7), (2400, 10.0, 12), (2400, 20.0, 21),
                           (60, 2.0, 1), (120, 2.0, 1), (300, 2.0, 1), (600, 2.0, 2), (1200, 2.0, 3)]

BOOT_TIMES = [60, 120, 300, 600]


def static_fleet_size(build_time, traffic):
    return [minimum[2] for minimum in STATIC_MINIMA if minimum[0] == build_time and minimum[1] == traffic][0]


def valid_threshold(build_time, traffic, up, down):
    return up <= down <= (static_fleet_size(build_time, traffic) + 1)

def sec_per_tick(*times):
    if any(map(lambda t: t < (LOW_RESOLUTION * 2), times)):
        return HIGH_RESOLUTION
    else:
        return LOW_RESOLUTION


def generate_jobs(jobs, path, trials=5):
    jobs_per_batch = 1000
    for job in jobs:
        job['sec_per_tick'] = sec_per_tick(job['build_run_time'],
                                           job.get('builder_boot_time', 9999),
                                           job.get('alarm_period_duration', 9999))
        job['ticks'] = LONG_TRIAL_DURATION_SECS / job['sec_per_tick']
        job['trials'] = trials
    batch_count = len(jobs) / jobs_per_batch + 1
    for batch in range(batch_count):
        start = batch * jobs_per_batch
        end = start + jobs_per_batch
        batch_jobs = jobs[start:end]
        in_dir = os.path.join(path, 'input')
        if not os.path.isdir(in_dir):
            os.mkdir(in_dir)
        with open(os.path.join(in_dir, '%04d' % batch), 'w') as batch_file:
            json.dump(batch_jobs, batch_file)


def static_jobs():
    jobs = [{'autoscale': False,
             'build_run_time': build_time,
             'builds_per_hour': traffic,
             'initial_builder_count': initial,
             'builds_per_hour_fn': Model.SINE}
            for build_time, traffic, initial in STATIC_MINIMA_LIMITED]
    return jobs


def generate_static_jobs(path):
    generate_jobs(static_jobs(), path, trials=1000)


def autoscaling_jobs():
    up_down_range = [1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 20, 24, 28, 32]
    alarm_count_range = [1, 2, 4]
    change_range = [1, 2, 4]
    # Whee! List comprehension!
    jobs = [{'autoscale': True,
             'build_run_time': build_time,
             'builds_per_hour': traffic,
             'builder_boot_time': boot_time,
             'initial_builder_count': up_threshold,
             'alarm_period_duration': alarm_period_duration,
             'scale_up_alarm_period_count': up_alarm_count,
             'scale_down_alarm_period_count': down_alarm_count,
             'scale_up_threshold': up_threshold,
             'scale_down_threshold': down_threshold,
             'scale_up_change': scale_up_change,
             'scale_down_change': scale_down_change,
             'builds_per_hour_fn': Model.SINE}
            # Start at optimum static fleet sizes
            for build_time, traffic, initial in STATIC_MINIMA_LIMITED
            for boot_time in BOOT_TIMES
            for alarm_period_duration in [60, 300, 900]
            for up_alarm_count in alarm_count_range
            for down_alarm_count in alarm_count_range
            # Assume it's silly for scale_up_threshold > scale_down_threshold
            for up_threshold, down_threshold in [(up, down) for up in up_down_range for down in up_down_range if valid_threshold(build_time, traffic, up, down)]
            for scale_up_change in change_range
            for scale_down_change in change_range]
    return jobs


def generate_autoscaling_jobs(path):
    generate_jobs(autoscaling_jobs(), path, trials=3)


def run_batch(path, batch_name, procs=6):
    in_dir = os.path.join(path, 'input')
    out_dir = os.path.join(path, 'output')
    out_file_path = os.path.join(out_dir, batch_name)
    if os.path.isfile(out_file_path):
        print 'Skipping', batch_name
        return
    else:
        print 'Running', batch_name
    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)
    with open(os.path.join(in_dir, batch_name), 'r') as in_file:
        batch_jobs = json.load(in_file)
    p = Pool(procs)
    try:
        results = p.map(run_job, batch_jobs)
    finally:
        p.close()
        p.join()
    with open(out_file_path, 'w') as out_file:
        json.dump(results, out_file)


def run_batches(path, **kwargs):
    for batch_name in sorted(os.listdir(os.path.join(path, 'input'))):
        run_batch(path, batch_name, **kwargs)


def load_results(path):
    output_path = os.path.join(path, 'output')
    batch_names = sorted(os.listdir(output_path))
    results = []
    for batch_name in batch_names:
        batch_results_path = os.path.join(output_path, batch_name)
        with open(batch_results_path, 'r') as batch_results_file:
            batch_results = json.load(batch_results_file)
            results.extend(batch_results)
    return results


if __name__ == '__main__':
    usage = 'Usage: python -m asgsim.batches <generate-auto|generate-static|run> path [procs]'
    if len(sys.argv) < 3:
        print usage
        exit(1)
    task = sys.argv[1]
    path = sys.argv[2]
    procs = 6
    if len(sys.argv) == 4:
        procs = int(sys.argv[3])
    if sys.argv[1] == 'generate-auto':
        print 'Generating autoscaling jobs in', path
        generate_autoscaling_jobs(sys.argv[2])
    elif sys.argv[1] == 'generate-static':
        print 'Generating static jobs in', path
        generate_static_jobs(sys.argv[2])
    elif sys.argv[1] == 'run':
        print 'Running jobs in %s with %d processes' % (path, procs)
        run_batches(sys.argv[2], procs=procs)
    else:
        print usage
        exit(1)
