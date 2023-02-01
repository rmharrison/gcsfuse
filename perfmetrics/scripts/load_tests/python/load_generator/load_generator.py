"""
Add logging in code to know about progress.
Add documentation to code.
Handle todos
Add reference of classes/constants in docs
Make the tasks with bs settable wherever possible.
"""
import json
import multiprocessing
import os
import sys
import threading
import time
import matplotlib.pyplot as plt
import numpy as np
import psutil
from absl import logging

MIN_RUN_TIME_IN_SECS = 4
MIN_OBSERVATION_INTERVAL_IN_SECS = 2
KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024

class LoadGenerator:

  def __init__(self, num_processes, num_threads, run_time=sys.maxsize,
      num_tasks_per_thread=sys.maxsize, num_tasks=sys.maxsize,
      observation_interval=MIN_OBSERVATION_INTERVAL_IN_SECS):
    self.num_processes = num_processes
    self.num_threads = num_threads
    self.run_time = min(sys.maxsize, run_time)
    self.num_tasks_per_thread = min(sys.maxsize, num_tasks_per_thread)
    self.num_tasks = min(sys.maxsize, num_tasks)
    self.observation_interval = observation_interval

    if self.run_time == sys.maxsize & self.num_tasks_per_thread == sys.maxsize & self.num_tasks == sys.maxsize:
      raise Exception("Out of run_time, num_tasks_per_thread and num_threads, "
                      "one has to be passed.")

    # Checks on load test setup configuration.
    if self.num_tasks_per_thread != sys.maxsize & self.num_tasks != sys.maxsize:
      raise Exception("num_tasks_per_thread and num_tasks both can't be passed.")

    # Run time should be at least MIN_RUN_TIME_IN_SECS in all cases even if
    # num_tasks_per_thread or num_tasks is set.
    if self.run_time < MIN_RUN_TIME_IN_SECS:
      logging.warning("run_time should be at least {0}. Overriding it to {0} "
                      "for this run.".format(MIN_RUN_TIME_IN_SECS))
      self.run_time = MIN_RUN_TIME_IN_SECS

    if observation_interval < MIN_OBSERVATION_INTERVAL_IN_SECS:
      raise Exception("observation_interval can't be less than "
                      "{0}".format(MIN_OBSERVATION_INTERVAL_IN_SECS))

    if observation_interval > (self.run_time / 2):
      raise Exception("observation_interval can't be more than "
                      "('run_time' / 2) i.e. {0}".format(self.run_time / 2))


  def pre_load_test(self, **kwargs):
    pass

  @staticmethod
  def _thread_task(task, assigned_process_id, assigned_thread_id,
      num_tasks_per_thread, pre_tasks_results_queue, tasks_results_queue,
      post_tasks_results_queue):

    cnt = 0
    tasks = [task.pre_task, task.task, task.post_task]
    queues = [pre_tasks_results_queue, tasks_results_queue,
              post_tasks_results_queue]
    while cnt < num_tasks_per_thread:
      for curr_task, curr_queue in zip(tasks, queues):
        start_time = time.perf_counter()
        result = curr_task(assigned_thread_id, assigned_process_id)
        end_time = time.perf_counter()
        curr_queue.put((assigned_process_id, assigned_thread_id, start_time,
                        end_time, result))
      cnt = cnt + 1

  @staticmethod
  def _process_task(task, assigned_process_id, num_threads, num_tasks_per_thread,
      pre_tasks_results_queue, tasks_results_queue, post_tasks_results_queue):

    threads = []
    for thread_num in range(num_threads):
      threads.append(threading.Thread(target=LoadGenerator._thread_task,
                                      args=(task, assigned_process_id,
                                            thread_num, num_tasks_per_thread,
                                            pre_tasks_results_queue,
                                            tasks_results_queue,
                                            post_tasks_results_queue)))

    for thread in threads:
      # ToDo: Check why we need to mark threads as daemon.
      thread.daemon = True
      thread.start()

    logging.debug("Threads started for process number: {0}".format(assigned_process_id))
    for thread in threads:
      thread.join()
    logging.debug("Threads completed for process number: {0}".format(assigned_process_id))

  @staticmethod
  def _convert_multiprocessing_queue_to_list(mp_queue):
    queue_size = mp_queue.qsize()
    return [mp_queue.get() for _ in range(queue_size)]

  def generate_load(self, task):
    tasks_results_queue = multiprocessing.Manager().Queue()
    pre_tasks_results_queue = multiprocessing.Manager().Queue()
    post_tasks_results_queue = multiprocessing.Manager().Queue()

    processes = []
    process_pids = []
    for process_id in range(self.num_processes):
      process = multiprocessing.Process(target=LoadGenerator._process_task,
                                        args=(task, process_id, self.num_threads,
                                              self.num_tasks_per_thread,
                                              pre_tasks_results_queue,
                                              tasks_results_queue,
                                              post_tasks_results_queue))
      processes.append(process)
      process_pids.append(process.pid)

    # Ignore the first psutil.cpu_percent call's output.
    psutil.cpu_percent()
    cpu_usage_pts = [psutil.cpu_percent(interval=self.observation_interval,
                                      percpu=True)]
    net_tcp_conns_pts = [psutil.net_connections(kind='tcp')]
    net_io_pts = [psutil.net_io_counters(pernic=True)]

    for process in processes:
      process.start()
    logging.debug("{0} number of processes started for task {1}".format(len(processes), task.TASK_NAME))

    # time_pts[0] is start time of load generation.
    time_pts = [time.perf_counter()]
    loading_percentages = [0.25, 0.50, 0.75]
    loading_checkpoints = list(map(lambda t: (t * self.run_time) + time_pts[0], [0.25, 0.50, 0.75]))
    curr_loading_idx = 0
    while (time_pts[-1] - time_pts[0]) < self.run_time and (tasks_results_queue.qsize() < self.num_tasks):
      # psutil.cpu_percent is blocking call for the process (and its core) in
      # which it runs. This blocking behavior also acts as a gap between
      # recording observations for other metrics.
      cpu_usage_pts.append(psutil.cpu_percent(interval=self.observation_interval,
                                            percpu=True))
      net_tcp_conns_pts.append(psutil.net_connections(kind='tcp'))
      net_io_pts.append(psutil.net_io_counters(pernic=True))
      curr_time = time.perf_counter()
      time_pts.append(curr_time)
      if curr_loading_idx < len(loading_checkpoints) and curr_time >= loading_checkpoints[curr_loading_idx]:
        logging.info("Load test completed {0}% for task: {1}".format(loading_percentages[curr_loading_idx] * 100, task.TASK_NAME))
        curr_loading_idx = curr_loading_idx + 1

    logging.info("Load test completed 100% for task: {0}".format(task.TASK_NAME))

    for process in processes:
      process.terminate()
    logging.debug("{0} number of processes terminated for task {1}".format(len(processes), task.TASK_NAME))

    return {'time_pts': time_pts, 'process_pids': process_pids,
            'tasks_results': LoadGenerator._convert_multiprocessing_queue_to_list(tasks_results_queue),
            'pre_tasks_results': LoadGenerator._convert_multiprocessing_queue_to_list(pre_tasks_results_queue),
            'post_tasks_results': LoadGenerator._convert_multiprocessing_queue_to_list(post_tasks_results_queue),
            'cpu_usage_pts': cpu_usage_pts, 'net_io_pts': net_io_pts,
            'net_tcp_conns_pts': net_tcp_conns_pts, }

  def _compute_percentiles(self, data_pts):
    np_array = np.array(data_pts)
    return {'min': min(data_pts), 'mean': np.mean(np_array), 'max': max(data_pts),
            '25': np.percentile(np_array, 25), '50': np.percentile(np_array, 50),
            '75': np.percentile(np_array, 75), '90': np.percentile(np_array, 90),
            '95': np.percentile(np_array, 95), '99': np.percentile(np_array, 99)}

  def _get_matplotlib_line_chart(self, x_pts, y_data, title, x_title, y_title, y_labels):
    fig, ax = plt.subplots()
    for y_pts, y_label in zip(y_data, y_labels):
      ax.plot(x_pts, y_pts, label=y_label)
    ax.legend(loc="upper right")
    ax.set_xlabel(x_title)
    ax.set_ylabel(y_title)
    ax.set_title(title)
    return fig

  def _compute_default_post_test_metrics(self, observations):
    # Time stamps
    start_time = observations['time_pts'][0]
    end_time = observations['time_pts'][1]
    time_pts = [time_pt - start_time for time_pt in observations['time_pts']]
    actual_run_time = time_pts[-1] - time_pts[0]

    # cpu stats
    cpu_usage_pts = [np.mean(all_cpus_per) for all_cpus_per in observations['cpu_usage_pts']]
    cpu_usage_pers = self._compute_percentiles(cpu_usage_pts)
    avg_cpu_usage = np.mean(cpu_usage_pts)

    # Network bandwidth stats
    upload_bytes_pts = []
    download_bytes_pts = []
    for net_io_pt in observations['net_io_pts']:
      upload_bytes = 0
      download_bytes = 0
      for _, net_io in net_io_pt.items():
        upload_bytes = upload_bytes + net_io.bytes_sent
        download_bytes = download_bytes + net_io.bytes_recv
      upload_bytes_pts.append(upload_bytes / MB)
      download_bytes_pts.append(download_bytes / MB)
    # Network bandwidth points (MiB/sec)
    upload_bw_pts = [0] + [(upload_bytes_pts[idx + 1] - upload_bytes_pts[idx]) / (time_pts[idx + 1] - time_pts[idx]) for idx in range(len(time_pts) - 1)]
    download_bw_pts = [0] + [(download_bytes_pts[idx + 1] - download_bytes_pts[idx]) / (time_pts[idx + 1] - time_pts[idx]) for idx in range(len(time_pts) - 1)]
    # Avg. Network bandwidth (MiB/sec)
    avg_upload_bw = (upload_bytes_pts[-1] - upload_bytes_pts[0]) / actual_run_time
    avg_download_bw = (download_bytes_pts[-1] - download_bytes_pts[0]) / actual_run_time

    # task latency stats
    task_lat_pts = [result[3] - result[2] for result in observations['tasks_results']]
    task_lat_pers = self._compute_percentiles(task_lat_pts)

    return {'start_time': start_time, 'end_time': end_time,
            'tasks_count': len(task_lat_pts),
            'tasks_per_sec': len(task_lat_pts) / actual_run_time,
            'actual_run_time': actual_run_time, 'time_pts': time_pts,
            'cpu_usage_pts': cpu_usage_pts, 'cpu_usage_pers': cpu_usage_pers,
            'avg_cpu_usage': avg_cpu_usage, 'upload_bw_pts': upload_bw_pts,
            'download_bw_pts': download_bw_pts, 'avg_upload_bw': avg_upload_bw,
            'avg_download_bw': avg_download_bw, 'task_lat_pers': task_lat_pers}

  def _dump_metrics_into_json(self, metrics, output_dir):
    with open(os.path.join(output_dir, 'metrics.json'), 'w') as fp:
      json.dump(metrics, fp)

  def _print_default_metrics(self, metrics):
    # Time metrics
    print("\nTime: ")
    print("\n\tStart time (epoch): ", metrics['start_time'])
    print("\n\tEnd time (epoch): ", metrics['end_time'])
    print("\n\tActual run time: ", metrics['actual_run_time'])

    # Task related
    print("\nTasks: ")
    print("\n\tTasks count: ", metrics['tasks_count'])
    print("\n\tTasks per sec: ", metrics['tasks_per_sec'])

    # Latency metrics
    print("\nTasks latencies: ")
    print("\tMinimum (in seconds): ", metrics['task_lat_pers'])
    print("\tMean (in seconds): ", metrics['task_lat_pers'])
    print("\t25th Percentile (in seconds): ",
          metrics['task_lat_pers']['25'])
    print("\t50th Percentile (in seconds): ",
          metrics['task_lat_pers']['50'])
    print("\t90th Percentile (in seconds): ",
          metrics['task_lat_pers']['90'])
    print("\t95th Percentile (in seconds): ",
          metrics['task_lat_pers']['95'])
    print("\tMaximum (in seconds): ", metrics['task_lat_pers']['max'])

    # CPU metrics
    print("\nCPU: ")
    print("\tAvg. CPU usage (%): ", metrics['avg_cpu_usage'])
    print("\tPeak CPU usage (%): ", metrics['cpu_usage_pers']['max'])

    # Bandwidth metrics
    print("\nNetwork bandwidth (psutil): ")
    print("\tAvg. Upload Bandwidth (MiB/sec): ", metrics['avg_upload_bw'])
    print("\tAvg. Download Bandwidth (MiB/sec): ", metrics['avg_download_bw'])

  def post_load_test(self, observations, output_dir="./", dump_metrics=True,
      print_metrics=True, **kwargs):
    metrics = self._compute_default_post_test_metrics(observations)
    bw_fig = self._get_matplotlib_line_chart(metrics['time_pts'],
                                             [metrics['upload_bw_pts'],
                                              metrics['download_bw_pts']],
                                             'Bandwidth (MiB/sec)', 'time (sec)',
                                             'MiB/sec', ['Upload', 'Download'])
    cpu_fig = self._get_matplotlib_line_chart(metrics['time_pts'],
                                              [metrics['cpu_usage_pts']],
                                              'CPU Usage (%)', 'time (sec)',
                                              '%', ['CPU'])

    # Dump metrics
    if dump_metrics:
      self._dump_metrics_into_json(metrics, output_dir)
      bw_fig.savefig(os.path.join(output_dir, 'bandwidth_variation.png'))
      cpu_fig.savefig(os.path.join(output_dir, 'cpu_variation.png'))

    # Print metrics
    if print_metrics:
      self._print_default_metrics(metrics)

    return {'metrics': metrics, 'plots': {'bandwidth': bw_fig, 'cpu': cpu_fig}}






