import datetime
import multiprocessing
import sys
import os
import time
import threading
import argparse
import tensorflow as tf
import warnings
import psutil
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from absl import logging

MIN_RUN_TIME_IN_SECS = 4
MIN_OBSERVATION_INTERVAL_IN_SECS = 2

def _convert_multiprocessing_queue_to_list(mp_queue):
  queue_size = mp_queue.qsize()
  return [mp_queue.get() for _ in range(queue_size)]

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

    # Checks on load test setup configuration.
    if num_tasks_per_thread != sys.maxsize & num_tasks != sys.maxsize:
      raise Exception("num_tasks_per_thread and num_tasks both can't be passed.")

    if run_time == sys.maxsize & num_tasks_per_thread == sys.maxsize & num_threads == sys.maxsize:
      raise Exception("Out of run_time, num_tasks_per_thread and num_threads, "
                      "one has to be passed.")

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


  @staticmethod
  def _thread_task(task, assigned_thread_id, assigned_process_id,
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
                                      args=(task, thread_num, assigned_process_id,
                                            num_tasks_per_thread,
                                            pre_tasks_results_queue,
                                            tasks_results_queue,
                                            post_tasks_results_queue)))

    for thread in threads:
      thread.daemon = True
      thread.start()

    for thread in threads:
      thread.join()

  def generate(self, task):
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
    cpu_per_pts = [psutil.cpu_percent(interval=self.observation_interval,
                                      percpu=True)]
    net_tcp_conns_pts = [psutil.net_connections(kind='tcp')]
    net_io_pts = [psutil.net_io_counters(pernic=True)]

    for process in processes:
      process.start()

    time_pts = [time.perf_counter()]
    while (time_pts[-1] - time_pts[0]) < self.run_time and (tasks_results_queue.qsize() < self.num_tasks):
      # psutil.cpu_percent is blocking call for the process (and its core) in
      # which it runs.
      cpu_per_pts.append(psutil.cpu_percent(interval=self.observation_interval,
                                            percpu=True))
      net_tcp_conns_pts.append(psutil.net_connections(kind='tcp'))
      net_io_pts.append(psutil.net_io_counters(pernic=True))
      time_pts.append(time.perf_counter())

    for process in processes:
      process.terminate()

    return {'tasks_results': _convert_multiprocessing_queue_to_list(tasks_results_queue),
            'pre_tasks_results': _convert_multiprocessing_queue_to_list(pre_tasks_results_queue),
            'post_tasks_results': _convert_multiprocessing_queue_to_list(post_tasks_results_queue),
            'cpu_per_pts': cpu_per_pts, 'net_io_pts': net_io_pts,
            'net_tcp_conns_pts': net_tcp_conns_pts, 'time_pts': time_pts,
            'process_pids': process_pids}







