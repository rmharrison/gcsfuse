import datetime
import multiprocessing
import sys
import os
import time
import threading
import argparse
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import tensorflow as tf
import warnings
import psutil
import numpy as np
import pandas as pd
from datetime import datetime, timezone


def _convert_multiprocessing_queue_to_list(mp_queue):
  queue_size = mp_queue.qsize()
  return [mp_queue.get() for _ in range(queue_size)]


class LoadGenerator:

  CPU_PERCENT_INTERVAL = 1

  def __init__(self, num_processes, num_threads, run_time, time_based=1, num_tasks_per_thread=1, num_tasks=-1):
    self.num_processes = num_processes
    self.num_threads = num_threads
    self.run_time = run_time
    self.time_based = time_based
    self.num_tasks_per_thread = num_tasks_per_thread
    self.num_tasks = num_tasks
    # self.thread_queues = [[multiprocessing.Manager().Queue()] * num_threads] * num_processes
    # self.tasks_results_queue = multiprocessing.Manager().Queue()
    # self.pre_tasks_results_queue = multiprocessing.Manager().Queue()
    # self.post_tasks_results_queue = multiprocessing.Manager().Queue()

    # ToDo: Handle different cases where we need to throw error.
    if num_tasks != -1:
      self.num_tasks_per_thread = np.inf
    else:
      self.num_tasks = np.inf

  @staticmethod
  def _thread_task(assigned_thread_id, assigned_process_id, num_tasks_per_thread,
      task, tasks_results_queue, pre_tasks_results_queue,
      post_tasks_results_queue):

    cnt = 0
    while cnt < num_tasks_per_thread:
      # ToDo: Shorten/clean the code.
      start_time = time.perf_counter()
      result = task.pre_task(assigned_thread_id, assigned_process_id)
      end_time = time.perf_counter()
      pre_tasks_results_queue.put((assigned_process_id, assigned_thread_id,
                                   start_time, end_time, result))

      start_time = time.perf_counter()
      result = task.task(assigned_thread_id, assigned_process_id)
      end_time = time.perf_counter()
      tasks_results_queue.put((assigned_process_id, assigned_thread_id,
                                   start_time, end_time, result))

      start_time = time.perf_counter()
      result = task.post_task(assigned_thread_id, assigned_process_id)
      end_time = time.perf_counter()
      post_tasks_results_queue.put((assigned_process_id, assigned_thread_id,
                                   start_time, end_time, result))

      cnt = cnt + 1


  @staticmethod
  def _process_task(assigned_process_id, num_threads, num_tasks_per_thread, task,
      tasks_results_queue, pre_tasks_results_queue, post_tasks_results_queue):
    threads = []
    for thread_num in range(num_threads):
      threads.append(threading.Thread(target=LoadGenerator._thread_task,
                                      args=(thread_num, assigned_process_id,
                                            num_tasks_per_thread,
                                            task, tasks_results_queue,
                                            pre_tasks_results_queue,
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
                                        args=(process_id, self.num_threads,
                                              self.num_tasks_per_thread,
                                              task, tasks_results_queue,
                                              pre_tasks_results_queue,
                                              post_tasks_results_queue))
      processes.append(process)
      process_pids.append(process.pid)

    # Ignore the first psutil.cpu_percent call's output.
    psutil.cpu_percent()
    cpu_per_pts = [psutil.cpu_percent(interval=self.CPU_PERCENT_INTERVAL,
                                      percpu=True)]
    net_tcp_conns_pts = [psutil.net_connections(kind='tcp')]
    net_io_pts = [psutil.net_io_counters(pernic=True)]

    for process in processes:
      process.start()

    time_pts = [time.perf_counter()]
    while (time_pts[-1] - time_pts[0]) < self.run_time and (tasks_results_queue.qsize() < self.num_tasks):
      cpu_per_pts.append(psutil.cpu_percent(interval=self.CPU_PERCENT_INTERVAL,
                                            percpu=True))
      net_tcp_conns_pts.append(psutil.net_connections(kind='tcp'))
      net_io_pts.append(psutil.net_io_counters(pernic=True))
      time_pts.append(time.perf_counter())

    for process in processes:
      process.terminate()

    return {'tasks_results_queue': _convert_multiprocessing_queue_to_list(tasks_results_queue),
            'pre_tasks_results_queue': _convert_multiprocessing_queue_to_list(pre_tasks_results_queue),
            'post_tasks_results_queue': _convert_multiprocessing_queue_to_list(post_tasks_results_queue),
            'cpu_per_pts': cpu_per_pts, 'net_io_pts': net_io_pts,
            'net_tcp_conns_pts': net_tcp_conns_pts, 'time_pts': time_pts,
            'process_pids': process_pids}







