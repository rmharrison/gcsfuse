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
import matplotlib.pyplot as plt
import json


class LoadTestTask:
  TASK_NAME = "ABSTRACT_TASK"

  def pre_load_test(self, num_processes, num_threads, run_time, time_based=1,
      num_tasks_per_thread=1, num_tasks=-1):
    pass

  def pre_task(self, assigned_process_id, assigned_thread_id):
    pass

  def task(self, assigned_process_id, assigned_thread_id):
    pass

  def post_task(self, assigned_process_id, assigned_thread_id):
    pass

  def _compute_percentiles(self, data_pts):
    np_array = np.array(data_pts)
    return {'min': min(data_pts), 'mean': np.mean(np_array), 'max': max(data_pts),
            '25': np.percentile(np_array, 25), '50': np.percentile(np_array, 50),
            '75': np.percentile(np_array, 75), '90': np.percentile(np_array, 90),
            '95': np.percentile(np_array, 95), '99': np.percentile(np_array, 99)}

  def _get_matplotlib_line_chart(self, x_pts, y_data, title, x_label, y_label, y_labels):
    fig, ax = plt.subplots()
    for y_pts, y_label in zip(y_data, y_labels):
      ax.plot(x_pts, y_pts, label=y_label)
    ax.legend(loc="upper right")
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title)
    return fig

  def _compute_default_post_test_metrics(self, **kwargs):
    # simple time points
    start_time = kwargs['time_pts'][0]
    time_pts = [time_pt -start_time for time_pt in kwargs['time_pts']]
    print(time_pts[-1], time_pts[0])
    actual_run_time = time_pts[-1] - time_pts[0]
    # cpu stats
    cpu_pts = [np.mean(all_cpus_per) for all_cpus_per in kwargs['cpu_per_pts']]
    cpu_lat_pers = self._compute_percentiles(cpu_pts)
    # upload and download bw stats
    upload_io_pts = []
    download_io_pts = []
    for idx in range(len(kwargs['net_io_pts'])):
      upload_io = 0
      download_io = 0
      for _, net_io in kwargs['net_io_pts'][idx].items():
        upload_io = upload_io + net_io.bytes_sent
        download_io = download_io + net_io.bytes_recv
      upload_io_pts.append(upload_io / (1024 * 1024 * 1024))
      download_io_pts.append(download_io / (1024 * 1024 * 1024))
    upload_bw_pts = [0] + [(upload_io_pts[idx + 1] - upload_io_pts[idx]) / (time_pts[idx + 1] - time_pts[idx]) for idx in range(len(time_pts) - 1)]
    download_bw_pts = [0] + [(download_io_pts[idx + 1] - download_io_pts[idx]) / (time_pts[idx + 1] - time_pts[idx]) for idx in range(len(time_pts) - 1)]
    avg_upload_bw = (upload_io_pts[-1] - upload_io_pts[0]) / actual_run_time
    avg_download_bw = (download_io_pts[-1] - download_io_pts[0]) / actual_run_time
    # task latency stats
    task_lat_pts = [result[3] - result[2] for result in kwargs['tasks_results_queue']]
    task_lat_pers = self._compute_percentiles(task_lat_pts)
    return {'actual_run_time': actual_run_time, 'cpu_pts': cpu_pts, 'time_pts': time_pts,
            'cpu_lat_pers': cpu_lat_pers, 'upload_bw_pts': upload_bw_pts,
            'download_bw_pts': download_bw_pts, 'avg_upload_bw': avg_upload_bw,
            'avg_download_bw': avg_download_bw, 'task_lat_pers': task_lat_pers}

  def post_load_test(self, **kwargs):
    metrics = self._compute_default_post_test_metrics(**kwargs)
    bw_fig = self._get_matplotlib_line_chart(metrics['time_pts'],
                                             [metrics['upload_bw_pts'], metrics['download_bw_pts']],
                                             'Bandwidth (MiB/sec)', 'time (sec)',
                                             'MiB/sec', ['Upload', 'Download'])
    cpu_fig = self._get_matplotlib_line_chart(metrics['time_pts'],
                                             [metrics['cpu_pts']],
                                             'CPU Usage (%)', 'time (sec)',
                                             '%', ['CPU'])

    output_dir = os.path.join(kwargs['output_dir'] , self.TASK_NAME, "")
    if not os.path.exists(output_dir):
      os.makedirs(output_dir)
    with open(os.path.join(output_dir, 'metrics.json'), 'w') as fp:
      json.dump(metrics, fp)

    bw_fig.savefig(os.path.join(output_dir, 'bandwidth.png'))
    cpu_fig.savefig(os.path.join(output_dir, 'cpu.png'))

    return {'metrics': metrics, 'plots': {'bandwidth': bw_fig, 'cpu': cpu_fig}}

