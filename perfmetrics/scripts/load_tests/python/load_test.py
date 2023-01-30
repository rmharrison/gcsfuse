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
warnings.filterwarnings("ignore")
import inspect
import importlib
from load_generator import load_generator as lg

def parse_args():
  # ToDo: Correct the description and defaults.
  parser = argparse.ArgumentParser(description='Load testing using multiprocessing')
  parser.add_argument('--task-file-path', type=str,
                      help='Path to task file.')
  parser.add_argument('--output-dir', type=str, default='./output/',
                      help='Path to task file.')
  parser.add_argument('--num-processes', type=int, default=1,
                      help='Number of processes to use.')
  parser.add_argument('--num-threads', type=int, default=1,
                      help='Number of threads to use in each process.')
  parser.add_argument('--run-time', type=int, default=60,
                      help='Number of seconds to run the load test.')
  parser.add_argument('--num-tasks', type=int, default=sys.maxsize,
                      help='Number of seconds to run the load test.')
  parser.add_argument('--num-tasks-per-thread', type=int, default=sys.maxsize,
                      help='')
  parser.add_argument('--cooling-time', type=int, default=30,
                      help='')
  args = parser.parse_args()
  return args

def main():
  args = parse_args()
  lg_obj = lg.LoadGenerator(args.num_processes, args.num_threads, args.run_time, 1, args.num_tasks_per_thread, args.num_tasks)
  for name, cls in inspect.getmembers(importlib.import_module(args.task_file_path), inspect.isclass):
    print(name)
    if cls.__module__ != args.task_file_path:
      continue
    print(name)
    obj = cls()
    results = lg_obj.generate(obj)
    results.update({'output_dir': args.output_dir})
    data = obj.post_load_test(**results)

  return


if __name__ == "__main__":
  main()