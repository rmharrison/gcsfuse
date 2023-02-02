import logging
import importlib
import importlib.machinery
import importlib.util
import inspect
import os
import sys
import time
import argparse

from load_generator import load_generator as lg
from load_generator import task


class LoadGeneratorForReadAndWriteTask(lg.LoadGenerator):

  TASK_TYPES = ['read', 'write']

  def pre_load_test(self, **kwargs):
    # only run custom logic for read and write tasks
    if getattr(kwargs['task'], 'TASK_TYPE', '').lower() not in self.TASK_TYPES:
      return

    if (not hasattr(kwargs['task'], 'FILE_PATH_FORMAT')) or \
        (not hasattr(kwargs['task'], 'FILE_SIZE')):
      raise ValueError(
          'Task of types - read or write must have FILE_PATH_FORMAT'
          ' and FILE_SIZE (in bytes) attributes set.'
          'method set.')
    if not hasattr(kwargs['task'], 'create_files'):
      raise NotImplementedError(
          'Task of types - read or write must have create '
          'files function to create files before generating '
          'load.')

    file_path_format = getattr(kwargs['task'], 'FILE_PATH_FORMAT')
    file_size = getattr(kwargs['task'], 'FILE_SIZE')

    if (file_path_format == '') or (file_size == 0):
      raise ValueError("Constant FILE_PATH_FORMAT can't be empty and value"
                       "of FILE_SIZE can't be zero.")
    kwargs['task'].create_files(self.num_processes)

  def post_load_test(self,
                     observations,
                     output_dir='./',
                     dump_metrics=True,
                     print_metrics=True,
                     **kwargs):

    metrics = super().post_load_test(observations, output_dir, dump_metrics,
                                     print_metrics)
    # only run custom logic for read and write tasks
    if getattr(kwargs['task'], 'TASK_TYPE', '').lower() not in self.TASK_TYPES:
      return metrics

    metrics = metrics['metrics']

    # compute bandwidth from task results
    total_io_bytes = sum(
        (task_result[4] for task_result in observations['tasks_results']))
    avg_computed_net_bw = total_io_bytes / metrics['actual_run_time']
    avg_computed_net_bw = avg_computed_net_bw / lg.MB
    metrics.update({'avg_computed_net_bw': avg_computed_net_bw})

    # dump metrics
    self._dump_metrics_into_json(metrics, output_dir)

    # print additional metrics
    print('\nNetwork bandwidth (computed by Sum(task response) / actual '
          'run time):')
    print('\tAvg. bandwidth (MiB/sec): ', metrics['avg_computed_net_bw'], '\n')
    return metrics


def import_module_using_src_code_path(src_code_path):
  module_name = src_code_path.split('/')[-1].replace('.py', '')
  loader = importlib.machinery.SourceFileLoader(module_name, src_code_path)
  spec = importlib.util.spec_from_loader(loader.name, loader)
  mod = importlib.util.module_from_spec(spec)
  loader.exec_module(mod)
  return mod


def parse_args():
  # ToDo: Correct the description and defaults.
  parser = argparse.ArgumentParser(
      description='Load testing using multiprocessing')
  parser.add_argument('--task-file-path', type=str, help='Path to task file.')
  parser.add_argument('--task-names', default='', help='')
  parser.add_argument(
      '--output-dir', type=str, default='./output/', help='Path to task file.')
  parser.add_argument(
      '--num-processes',
      type=int,
      default=1,
      help='Number of processes to use.')
  parser.add_argument(
      '--num-threads',
      type=int,
      default=1,
      help='Number of threads to use in each process.')
  parser.add_argument(
      '--run-time',
      type=int,
      default=sys.maxsize,
      help='Number of seconds to run the load test.')
  parser.add_argument(
      '--num-tasks',
      type=int,
      default=sys.maxsize,
      help='Number of seconds to run the load test.')
  parser.add_argument(
      '--num-tasks-per-thread', type=int, default=sys.maxsize, help='')
  parser.add_argument(
      '--observation-interval',
      type=int,
      default=lg.MIN_OBSERVATION_INTERVAL_IN_SECS,
      help='')
  parser.add_argument('--cooling-time', type=int, default=10, help='')
  parser.add_argument('--only-print', action='store_true', help='')
  parser.add_argument(
      '--log-level', type=str, default='INFO', help='Path to task file.')
  args = parser.parse_args()
  args.task_names = args.task_names.replace(' ', '').split(',')
  args.task_names = [el for el in args.task_names if len(el)]
  return args


def compare_and_dump_metrics_into_csv(output_dir, load_test_results):
  heading = [
      'S.No', 'Task Name', 'Avg. Download Bandwidth', 'Avg. Upload Bandwidth',
      'Avg. Bandwidth (computed)', 'Avg. CPU usage', 'Peak CPU usage',
      'Avg. Latency', 'Max. Latency'
  ]
  lines = [','.join(heading)]
  for idx, result in enumerate(load_test_results):
    line = [
        idx + 1, result[0], result[1]['avg_download_bw'],
        result[1]['avg_upload_bw'], result[1].get('avg_computed_net_bw', ''),
        result[1]['avg_cpu_usage'], result[1]['cpu_usage_pers']['max'],
        result[1]['task_lat_pers']['mean'], result[1]['task_lat_pers']['max']
    ]
    lines.append(','.join(list(map(str, line))) + '\n')

  with open(
      os.path.join(output_dir, 'comparison.csv'), 'w', encoding='UTF-8') as f_p:
    f_p.writelines(lines)


def main():
  args = parse_args()

  logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

  logging.info('Initialising Load Generator...')
  lg_obj = LoadGeneratorForReadAndWriteTask(
      num_processes=args.num_processes,
      num_threads=args.num_threads,
      run_time=args.run_time,
      num_tasks_per_thread=args.num_tasks_per_thread,
      num_tasks=args.num_tasks,
      observation_interval=args.observation_interval)

  logging.info('Starting load generation...')
  mod = import_module_using_src_code_path(args.task_file_path)
  mod_classes = [cls for _, cls in inspect.getmembers(mod, inspect.isclass)]
  load_test_results = []
  for idx, cls in enumerate(mod_classes):

    # Skip classes imported in the task file
    if cls.__module__ != mod.__name__:
      continue
    # Skip classes that are not of type (Todo: add reference here) or don't
    # have implementation.
    if (not issubclass(cls, task.LoadTestTask)) or (inspect.isabstract(cls)):
      continue
    # Skip if user only wants to run for a specific task
    if len(args.task_names) and (cls.TASK_NAME not in args.task_names):
      continue

    task_obj = cls()
    logging.info('\nRunning pre load test task for: {0}'.format(cls.TASK_NAME))
    lg_obj.pre_load_test(task=task_obj)

    logging.info('Generating load for: {0}'.format(cls.TASK_NAME))

    observations = lg_obj.generate_load(task_obj)

    output_dir = os.path.join(args.output_dir, cls.TASK_NAME)
    if not os.path.exists(output_dir):
      os.makedirs(output_dir)

    logging.info('Running post load test task for: {0}'.format(cls.TASK_NAME))
    dump_metrics = not args.only_print
    metrics = lg_obj.post_load_test(
        observations,
        output_dir=output_dir,
        dump_metrics=dump_metrics,
        print_metrics=True,
        task=task_obj)
    load_test_results.append((cls.TASK_NAME, metrics))

    logging.info('Load test completed for task: {0}'.format(cls.TASK_NAME))

    # don't sleep if it is last test.
    if idx < (len(mod_classes) - 1):
      logging.info('Sleeping for {0} seconds...'.format(args.cooling_time))
      time.sleep(args.cooling_time)

  # Create a comparison between all tasks and dump the csv.
  if (len(load_test_results) > 0) & (not args.only_print):
    logging.info('Writing comparison of all tasks to csv...')
    compare_and_dump_metrics_into_csv(args.output_dir, load_test_results)


if __name__ == '__main__':
  main()
