from load_generator import task
import os

class ReadFromOSTask(task.LoadTestTask):
  TASK_NAME = "Read_100MiB_GCSFUSE"

  FILE_PATH = "/home/ayushsethi/python-tf/fio/gcs/200mb/1_thread.{process_id}.0"
  FILE_SIZE = 200 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    filepath = self.FILE_PATH.format(process_id=assigned_process_id)
    my_file = os.open(filepath, os.O_DIRECT)
    content = os.read(my_file, self.FILE_SIZE)
    os.close(my_file)
    return len(content)

  def post_load_test(self, **kwargs):
    default_results = super().post_load_test(**kwargs)
    metrics = default_results['metrics']

    # compute bandwidth from task results
    total_io_bytes = sum([task_result[4]
                          for task_result in kwargs['tasks_results_queue']])
    avg_computed_net_bw = total_io_bytes / metrics['actual_run_time']
    metrics.update({'avg_computed_net_bw': avg_computed_net_bw})

    # dump the results
    self._dump_results_into_json(metrics, kwargs['output_dir'])
    print("Avg. computed BW: ", metrics['avg_computed_net_bw'])
    print("Avg. download network BW: ", metrics['avg_download_bw'])
    print("Avg. upload network BW: ", metrics['avg_upload_bw'])
    print("Avg. CPU utilization(%): ", metrics[''])
    return {'metrics': metrics}


