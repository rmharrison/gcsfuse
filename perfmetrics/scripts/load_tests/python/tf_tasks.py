from load_generator import task
import os
from absl import logging

def _create_file_with_give_size(file_path, file_size):

def _gcsfuse_os_read_task(file_path, file_size):
    my_file = os.open(file_path, os.O_DIRECT)
    content = os.read(my_file, file_size)
    os.close(my_file)
    return len(content)

class GCSFuseRead200MB(task.LoadTestTask):

  TASK_NAME = "gcsfuse_200mb"

  TASK_TYPE = "read"
  FILE_PATH_FORMAT = "/home/ayushsethi/python-tf/fio/gcs/200mb/1_thread.{process_num}.0"
  FILE_SIZE = 200 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_id=assigned_process_id)
    return _gcsfuse_os_read_task(file_path, self.FILE_SIZE)


class GCSFuseRead100MB(task.LoadTestTask):

  TASK_NAME = "gcsfuse_200mb"

  TASK_TYPE = "read"
  FILE_PATH = "/home/ayushsethi/python-tf/fio/gcs/100mb/1_thread.{process_id}.0"
  FILE_SIZE = 100 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH.format(process_id=assigned_process_id)
    return _gcsfuse_os_read_task(file_path, self.FILE_SIZE)




