import os
from absl import logging
from load_generator import task

def _create_binary_file(file_path, file_size):
  if os.path.exists(file_path) and os.path.getsize(file_path) == file_size:
    return
  logging.info("Creating file {0} of size {1}.".format(file_path, file_size))
  with open(file_path, "wb") as fp:
    fp.truncate(file_size)

class OSRead(task.LoadTestTask):

  TASK_TYPE = "read"

  FILE_PATH_FORMAT = ""
  FILE_SIZE = 0
  BLOCK_SIZE = 0

  def _os_direct_read_task(self, file_path, file_size, block_size):
    my_file = os.open(file_path, os.O_DIRECT)
    fp = open(my_file, 'rb')
    content_len = 0
    for _ in range(file_size, block_size):
      content = fp.read(block_size)
      content_len = content_len + len(content)
    fp.close()
    return content_len

  def create_files(self, file_path_format, file_size, num_processes):
    if self.FILE_PATH_FORMAT == "" or self.FILE_SIZE == 0:
      raise Exception("Task of types - read or write must have non empty "
                      "FILE_PATH_FORMAT and non zero FILE_SIZE (in bytes) "
                      "attributes set.")

    # Create one file per process for read and write tasks.
    if not os.path.exists(os.path.dirname(file_path_format)):
      raise Exception("Directory containing files for task not exists.")

    logging.info("One file is created per process of size {0} using the format "
                 "{1}".format(file_size, file_path_format))
    for process_num in range(num_processes):
      file_path = file_path_format.format(process_num=process_num)
      _create_binary_file(file_path, file_size)
    return

class OSRead256KB(OSRead):

  TASK_NAME = "256KB"

  FILE_PATH_FORMAT = "/gcs/256kb/read.{process_num}.0"
  FILE_SIZE = 256 * 1024
  BLOCK_SIZE = 256 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._os_direct_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)

class OSRead3MB(OSRead):

  TASK_NAME = "3mb"

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._os_direct_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)

class OSRead5MB(OSRead):

  TASK_NAME = "5mb"

  FILE_PATH_FORMAT = "/gcs/5mb/read.{process_num}.0"
  FILE_SIZE = 5 * 1024 * 1024
  BLOCK_SIZE = 5 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._os_direct_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)

class OSRead50MB(OSRead):

  TASK_NAME = "50mb"

  FILE_PATH_FORMAT = "/gcs/50mb/read.{process_num}.0"
  FILE_SIZE = 50 * 1024 * 1024
  BLOCK_SIZE = 50 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._os_direct_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)

class OSRead100MB(OSRead):

  TASK_NAME = "100mb"

  FILE_PATH_FORMAT = "/gcs/100mb/read.{process_num}.0"
  FILE_SIZE = 100 * 1024 * 1024
  BLOCK_SIZE = 100 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._os_direct_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)

class OSRead200MB(OSRead):

  TASK_NAME = "200mb"

  FILE_PATH_FORMAT = "/gcs/200mb/read.{process_num}.0"
  FILE_SIZE = 200 * 1024 * 1024
  BLOCK_SIZE = 200 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._os_direct_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)







