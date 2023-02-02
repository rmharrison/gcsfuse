import logging
from load_generator import task
import tensorflow as tf


def _create_binary_file(file_path, file_size):
  if tf.io.gfile.exists(file_path) and tf.io.gfile.stat(
      file_path).length == file_size:
    return
  logging.info('Creating file {0} of size {1}.'.format(file_path, file_size))
  with tf.io.gfile.GFile(file_path, 'wb') as f_p:
    content = b'\t' * file_size
    f_p.write(content)


class TFGFileRead(task.LoadTestTask):

  TASK_TYPE = 'read'
  FILE_PATH_FORMAT = ''
  FILE_SIZE = 0
  BLOCK_SIZE = 0

  def _tf_read_task(self, file_path, file_size, block_size):
    content_len = 0
    with tf.io.gfile.GFile(file_path, 'rb') as fp:
      for _ in range(0, file_size, block_size):
        content = fp.read(block_size)
        content_len = content_len + len(content)
      fp.close()
    return content_len

  def create_files(self, num_processes):
    logging.info('One file is created per process of size {0} using the format '
                 '{1}'.format(self.FILE_SIZE, self.FILE_PATH_FORMAT))
    for process_num in range(num_processes):
      file_path = self.FILE_PATH_FORMAT.format(process_num=process_num)
      _create_binary_file(file_path, self.FILE_SIZE)


class TFGFileRead256KB(TFGFileRead):

  TASK_NAME = '256kb'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/256kb/' \
                     'read.{process_num}.0'
  FILE_SIZE = 256 * 1024
  BLOCK_SIZE = 256 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._tf_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)


class TFGFileRead3MB(TFGFileRead):

  TASK_NAME = '3mb'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/3mb/' \
                     'read.{process_num}.0'
  FILE_SIZE = 3 * 1024 * 1024
  BLOCK_SIZE = 3 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._tf_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)


class TFGFileRead5MB(TFGFileRead):

  TASK_NAME = '5mb'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/5mb/' \
                     'read.{process_num}.0'
  FILE_SIZE = 5 * 1024 * 1024
  BLOCK_SIZE = 5 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._tf_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)


class TFGFileRead50MB(TFGFileRead):

  TASK_NAME = '50mb'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/50mb/' \
                     'read.{process_num}.0'
  FILE_SIZE = 50 * 1024 * 1024
  BLOCK_SIZE = 50 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._tf_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)


class TFGFileRead100MB(TFGFileRead):

  TASK_NAME = '100mb'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/100mb/' \
                     'read.{process_num}.0'
  FILE_SIZE = 100 * 1024 * 1024
  BLOCK_SIZE = 100 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._tf_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)


class TFGFileRead200MB(TFGFileRead):

  TASK_NAME = '200mb'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/200mb/' \
                     'read.{process_num}.0'
  FILE_SIZE = 200 * 1024 * 1024
  BLOCK_SIZE = 200 * 1024 * 1024

  def task(self, assigned_process_id, assigned_thread_id):
    file_path = self.FILE_PATH_FORMAT.format(process_num=assigned_process_id)
    return self._tf_read_task(file_path, self.FILE_SIZE, self.BLOCK_SIZE)
