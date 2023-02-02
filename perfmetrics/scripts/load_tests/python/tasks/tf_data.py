import os
from absl import logging
from load_generator import task
import tensorflow as tf

SINGLE_RECORD_SIZE = 256 * 1024 * 1024

def _create_tfrecord_file(file_path, file_size):
  # We only check existence in this case because actual TFRecord's file size is
  # not exactly equal to file_size
  if os.path.exists(file_path):
    return
  logging.info("Creating TFRecord file {0} of size {1}.".format(file_path,
                                                                file_size))
  content = b'\t' * SINGLE_RECORD_SIZE
  writer = tf.io.TFRecordWriter(file_path)
  for _ in range(file_size, len(content)):
    writer.write(content)
  writer.close()

class TFDataRead(task.LoadTestTask):

  TASK_TYPE = "read"
  FILE_PATH_FORMAT = ""
  FILE_SIZE = 0
  PREFETCH = 0
  NUM_PARALLEL_CALLS = 0
  NUM_FILES = 0
  SHARD = 1

  def __init__(self):
    super().__init__()
    self.file_names = [self.FILE_PATH_FORMAT.format(file_num=file_num)
                       for file_num in range(self.NUM_FILES)]

  def _tf_data_task(self, file_names):
    content_len = 0
    files_dataset = tf.data.Dataset.from_tensor_slices(file_names)
    def tfrecord(path):
      return tf.data.TFRecordDataset(path).prefetch(self.PREFETCH).shard(self.SHARD, 0)

    dataset = files_dataset.interleave(tfrecord,
                                       cycle_length=self.NUM_PARALLEL_CALLS,
                                       num_parallel_calls=self.NUM_PARALLEL_CALLS)
    for record in dataset:
      content_len = content_len + len(record.numpy())

    return content_len

  def create_files(self, file_path_format, file_size, num_processes):
    if self.FILE_PATH_FORMAT == "" or self.FILE_SIZE == 0:
      raise Exception("Task of types - read or write must have non empty "
                      "FILE_PATH_FORMAT and non zero FILE_SIZE (in bytes) "
                      "attributes set.")

    logging.info("Creating {0} TFRecord files using the format "
                 "{1}".format(file_size, file_path_format))
    for file_num in range(self.NUM_FILES):
      file_path = file_path_format.format(file_num=file_num)
      _create_tfrecord_file(file_path, file_size)
    return


class TFDataReadGCSFuseAutotune100MB(TFDataRead):

  TASK_NAME = "GCSFUSE_100MB_AUTOTUNE"

  FILE_PATH_FORMAT = "/gcs/100mb/read.{file_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = -1
  NUM_PARALLEL_CALLS = -1
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadGCSFuse16P100MB(TFDataRead):

  TASK_NAME = "GCSFUSE_100MB_16P"

  FILE_PATH_FORMAT = "/gcs/100mb/read.{file_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 16
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadGCSFuse64P100MB(TFDataRead):

  TASK_NAME = "GCSFUSE_100MB_64P"

  FILE_PATH_FORMAT = "/gcs/100mb/read.{file_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 64
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadGCSFuse100P100MB(TFDataRead):

  TASK_NAME = "GCSFUSE_100MB_100P"

  FILE_PATH_FORMAT = "/gcs/100mb/read.{file_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadGCSFuseShard100P100MB(TFDataRead):

  TASK_NAME = "GCSFUSE_SHARD_100MB_100P"

  FILE_PATH_FORMAT = "/gcs/100mb/read.{file_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024
  SHARD = 800

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadAutotune100MB(TFDataRead):

  TASK_NAME = "TF_100MB_AUTOTUNE"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/100mb/read.{process_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = -1
  NUM_PARALLEL_CALLS = -1
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataRead16P100MB(TFDataRead):

  TASK_NAME = "TF_100MB_16P"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/100mb/read.{process_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = -16
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataRead64P100MB(TFDataRead):

  TASK_NAME = "TF_100MB_64P"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/100mb/read.{process_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 64
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataRead100P100MB(TFDataRead):

  TASK_NAME = "TF_100MB_100P"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/100mb/read.{process_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadShard100P100MB(TFDataRead):

  TASK_NAME = "TF_SHARD_100MB_100P"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/100mb/read.{process_num}.tfrecord"
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024
  SHARD = 800

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)
  
class TFDataReadGCSFuseAutotune200MB(TFDataRead):

  TASK_NAME = "GCSFUSE_200MB_AUTOTUNE"

  FILE_PATH_FORMAT = "/gcs/200mb/read.{file_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = -1
  NUM_PARALLEL_CALLS = -1
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadGCSFuse16P200MB(TFDataRead):

  TASK_NAME = "GCSFUSE_200MB_16P"

  FILE_PATH_FORMAT = "/gcs/200mb/read.{file_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 16
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadGCSFuse64P200MB(TFDataRead):

  TASK_NAME = "GCSFUSE_200MB_64P"

  FILE_PATH_FORMAT = "/gcs/200mb/read.{file_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 64
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadGCSFuse100P200MB(TFDataRead):

  TASK_NAME = "GCSFUSE_200MB_100P"

  FILE_PATH_FORMAT = "/gcs/200mb/read.{file_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadGCSFuseShard100P200MB(TFDataRead):

  TASK_NAME = "GCSFUSE_SHARD_200MB_100P"

  FILE_PATH_FORMAT = "/gcs/200mb/read.{file_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024
  SHARD = 800

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadAutotune200MB(TFDataRead):

  TASK_NAME = "TF_200MB_AUTOTUNE"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/200mb/read.{process_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = -1
  NUM_PARALLEL_CALLS = -1
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataRead16P200MB(TFDataRead):

  TASK_NAME = "TF_200MB_16P"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/200mb/read.{process_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = -16
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataRead64P200MB(TFDataRead):

  TASK_NAME = "TF_200MB_64P"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/200mb/read.{process_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 64
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataRead100P200MB(TFDataRead):

  TASK_NAME = "TF_200MB_100P"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/200mb/read.{process_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

class TFDataReadShard100P200MB(TFDataRead):

  TASK_NAME = "TF_SHARD_200MB_100P"

  FILE_PATH_FORMAT = "gs://load-test-bucket/python/files/200mb/read.{process_num}.tfrecord"
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024
  SHARD = 800

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)

