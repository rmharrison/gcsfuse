import os
import logging
from load_generator import task
import tensorflow as tf

SINGLE_RECORD_SIZE = 256 * 1024


def _create_tfrecord_file(file_path, file_size):
  # We only check existence in this case because actual TFRecord's file size is
  # not exactly equal to file_size
  if tf.io.gfile.exists(file_path):
    return
  logging.info('Creating TFRecord file {0} of size {1}.'.format(
      file_path, file_size))
  content = b'\t' * SINGLE_RECORD_SIZE
  writer = tf.io.TFRecordWriter(file_path)
  for _ in range(0, file_size, len(content)):
    writer.write(content)
  writer.close()


class TFDataRead(task.LoadTestTask):

  TASK_TYPE = 'read'
  FILE_PATH_FORMAT = ''
  FILE_SIZE = 0
  PREFETCH = 0
  NUM_PARALLEL_CALLS = 0
  NUM_FILES = 0
  SHARD = 1

  def __init__(self):
    super().__init__()
    self.file_names = [
        self.FILE_PATH_FORMAT.format(file_num=file_num)
        for file_num in range(self.NUM_FILES)
    ]

  def _tf_data_task(self, file_names):
    content_len = 0
    files_dataset = tf.data.Dataset.from_tensor_slices(file_names)

    def tfrecord(path):
      return tf.data.TFRecordDataset(path).prefetch(self.PREFETCH).shard(
          self.SHARD, 0)

    dataset = files_dataset.interleave(
        tfrecord,
        cycle_length=self.NUM_PARALLEL_CALLS,
        num_parallel_calls=self.NUM_PARALLEL_CALLS)
    for record in dataset:
      content_len = content_len + len(record.numpy())

    return content_len

  def create_files(self, num_processes):
    logging.info('Creating {0} bytes TFRecord files using the format '
                 '{1}'.format(self.FILE_SIZE, self.FILE_PATH_FORMAT))

    file_path_0 = self.FILE_PATH_FORMAT.format(file_num=0)
    _create_tfrecord_file(file_path_0, self.FILE_SIZE)
    for file_num in range(1, self.NUM_FILES):
      file_path = self.FILE_PATH_FORMAT.format(file_num=file_num)
      if not tf.io.gfile.exists(file_path):
        logging.info('Creating TFRecord file {0} of size {1}.'.format(
            file_path, self.FILE_SIZE))
        tf.io.gfile.copy(file_path_0, file_path)

  def pre_task(self, assigned_process_id, assigned_thread_id):
    # Clear the page cache as there is no way to bypass cache in tf.data.
    # Note: This takes time and hence decrease the average bandwidth.
    os.system("sudo sh -c 'sync; echo 3 >  /proc/sys/vm/drop_caches'")


class TFDataReadGCSFuseAutotune100MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_100MB_AUTOTUNE'

  FILE_PATH_FORMAT = '/gcs/100mb/read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = -1
  NUM_PARALLEL_CALLS = -1
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuse16P100MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_100MB_16P'

  FILE_PATH_FORMAT = '/gcs/100mb/read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 16
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuse64P100MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_100MB_64P'

  FILE_PATH_FORMAT = '/gcs/100mb/read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 64
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuse100P100MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_100MB_100P'

  FILE_PATH_FORMAT = '/gcs/100mb/read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuseShard100P100MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_SHARD_100MB_100P'

  FILE_PATH_FORMAT = '/gcs/100mb/read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024
  SHARD = 800

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadAutotune100MB(TFDataRead):

  TASK_NAME = 'TF_100MB_AUTOTUNE'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/100mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = -1
  NUM_PARALLEL_CALLS = -1
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataRead16P100MB(TFDataRead):

  TASK_NAME = 'TF_100MB_16P'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/100mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = -16
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataRead64P100MB(TFDataRead):

  TASK_NAME = 'TF_100MB_64P'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/100mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 64
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataRead100P100MB(TFDataRead):

  TASK_NAME = 'TF_100MB_100P'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/100mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadShard100P100MB(TFDataRead):

  TASK_NAME = 'TF_SHARD_100MB_100P'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/100mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 100 * 1024 * 1024
  PREFETCH = 100
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024
  SHARD = 800

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuseAutotune200MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_200MB_AUTOTUNE'

  FILE_PATH_FORMAT = '/gcs/200mb/read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = -1
  NUM_PARALLEL_CALLS = -1
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuse16P200MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_200MB_16P'

  FILE_PATH_FORMAT = '/gcs/200mb/read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 16
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuse64P200MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_200MB_64P'

  FILE_PATH_FORMAT = '/gcs/200mb/read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 64
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuse100P200MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_200MB_100P'

  FILE_PATH_FORMAT = '/gcs/200mb/read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadGCSFuseShard100P200MB(TFDataRead):

  TASK_NAME = 'GCSFUSE_SHARD_200MB_100P'

  FILE_PATH_FORMAT = '/gcs/200mb/read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024
  SHARD = 800

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadAutotune200MB(TFDataRead):

  TASK_NAME = 'TF_200MB_AUTOTUNE'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/200mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = -1
  NUM_PARALLEL_CALLS = -1
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataRead16P200MB(TFDataRead):

  TASK_NAME = 'TF_200MB_16P'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/200mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = -16
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataRead64P200MB(TFDataRead):

  TASK_NAME = 'TF_200MB_64P'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/200mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 64
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataRead100P200MB(TFDataRead):

  TASK_NAME = 'TF_200MB_100P'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/200mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)


class TFDataReadShard100P200MB(TFDataRead):

  TASK_NAME = 'TF_SHARD_200MB_100P'

  FILE_PATH_FORMAT = 'gs://load-test-bucket/python/files/200mb/' \
                     'read.{file_num}.tfrecord'
  FILE_SIZE = 200 * 1024 * 1024
  PREFETCH = 200
  NUM_PARALLEL_CALLS = 100
  NUM_FILES = 1024
  SHARD = 800

  def task(self, assigned_process_id, assigned_thread_id):
    return self._tf_data_task(self.file_names)
