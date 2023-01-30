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

