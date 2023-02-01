from abc import ABC, abstractmethod

class LoadTestTask(ABC):

  TASK_NAME = "ABSTRACT_TASK"

  def pre_task(self, assigned_process_id, assigned_thread_id):
    pass

  @abstractmethod
  def task(self, assigned_process_id, assigned_thread_id):
    pass

  def post_task(self, assigned_process_id, assigned_thread_id):
    pass



