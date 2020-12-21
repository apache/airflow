:mod:`airflow.jobs.local_task_job`
==================================

.. py:module:: airflow.jobs.local_task_job


Module Contents
---------------

.. py:class:: LocalTaskJob(task_instance: TaskInstance, ignore_all_deps: bool = False, ignore_depends_on_past: bool = False, ignore_task_deps: bool = False, ignore_ti_state: bool = False, mark_success: bool = False, pickle_id: Optional[str] = None, pool: Optional[str] = None, *args, **kwargs)

   Bases: :class:`airflow.jobs.base_job.BaseJob`

   LocalTaskJob runs a single task instance.

   .. attribute:: __mapper_args__
      

      

   
   .. method:: _execute(self)



   
   .. method:: on_kill(self)



   
   .. method:: heartbeat_callback(self, session=None)

      Self destruct task if state has been moved away from running externally




