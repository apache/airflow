:mod:`airflow.executors.debug_executor`
=======================================

.. py:module:: airflow.executors.debug_executor

.. autoapi-nested-parse::

   DebugExecutor

   .. seealso::
       For more information on how the DebugExecutor works, take a look at the guide:
       :ref:`executor:DebugExecutor`



Module Contents
---------------

.. py:class:: DebugExecutor

   Bases: :class:`airflow.executors.base_executor.BaseExecutor`

   This executor is meant for debugging purposes. It can be used with SQLite.

   It executes one task instance at time. Additionally to support working
   with sensors, all sensors ``mode`` will be automatically set to "reschedule".

   .. attribute:: _terminated
      

      

   
   .. method:: execute_async(self, *args, **kwargs)

      The method is replaced by custom trigger_task implementation.



   
   .. method:: sync(self)



   
   .. method:: _run_task(self, ti: TaskInstance)



   
   .. method:: queue_task_instance(self, task_instance: TaskInstance, mark_success: bool = False, pickle_id: Optional[str] = None, ignore_all_deps: bool = False, ignore_depends_on_past: bool = False, ignore_task_deps: bool = False, ignore_ti_state: bool = False, pool: Optional[str] = None, cfg_path: Optional[str] = None)

      Queues task instance with empty command because we do not need it.



   
   .. method:: trigger_tasks(self, open_slots: int)

      Triggers tasks. Instead of calling exec_async we just
      add task instance to tasks_to_run queue.

      :param open_slots: Number of open slots



   
   .. method:: end(self)

      When the method is called we just set states of queued tasks
      to UPSTREAM_FAILED marking them as not executed.



   
   .. method:: terminate(self)



   
   .. method:: change_state(self, key: TaskInstanceKey, state: str, info=None)




