:mod:`airflow.executors.sequential_executor`
============================================

.. py:module:: airflow.executors.sequential_executor

.. autoapi-nested-parse::

   SequentialExecutor

   .. seealso::
       For more information on how the SequentialExecutor works, take a look at the guide:
       :ref:`executor:SequentialExecutor`



Module Contents
---------------

.. py:class:: SequentialExecutor

   Bases: :class:`airflow.executors.base_executor.BaseExecutor`

   This executor will only run one task instance at a time, can be used
   for debugging. It is also the only executor that can be used with sqlite
   since sqlite doesn't support multiple connections.

   Since we want airflow to work out of the box, it defaults to this
   SequentialExecutor alongside sqlite as you first install it.

   
   .. method:: execute_async(self, key: TaskInstanceKey, command: CommandType, queue: Optional[str] = None, executor_config: Optional[Any] = None)



   
   .. method:: sync(self)



   
   .. method:: end(self)

      End the executor.



   
   .. method:: terminate(self)

      Terminate the executor is not doing anything.




