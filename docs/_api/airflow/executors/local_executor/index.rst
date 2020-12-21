:mod:`airflow.executors.local_executor`
=======================================

.. py:module:: airflow.executors.local_executor

.. autoapi-nested-parse::

   LocalExecutor

   .. seealso::
       For more information on how the LocalExecutor works, take a look at the guide:
       :ref:`executor:LocalExecutor`



Module Contents
---------------

.. data:: ExecutorWorkType
   

   

.. py:class:: LocalWorkerBase(result_queue: 'Queue[TaskInstanceStateType]')

   Bases: :class:`multiprocessing.Process`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   LocalWorkerBase implementation to run airflow commands. Executes the given
   command and puts the result into a result queue when done, terminating execution.

   :param result_queue: the queue to store result state

   
   .. method:: run(self)



   
   .. method:: execute_work(self, key: TaskInstanceKey, command: CommandType)

      Executes command received and stores result state in queue.

      :param key: the key to identify the task instance
      :param command: the command to execute



   
   .. method:: _execute_work_in_subprocess(self, command: CommandType)



   
   .. method:: _execute_work_in_fork(self, command: CommandType)



   
   .. method:: do_work(self)

      Called in the subprocess and should then execute tasks




.. py:class:: LocalWorker(result_queue: 'Queue[TaskInstanceStateType]', key: TaskInstanceKey, command: CommandType)

   Bases: :class:`airflow.executors.local_executor.LocalWorkerBase`

   Local worker that executes the task.

   :param result_queue: queue where results of the tasks are put.
   :param key: key identifying task instance
   :param command: Command to execute

   
   .. method:: do_work(self)




.. py:class:: QueuedLocalWorker(task_queue: 'Queue[ExecutorWorkType]', result_queue: 'Queue[TaskInstanceStateType]')

   Bases: :class:`airflow.executors.local_executor.LocalWorkerBase`

   LocalWorker implementation that is waiting for tasks from a queue and will
   continue executing commands as they become available in the queue.
   It will terminate execution once the poison token is found.

   :param task_queue: queue from which worker reads tasks
   :param result_queue: queue where worker puts results after finishing tasks

   
   .. method:: do_work(self)




.. py:class:: LocalExecutor(parallelism: int = PARALLELISM)

   Bases: :class:`airflow.executors.base_executor.BaseExecutor`

   LocalExecutor executes tasks locally in parallel.
   It uses the multiprocessing Python library and queues to parallelize the execution
   of tasks.

   :param parallelism: how many parallel processes are run in the executor

   .. py:class:: UnlimitedParallelism(executor: 'LocalExecutor')

      Implements LocalExecutor with unlimited parallelism, starting one process
      per each command to execute.

      :param executor: the executor instance to implement.

      
      .. method:: start(self)

         Starts the executor.



      
      .. method:: execute_async(self, key: TaskInstanceKey, command: CommandType, queue: Optional[str] = None, executor_config: Optional[Any] = None)

         Executes task asynchronously.

         :param key: the key to identify the task instance
         :param command: the command to execute
         :param queue: Name of the queue
         :param executor_config: configuration for the executor



      
      .. method:: sync(self)

         Sync will get called periodically by the heartbeat method.



      
      .. method:: end(self)

         This method is called when the caller is done submitting job and
         wants to wait synchronously for the job submitted previously to be
         all done.




   .. py:class:: LimitedParallelism(executor: 'LocalExecutor')

      Implements LocalExecutor with limited parallelism using a task queue to
      coordinate work distribution.

      :param executor: the executor instance to implement.

      
      .. method:: start(self)

         Starts limited parallelism implementation.



      
      .. method:: execute_async(self, key: TaskInstanceKey, command: CommandType, queue: Optional[str] = None, executor_config: Optional[Any] = None)

         Executes task asynchronously.

         :param key: the key to identify the task instance
         :param command: the command to execute
         :param queue: name of the queue
         :param executor_config: configuration for the executor



      
      .. method:: sync(self)

         Sync will get called periodically by the heartbeat method.



      
      .. method:: end(self)

         Ends the executor. Sends the poison pill to all workers.




   
   .. method:: start(self)

      Starts the executor



   
   .. method:: execute_async(self, key: TaskInstanceKey, command: CommandType, queue: Optional[str] = None, executor_config: Optional[Any] = None)

      Execute asynchronously.



   
   .. method:: sync(self)

      Sync will get called periodically by the heartbeat method.



   
   .. method:: end(self)

      Ends the executor.
      :return:



   
   .. method:: terminate(self)

      Terminate the executor is not doing anything.




