:mod:`airflow.executors.dask_executor`
======================================

.. py:module:: airflow.executors.dask_executor

.. autoapi-nested-parse::

   DaskExecutor

   .. seealso::
       For more information on how the DaskExecutor works, take a look at the guide:
       :ref:`executor:DaskExecutor`



Module Contents
---------------

.. py:class:: DaskExecutor(cluster_address=None)

   Bases: :class:`airflow.executors.base_executor.BaseExecutor`

   DaskExecutor submits tasks to a Dask Distributed cluster.

   
   .. method:: start(self)



   
   .. method:: execute_async(self, key: TaskInstanceKey, command: CommandType, queue: Optional[str] = None, executor_config: Optional[Any] = None)



   
   .. method:: _process_future(self, future: Future)



   
   .. method:: sync(self)



   
   .. method:: end(self)



   
   .. method:: terminate(self)




