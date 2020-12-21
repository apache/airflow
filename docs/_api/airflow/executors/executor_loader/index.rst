:mod:`airflow.executors.executor_loader`
========================================

.. py:module:: airflow.executors.executor_loader

.. autoapi-nested-parse::

   All executors.



Module Contents
---------------

.. data:: log
   

   

.. py:class:: ExecutorLoader

   Keeps constants for all the currently available executors.

   .. attribute:: _default_executor
      :annotation: :Optional[BaseExecutor]

      

   .. attribute:: executors
      

      

   
   .. classmethod:: get_default_executor(cls)

      Creates a new instance of the configured executor if none exists and returns it



   
   .. classmethod:: load_executor(cls, executor_name: str)

      Loads the executor.

      This supports the following formats:
      * by executor name for core executor
      * by ``{plugin_name}.{class_name}`` for executor from plugins
      * by import path.

      :return: an instance of executor class via executor_name



   
   .. classmethod:: __load_celery_kubernetes_executor(cls)

      :return: an instance of CeleryKubernetesExecutor




.. data:: UNPICKLEABLE_EXECUTORS
   

   

