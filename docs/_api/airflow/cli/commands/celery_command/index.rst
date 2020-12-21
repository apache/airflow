:mod:`airflow.cli.commands.celery_command`
==========================================

.. py:module:: airflow.cli.commands.celery_command

.. autoapi-nested-parse::

   Celery command



Module Contents
---------------

.. data:: WORKER_PROCESS_NAME
   :annotation: = worker

   

.. function:: flower(args)
   Starts Flower, Celery monitoring tool


.. function:: _serve_logs(skip_serve_logs: bool = False) -> Optional[Process]
   Starts serve_logs sub-process


.. function:: worker(args)
   Starts Airflow Celery worker


.. function:: stop_worker(args)
   Sends SIGTERM to Celery worker


