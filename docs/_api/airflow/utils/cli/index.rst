:mod:`airflow.utils.cli`
========================

.. py:module:: airflow.utils.cli

.. autoapi-nested-parse::

   Utilities module for cli



Module Contents
---------------

.. data:: T
   

   

.. function:: action_logging(f: T) -> T
   Decorates function to execute function at the same time submitting action_logging
   but in CLI context. It will call action logger callbacks twice,
   one for pre-execution and the other one for post-execution.

   Action logger will be called with below keyword parameters:
       sub_command : name of sub-command
       start_datetime : start datetime instance by utc
       end_datetime : end datetime instance by utc
       full_command : full command line arguments
       user : current user
       log : airflow.models.log.Log ORM instance
       dag_id : dag id (optional)
       task_id : task_id (optional)
       execution_date : execution date (optional)
       error : exception instance if there's an exception

   :param f: function instance
   :return: wrapped function


.. function:: _build_metrics(func_name, namespace)
   Builds metrics dict from function args
   It assumes that function arguments is from airflow.bin.cli module's function
   and has Namespace instance where it optionally contains "dag_id", "task_id",
   and "execution_date".

   :param func_name: name of function
   :param namespace: Namespace instance from argparse
   :return: dict with metrics


.. function:: process_subdir(subdir: Optional[str])
   Expands path to absolute by replacing 'DAGS_FOLDER', '~', '.', etc.


.. function:: get_dag_by_file_location(dag_id: str)
   Returns DAG of a given dag_id by looking up file location


.. function:: get_dag(subdir: Optional[str], dag_id: str) -> DAG
   Returns DAG of a given dag_id


.. function:: get_dags(subdir: Optional[str], dag_id: str, use_regex: bool = False)
   Returns DAG(s) matching a given regex or dag_id


.. function:: get_dag_by_pickle(pickle_id, session=None)
   Fetch DAG from the database using pickling


.. function:: setup_locations(process, pid=None, stdout=None, stderr=None, log=None)
   Creates logging paths


.. function:: setup_logging(filename)
   Creates log file handler for daemon process


.. function:: sigint_handler(sig, frame)
   Returns without error on SIGINT or SIGTERM signals in interactive command mode
   e.g. CTRL+C or kill <PID>


.. function:: sigquit_handler(sig, frame)
   Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
   e.g. kill -s QUIT <PID> or CTRL+    


.. py:class:: ColorMode

   Coloring modes. If `auto` is then automatically detected.

   .. attribute:: ON
      :annotation: = on

      

   .. attribute:: OFF
      :annotation: = off

      

   .. attribute:: AUTO
      :annotation: = auto

      


.. function:: should_use_colors(args) -> bool
   Processes arguments and decides whether to enable color in output


