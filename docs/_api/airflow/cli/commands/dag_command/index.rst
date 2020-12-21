:mod:`airflow.cli.commands.dag_command`
=======================================

.. py:module:: airflow.cli.commands.dag_command

.. autoapi-nested-parse::

   Dag sub-commands



Module Contents
---------------

.. function:: _tabulate_dag_runs(dag_runs: List[DagRun], tablefmt: str = 'fancy_grid') -> str

.. function:: _tabulate_dags(dags: List[DAG], tablefmt: str = 'fancy_grid') -> str

.. function:: dag_backfill(args, dag=None)
   Creates backfill job or dry run for a DAG


.. function:: dag_trigger(args)
   Creates a dag run for the specified dag


.. function:: dag_delete(args)
   Deletes all DB records related to the specified dag


.. function:: dag_pause(args)
   Pauses a DAG


.. function:: dag_unpause(args)
   Unpauses a DAG


.. function:: set_is_paused(is_paused, args)
   Sets is_paused for DAG by a given dag_id


.. function:: dag_show(args)
   Displays DAG or saves it's graphic representation to the file


.. function:: _display_dot_via_imgcat(dot: Dot)

.. function:: _save_dot_to_file(dot: Dot, filename: str)

.. function:: dag_state(args)
   Returns the state (and conf if exists) of a DagRun at the command line.
   >>> airflow dags state tutorial 2015-01-01T00:00:00.000000
   running
   >>> airflow dags state a_dag_with_conf_passed 2015-01-01T00:00:00.000000
   failed, {"name": "bob", "age": "42"}


.. function:: dag_next_execution(args)
   Returns the next execution datetime of a DAG at the command line.
   >>> airflow dags next-execution tutorial
   2018-08-31 10:38:00


.. function:: dag_list_dags(args)
   Displays dags with or without stats at the command line


.. function:: dag_report(args)
   Displays dagbag stats at the command line


.. function:: dag_list_jobs(args, dag=None)
   Lists latest n jobs


.. function:: dag_list_dag_runs(args, dag=None)
   Lists dag runs for a given DAG


.. function:: dag_test(args, session=None)
   Execute one single DagRun for a given DAG and execution date, using the DebugExecutor.


