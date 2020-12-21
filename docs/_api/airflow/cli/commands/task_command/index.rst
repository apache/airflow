:mod:`airflow.cli.commands.task_command`
========================================

.. py:module:: airflow.cli.commands.task_command

.. autoapi-nested-parse::

   Task sub-commands



Module Contents
---------------

.. function:: _run_task_by_selected_method(args, dag, ti)
   Runs the task in one of 3 modes

   - using LocalTaskJob
   - as raw task
   - by executor


.. function:: _run_task_by_executor(args, dag, ti)
   Sends the task to the executor for execution. This can result in the task being started by another host
   if the executor implementation does


.. function:: _run_task_by_local_task_job(args, ti)
   Run LocalTaskJob, which monitors the raw task execution process


.. data:: RAW_TASK_UNSUPPORTED_OPTION
   :annotation: = ['ignore_all_dependencies', 'ignore_depends_on_past', 'ignore_dependencies', 'force']

   

.. function:: _run_raw_task(args, ti)
   Runs the main task handling code


.. function:: _capture_task_logs(ti)
   Manage logging context for a task run

   - Replace the root logger configuration with the airflow.task configuration
     so we can capture logs from any custom loggers used in the task.

   - Redirect stdout and stderr to the task instance log, as INFO and WARNING
     level messages, respectively.


.. function:: task_run(args, dag=None)
   Runs a single task instance


.. function:: task_failed_deps(args)
   Returns the unmet dependencies for a task instance from the perspective of the
   scheduler (i.e. why a task instance doesn't get scheduled and then queued by the
   scheduler, and then run by an executor).
   >>> airflow tasks failed-deps tutorial sleep 2015-01-01
   Task instance dependencies not met:
   Dagrun Running: Task instance's dagrun did not exist: Unknown reason
   Trigger Rule: Task's trigger rule 'all_success' requires all upstream tasks
   to have succeeded, but found 1 non-success(es).


.. function:: task_state(args)
   Returns the state of a TaskInstance at the command line.
   >>> airflow tasks state tutorial sleep 2015-01-01
   success


.. function:: task_list(args, dag=None)
   Lists the tasks within a DAG at the command line


.. data:: SUPPORTED_DEBUGGER_MODULES
   :annotation: :List[str] = ['pudb', 'web_pdb', 'ipdb', 'pdb']

   

.. function:: _guess_debugger()
   Trying to guess the debugger used by the user. When it doesn't find any user-installed debugger,
   returns ``pdb``.

   List of supported debuggers:

   * `pudb <https://github.com/inducer/pudb>`__
   * `web_pdb <https://github.com/romanvm/python-web-pdb>`__
   * `ipdb <https://github.com/gotcha/ipdb>`__
   * `pdb <https://docs.python.org/3/library/pdb.html>`__


.. function:: task_states_for_dag_run(args)
   Get the status of all task instances in a DagRun


.. function:: task_test(args, dag=None)
   Tests task for a given dag_id


.. function:: task_render(args)
   Renders and displays templated fields for a given task


.. function:: task_clear(args)
   Clears all task instances or only those matched by regex for a DAG(s)


