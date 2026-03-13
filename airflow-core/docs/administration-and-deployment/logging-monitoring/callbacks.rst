 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



Callbacks
=========

A valuable component of logging and monitoring is the use of task callbacks to act upon changes in state of a given Dag or task, or across all tasks in a given Dag.
For example, you may wish to alert when certain tasks have failed, or invoke a callback when your Dag succeeds.

There are three different places where callbacks can be defined.

- Callbacks set in the Dag definition will be applied at the Dag level.
- Using ``default_args``, callbacks can be set for each task in a Dag.
- Individual callbacks can be set for a task by setting that callback within the task definition itself.

.. note::

    Callback functions are only invoked when the Dag or task state changes due to execution by a worker.
    As such, Dag and task changes set by the command line interface (:doc:`CLI <../../howto/usage-cli>`) or user interface (:doc:`UI <../../ui>`) do not
    execute callback functions.

.. warning::

    Callback functions are executed after tasks are completed.
    Errors in callback functions will show up in dag processor logs rather than task logs.
    By default, dag processor logs do not show up in the UI and instead can be found in
    ``$AIRFLOW_HOME/logs/dag_processor/latest/dags-folder/<the_path_for_your_dag>/DAG_FILE.py.log``

.. note::
    As of Airflow 2.6.0, callbacks now supports a list of callback functions, allowing users to specify multiple functions
    to be executed in the desired event. Simply pass a list of callback functions to the callback args when defining your Dag/task
    callbacks: e.g ``on_failure_callback=[callback_func_1, callback_func_2]``


Callback Types
--------------

There are six types of events that can trigger a callback:

=========================================== ======================================================================= =================
Name                                        Description                                                             Availability
=========================================== ======================================================================= =================
``on_success_callback``                     Invoked when the :ref:`Dag succeeds <dag-run:dag-run-status>`           Dag or Task
                                            or :ref:`task succeeds <concepts:task-instances>`.
``on_failure_callback``                     Invoked when the :ref:`Dag fails <dag-run:dag-run-status>`              Dag or Task
                                            or task :ref:`fails <concepts:task-instances>`.
``on_retry_callback``                       Invoked when the task is :ref:`up for retry <concepts:task-instances>`. Task
``on_execute_callback``                     Invoked right before the task begins executing.                         Task
``on_skipped_callback``                     Invoked when the task is :ref:`running <concepts:task-instances>`       Task
                                            and AirflowSkipException raised. Explicitly it is NOT called if a task
                                            is not started to be executed because of a preceding branching
                                            decision in the Dag or a trigger rule which causes execution
                                            to skip so that the task execution is never scheduled.
=========================================== ======================================================================= =================


Context Mapping
---------------

A context mapping that contains runtime information about a task instance is passed to every callback.
Full list of variables available in ``context`` are in :doc:`docs <../../templates-ref>` and `code <https://github.com/apache/airflow/blob/main/task-sdk/src/airflow/sdk/definitions/context.py>`_.


Dag Callbacks
^^^^^^^^^^^^^

As the context mapping describes execution of a task instance, contexts passed to Dag callbacks will also contain task instance variables,
and the task selected depends on the state of a Dag:

#. On regular failure, the latest failed task is selected.
#. On Dag run timeout, the latest started but not finished task is passed.
#. If tasks are deadlocked, a task that should have run next but couldn't is passed.
#. On success, the latest succeeded task is passed.

It's not recommended to rely on task instance variables in Dag callbacks except for human analysis, as they reflect only partial information about the Dag's state.
For example, a timeout may be caused by a number of stalling tasks, but only one will eventually be selected for context.

.. note::
    Before Airflow 3.2.0, the rules above did not apply and the task instance passed to Dag callback was not related to Dag state, rather being selected as the latest task in the Dag
    lexicographically.


Examples
--------

Using Custom Callback Methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the following example, failures in ``task1`` call the ``task_failure_alert`` function, and success at Dag level calls the ``dag_success_alert`` function.
Before each task begins to execute, the ``task_execute_callback`` function will be called:

.. code-block:: python

    from airflow.sdk import DAG
    from airflow.providers.standard.operators.empty import EmptyOperator


    def task_execute_callback(context):
        print(f"Task has begun execution, task_instance_key_str: {context['task_instance_key_str']}")


    def task_failure_alert(context):
        print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


    def dag_success_alert(context):
        print(f"Dag has succeeded, run_id: {context['run_id']}")


    with DAG(
        dag_id="example_callback",
        on_success_callback=dag_success_alert,
        default_args={"on_execute_callback": task_execute_callback},
    ):
        task1 = EmptyOperator(task_id="task1", on_failure_callback=[task_failure_alert])
        task2 = EmptyOperator(task_id="task2")
        task3 = EmptyOperator(task_id="task3")
        task1 >> task2 >> task3


Using Notifiers
^^^^^^^^^^^^^^^

You can use Notifiers in your Dag definition by passing it as an argument to the ``on_*_callbacks``.
For example, you can use it with ``on_success_callback`` or ``on_failure_callback`` to send notifications based
on the status of a task or a Dag run.

Here's an example of using a custom notifier:

.. code-block:: python

    from airflow.sdk import DAG
    from airflow.providers.standard.operators.bash import BashOperator

    from myprovider.notifier import MyNotifier

    with DAG(
        dag_id="example_notifier",
        on_success_callback=MyNotifier(message="Success!"),
        on_failure_callback=MyNotifier(message="Failure!"),
    ):
        task = BashOperator(
            task_id="example_task",
            bash_command="exit 1",
            on_success_callback=MyNotifier(message="Task Succeeded!"),
        )

For a list of community-managed Notifiers, see :doc:`apache-airflow-providers:core-extensions/notifications`.
For more information on writing a custom Notifier, see the :doc:`Notifiers <../../howto/notifications>` how-to page.

Deadline Alert Callbacks
^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the Dag/task lifecycle callbacks above, Airflow supports **Deadline Alert** callbacks which
trigger when a Dag run exceeds a configured time threshold. Deadline Alert callbacks use
:class:`~airflow.sdk.AsyncCallback` (runs in the Triggerer) or :class:`~airflow.sdk.SyncCallback`
(runs in the executor) and are configured on the Dag via the ``deadline`` parameter.

For full details, see :doc:`/howto/deadline-alerts`.
