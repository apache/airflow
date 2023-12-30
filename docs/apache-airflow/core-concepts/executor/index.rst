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

Executor
========

Executors are the mechanism by which :doc:`task instances </core-concepts/tasks>` get run. They have a common API and are "pluggable", meaning you can swap executors based on your installation needs.

Airflow can only have one executor configured at a time; this is set by the ``executor`` option in the ``[core]``
section of :doc:`the configuration file </howto/set-config>`.

Built-in executors are referred to by name, for example:

.. code-block:: ini

    [core]
    executor = KubernetesExecutor

.. note::
    For more information on Airflow's configuration, see :doc:`/howto/set-config`.

If you want to check which executor is currently set, you can use the ``airflow config get-value core executor`` command:

.. code-block:: bash

    $ airflow config get-value core executor
    SequentialExecutor


Executor Types
--------------

There are two types of executors - those that run tasks *locally* (inside the ``scheduler`` process), and those that run their tasks *remotely* (usually via a pool of *workers*). Airflow comes configured with the ``SequentialExecutor`` by default, which is a local executor, and the simplest option for execution. However, the ``SequentialExecutor`` is not suitable for production since it does not allow for parallel task running and due to that, some Airflow features (e.g. running sensors) will not work properly. You should instead use the ``LocalExecutor`` for small, single-machine production installations, or one of the remote executors for a multi-machine/cloud installation.


**Local Executors**

.. toctree::
    :maxdepth: 1

    local
    sequential

The DebugExecutor also exists but has been deprecated in favor of using the ``dag.test()`` command for :doc:`debugging <../debug>`.


**Remote Executors**

* :doc:`CeleryExecutor apache-airflow-providers-celery:celery_executor`
* :doc:`CeleryKubernetesExecutor apache-airflow-providers-celery:celery_kubernetes_executor`
* :doc:`DaskExecutor apache-airflow-providers-daskexecutor:dask_executor`
* :doc:`KubernetesExecutor <apache-airflow-providers-cncf-kubernetes:kubernetes_executor>`
* :doc:`KubernetesLocalExecutor <apache-airflow-providers-cncf-kubernetes:local_kubernetes_executor>`


.. note::

    New Airflow users may assume they need to run a separate executor process using one of the Local or Remote Executors. This is not correct. The executor logic runs *inside* the scheduler process, and will run the tasks locally or not depending the executor selected.

Writing Your Own Executor
-------------------------

All Airflow executors implement a common interface so that they are pluggable and any executor has access to all abilities and integrations within Airflow. Primarily, the Airflow scheduler uses this interface to interact with the executor, but other components such as logging, CLI and backfill do as well.
The public interface is the :class:`~airflow.executors.base_executor.BaseExecutor`. You can look through the code for the most detailed and up to date interface, but some important highlights are outlined below.

.. note::
    For more information about Airflow's public interface see :doc:`/public-airflow-interface`.

Some reasons you may want to write a custom executor include:

* An executor does not exist which fits your specific use case, such as a specific tool or service for compute.
* You'd like to use an executor that leverages a compute service from your preferred cloud provider.
* You have a private tool/service for task execution that is only available to you or your organization.


Important BaseExecutor Methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These methods don't require overriding to implement your own executor, but are useful to be aware of:

* ``heartbeat``: The Airflow scheduler Job loop will periodically call heartbeat on the executor. This is one of the main points of interaction between the Airflow scheduler and the executor. This method updates some metrics, triggers newly queued tasks to execute and updates state of running/completed tasks.
* ``queue_command``: The Airflow Executor will call this method of the BaseExecutor to provide tasks to be run by the executor. The BaseExecutor simply adds the TaskInstances to an internal list of queued tasks within the executor.
* ``get_event_buffer``: The Airflow scheduler calls this method to retrieve the current state of the TaskInstances the executor is executing.
* ``has_task``: The scheduler uses this BaseExecutor method to determine if an executor already has a specific task instance queued or running.
* ``send_callback``: Sends any callbacks to the sink configured on the executor.


Mandatory Methods to Implement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following methods must be overridden at minimum to have your executor supported by Airflow:

* ``sync``: Sync will get called periodically during executor heartbeats. Implement this method to update the state of the tasks which the executor knows about. Optionally, attempting to execute queued tasks that have been received from the scheduler.
* ``execute_async``: Executes a command asynchronously. A command in this context is an Airflow CLI command to run an Airflow task. This method is called (after a few layers) during executor heartbeat which is run periodically by the scheduler. In practice, this method often just enqueues tasks into an internal or external queue of tasks to be run (e.g. ``KubernetesExecutor``). But can also execute the tasks directly as well (e.g. ``LocalExecutor``). This will depend on the executor.


Optional Interface Methods to Implement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following methods aren't required to override to have a functional Airflow executor. However, some powerful capabilities and stability can come from implementing them:

* ``start``: The Airflow scheduler (and backfill) job will call this method after it initializes the executor object. Any additional setup required by the executor can be completed here.
* ``end``: The Airflow scheduler (and backfill) job will call this method as it is tearing down. Any synchronous cleanup required to finish running jobs should be done here.
* ``terminate``: More forcefully stop the executor, even killing/stopping in-flight tasks instead of synchronously waiting for completion.
* ``cleanup_stuck_queued_tasks``: If tasks are stuck in the queued state for longer than ``task_queued_timeout`` then they are collected by the scheduler and provided to the executor to have an opportunity to handle them (perform any graceful cleanup/teardown) via this method and return the Task Instances for a warning message displayed to users.
* ``try_adopt_task_instances``: Tasks that have been abandoned (e.g. from a scheduler job that died) are provided to the executor to adopt or otherwise handle them via this method. Any tasks that cannot be adopted (by default the BaseExector assumes all cannot be adopted) should be returned.
* ``get_cli_commands``: Executors may vend CLI commands to users by implementing this method, see the `CLI`_ section below for more details.
* ``get_task_log``: Executors may vend log messages to Airflow task logs by implementing this method, see the `Logging`_ section below for more details.

Compatibility Attributes
^^^^^^^^^^^^^^^^^^^^^^^^

The ``BaseExecutor`` class interface contains a set of attributes that Airflow core code uses to check the features that your executor is compatible with. When writing your own Airflow executor be sure to set these correctly for your use case. Each attribute is simply a boolean to enable/disable a feature or indicate that a feature is supported/unsupported by the executor:

* ``supports_pickling``: Whether or not the executor supports reading pickled DAGs from the Database before execution (rather than reading the DAG definition from the file system).
* ``supports_sentry``: Whether or not the executor supports `Sentry <https://sentry.io>`_.

* ``is_local``: Whether or not the executor is remote or local. See the `Executor Types`_ section above.
* ``is_single_threaded``: Whether or not the executor is single threaded. This is particularly relevant to what database backends are supported. Single threaded executors can run with any backend, including SQLite.
* ``is_production``: Whether or not the executor should be used for production purposes. A UI message is displayed to users when they are using a non-production ready executor.

* ``change_sensor_mode_to_reschedule``: Running Airflow sensors in poke mode can block the thread of executors and in some cases Airflow.
* ``serve_logs``: Whether or not the executor supports serving logs, see :doc:`/administration-and-deployment/logging-monitoring/logging-tasks`.

CLI
^^^

Executors may vend CLI commands which will be included in the ``airflow`` command line tool by implementing the ``get_cli_commands`` method. Executors such as ``CeleryExecutor`` and ``KubernetesExecutor`` for example, make use of this mechanism. The commands can be used to setup required workers, initialize environment or set other configuration. Commands are only vended for the currently configured executor. A pseudo-code example of implementing CLI command vending from an executor can be seen below:

.. code-block:: python

    @staticmethod
    def get_cli_commands() -> list[GroupCommand]:
        sub_commands = [
            ActionCommand(
                name="command_name",
                help="Description of what this specific command does",
                func=lazy_load_command("path.to.python.function.for.command"),
                args=(),
            ),
        ]

        return [
            GroupCommand(
                name="my_cool_executor",
                help="Description of what this group of commands do",
                subcommands=sub_commands,
            ),
        ]

.. note::
    Currently there are no strict rules in place for the Airflow command namespace. It is up to developers to use names for their CLI commands that are sufficiently unique so as to not cause conflicts with other Airflow executors or components.

.. note::
    When creating a new executor, or updating any existing executors, be sure to not import or execute any expensive operations/code at the module level. Executor classes are imported in several places and if they are slow to import this will negatively impact the performance of your Airflow environment, especially for CLI commands.

Logging
^^^^^^^

Executors may vend log messages which will be included in the Airflow task logs by implementing the ``get_task_logs`` method. This can be helpful if the execution environment has extra context in the case of task failures, which may be due to the execution environment itself rather than the Airflow task code. It can also be helpful to include setup/teardown logging from the execution environment.
The ``KubernetesExecutor`` leverages this this capability to include logs from the pod which ran a specific Airflow task and display them in the logs for that Airflow task. A pseudo-code example of implementing task log vending from an executor can be seen below:

.. code-block:: python

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        messages = []
        log = []
        try:
            res = helper_function_to_fetch_logs_from_execution_env(ti, try_number)
            for line in res:
                log.append(remove_escape_codes(line.decode()))
            if log:
                messages.append("Found logs from execution environment!")
        except Exception as e:  # No exception should cause task logs to fail
            messages.append(f"Failed to find logs from execution environment: {e}")
        return messages, ["\n".join(log)]

Next Steps
^^^^^^^^^^

Once you have created a new executor class implementing the ``BaseExecutor`` interface, you can configure Airflow to use it by setting the ``core.executor`` configuration value to the module path of your executor:

.. code-block:: ini

    [core]
    executor = my_company.executors.MyCustomExecutor

.. note::
    For more information on Airflow's configuration, see :doc:`/howto/set-config` and for more information on managing Python modules in Airflow see :doc:`/administration-and-deployment/modules_management`.
