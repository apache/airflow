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

Executors are set by the ``executor`` option in the ``[core]`` section of :doc:`the configuration file </howto/set-config>`.

.. _executor-basic-configuration:

Built-in executors are referred to by name, for example:

.. code-block:: ini

    [core]
    executor = KubernetesExecutor

Custom or third-party executors can be configured by providing the module path of the executor python class, for example:

.. code-block:: ini

    [core]
    executor = my.custom.executor.module.ExecutorClass

.. note::
    For more information on Airflow's configuration, see :doc:`/howto/set-config`.

If you want to check which executor is currently set, you can use the ``airflow config get-value core executor`` command:

.. code-block:: bash

    $ airflow config get-value core executor
    LocalExecutor

Executor Types
--------------

There is only one type of executor that runs tasks *locally* (inside the ``scheduler`` process) in the repo tree, but custom ones
can be written to achieve similar results, and there are those that run their tasks *remotely* (usually via a pool of *workers*). Airflow comes configured with the ``LocalExecutor`` by default, which is a local executor, and the simplest option for execution.
However, as the ``LocalExecutor`` runs processes in the scheduler process that can have an impact on the performance of the scheduler. You can use the ``LocalExecutor``
for small, single-machine production installations, or one of the remote executors for a multi-machine/cloud installation.


.. _executor-types-comparison:

Local Executors
^^^^^^^^^^^^^^^

Airflow tasks are run locally within the scheduler process.

**Pros**: Very easy to use, fast, very low latency, and few requirements for setup.

**Cons**: Limited in capabilities and shares resources with the Airflow scheduler.

**Examples**:

.. toctree::
    :maxdepth: 1

    local

Remote Executors
^^^^^^^^^^^^^^^^

Remote executors can further be divided into two categories:

*Queued/Batch Executors*

Airflow tasks are sent to a central queue where remote workers pull tasks to execute. Often workers are persistent and run multiple tasks at once.

**Pros**: More robust since you're decoupling workers from the scheduler process. Workers can be large hosts that can churn through many tasks (often in parallel) which is cost effective. Latency can be relatively low since workers can be provisioned to be running at all times to take tasks immediately from the queue.

**Cons**: Shared workers have the noisy neighbor problem with tasks competing for resources on the shared hosts or competing for how the environment/system is configured. They can also be expensive if your workload is not constant, you may have workers idle, overly scaled in resources, or you have to manage scaling them up and down.

**Examples**:

* :doc:`CeleryExecutor <apache-airflow-providers-celery:celery_executor>`
* :doc:`BatchExecutor <apache-airflow-providers-amazon:executors/batch-executor>`
* :doc:`EdgeExecutor <apache-airflow-providers-edge3:edge_executor>` (Experimental Pre-Release)


*Containerized Executors*

Airflow tasks are executed ad hoc inside containers/pods. Each task is isolated in its own containerized environment that is deployed when the Airflow task is queued.

**Pros**: Each Airflow task is isolated to one container so no noisy neighbor problem. The execution environment can be customized for specific tasks (system libs, binaries, dependencies, amount of resources, etc). Cost effective as the workers are only alive for the duration of the task.

**Cons**: There is latency on startup since the container or pod needs to deploy before the task can begin. Can be expensive if you're running many short/small tasks. No workers to manage however you must manage something like a Kubernetes cluster.

**Examples**:

* :doc:`KubernetesExecutor <apache-airflow-providers-cncf-kubernetes:kubernetes_executor>`
* :doc:`EcsExecutor <apache-airflow-providers-amazon:executors/ecs-executor>`

.. note::

    New Airflow users may assume they need to run a separate executor process using one of the Local or Remote Executors. This is not correct. The executor logic runs *inside* the scheduler process, and will run the tasks locally or not depending on the executor selected.

.. _using-multiple-executors-concurrently:

Using Multiple Executors Concurrently
-------------------------------------

Starting with version 2.10.0, Airflow can now operate with a multi-executor configuration. Each executor has its own set of pros and cons, often they are trade-offs between latency, isolation and compute efficiency among other properties (see :ref:`here <executor-types-comparison>` for comparisons of executors). Running multiple executors allows you to make better use of the strengths of all the available executors and avoid their weaknesses. In other words, you can use a specific executor for a specific set of tasks where its particular merits and benefits make the most sense for that use case.

Configuration
^^^^^^^^^^^^^

Configuring multiple executors uses the same configuration option (as described :ref:`here <executor-basic-configuration>`) as single executor use cases, leveraging a comma separated list notation to specify multiple executors.

.. note::
    The first executor in the list (either on its own or along with other executors) will behave the same as it did in pre-2.10.0 releases. In other words, this will be the default executor for the environment. Any Airflow Task or Dag that does not specify a specific executor will use this environment level executor. All other executors in the list will be initialized and ready to run tasks if specified on an Airflow Task or Dag. If you do not specify an executor in this configuration list, it cannot be used to run tasks.

Some examples of valid multiple executor configuration:

.. code-block:: ini

    [core]
    executor = LocalExecutor

.. code-block:: ini

    [core]
    executor = LocalExecutor,CeleryExecutor

.. code-block:: ini

    [core]
    executor = KubernetesExecutor,my.custom.module.ExecutorClass


.. note::
    Using two instances of the _same_ executor class is not currently supported.

To make it easier to specify executors on tasks and Dags, executor configuration now supports aliases. You may then use this alias to refer to the executor in your Dags (see below).

.. code-block:: ini

    [core]
    executor = LocalExecutor,ShortName:my.custom.module.ExecutorClass

.. note::
    If a Dag specifies a task to use an executor that is not configured, the Dag will fail to parse and a warning dialog will be shown in the Airflow UI. Please ensure that all executors you wish to use are specified in Airflow configuration on *any* host/container that is running an Airflow component (scheduler, workers, etc).

Writing Dags and Tasks
^^^^^^^^^^^^^^^^^^^^^^

To specify an executor for a task, make use of the executor parameter on Airflow Operators:

.. code-block:: python

    BashOperator(
        task_id="hello_world",
        executor="LocalExecutor",
        bash_command="echo 'hello world!'",
    )

.. code-block:: python

    @task(executor="LocalExecutor")
    def hello_world():
        print("hello world!")

To specify an executor for an entire Dag, make use of the existing Airflow mechanism of default arguments. All tasks in the Dag will then use the specified executor (unless explicitly overridden by a specific task):

.. code-block:: python

    def hello_world():
        print("hello world!")


    def hello_world_again():
        print("hello world again!")


    with DAG(
        dag_id="hello_worlds",
        default_args={"executor": "LocalExecutor"},  # Applies to all tasks in the Dag
    ) as dag:
        # All tasks will use the executor from default args automatically
        hw = hello_world()
        hw_again = hello_world_again()

.. note::
    Tasks store the executor they were configured to run on in the Airflow database. Changes are reflected after each parsing of a Dag.

Monitoring
^^^^^^^^^^

When using a single executor, Airflow metrics will behave as they were <2.9. But if multiple executors are configured then the executor metrics (``executor.open_slots``, ``executor.queued_slots``, and ``executor.running_tasks``) will be published for each executor configured, with the executor name appended to the metric name (e.g. ``executor.open_slots.<executor class name>``).

Logging works the same as the single executor use case.

Statically-coded Hybrid Executors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There were two "statically coded" executors, but they are no longer supported starting from Airflow 3.0.0.

These executors are hybrids of two different executors: the :doc:`LocalKubernetesExecutor <apache-airflow-providers-cncf-kubernetes:local_kubernetes_executor>` and the :doc:`CeleryKubernetesExecutor <apache-airflow-providers-celery:celery_kubernetes_executor>`. Their implementation is not native or intrinsic to core Airflow. These hybrid executors instead make use of the ``queue`` field on Task Instances to indicate and persist which sub-executor to run on. This is a misuse of the ``queue`` field and makes it impossible to use it for its intended purpose when using these hybrid executors.

Executors such as these also require hand crafting new "concrete" classes to create each permutation of possible combinations of executors. This is untenable as more executors are created and leads to more maintenance overhead. Bespoke coding effort should not be required to use any combination of executors.

Therefore using these types of executors is no longer supported starting from Airflow 3.0.0. It's recommended to use the :ref:`Using Multiple Executors Concurrently <using-multiple-executors-concurrently>` feature instead.


Writing Your Own Executor
-------------------------

All Airflow executors implement a common interface so that they are pluggable and any executor has access to all abilities and integrations within Airflow. Primarily, the Airflow scheduler uses this interface to interact with the executor, but other components such as logging and CLI do as well.
The public interface is the :class:`~airflow.executors.base_executor.BaseExecutor`. You can look through the code for the most detailed and up to date interface, but some important highlights are outlined below.

.. note::
    For more information about Airflow's public interface see :doc:`/public-airflow-interface`.

Some reasons you may want to write a custom executor include:

* An executor does not exist which fits your specific use case, such as a specific tool or service for compute.
* You'd like to use an executor that leverages a compute service from your preferred cloud provider.
* You have a private tool/service for task execution that is only available to you or your organization.

Workloads
^^^^^^^^^

A workload in context of an Executor is the fundamental unit of execution for an executor. It represents a discrete
operation or job that the executor runs on a worker. For example, it can run user code encapsulated in an Airflow task
on a worker.

Example:

.. code-block:: python

    ExecuteTask(
        token="mock",
        ti=TaskInstance(
            id=UUID("4d828a62-a417-4936-a7a6-2b3fabacecab"),
            task_id="mock",
            dag_id="mock",
            run_id="mock",
            try_number=1,
            map_index=-1,
            pool_slots=1,
            queue="default",
            priority_weight=1,
            executor_config=None,
            parent_context_carrier=None,
            context_carrier=None,
            queued_dttm=None,
        ),
        dag_rel_path=PurePosixPath("mock.py"),
        bundle_info=BundleInfo(name="n/a", version="no matter"),
        log_path="mock.log",
        type="ExecuteTask",
    )


Important BaseExecutor Methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These methods don't require overriding to implement your own executor, but are useful to be aware of:

* ``heartbeat``: The Airflow scheduler Job loop will periodically call heartbeat on the executor. This is one of the main points of interaction between the Airflow scheduler and the executor. This method updates some metrics, triggers newly queued tasks to execute and updates state of running/completed tasks.
* ``queue_workload``: The Airflow Executor will call this method of the BaseExecutor to provide tasks to be run by the executor. The BaseExecutor simply adds the *workloads* (check section above to understand) to an internal list of queued workloads to run within the executor. All executors present in the repository use this method.
* ``get_event_buffer``: The Airflow scheduler calls this method to retrieve the current state of the TaskInstances the executor is executing.
* ``has_task``: The scheduler uses this BaseExecutor method to determine if an executor already has a specific task instance queued or running.
* ``send_callback``: Sends any callbacks to the sink configured on the executor.


Mandatory Methods to Implement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following methods must be overridden at minimum to have your executor supported by Airflow:

* ``sync``: Sync will get called periodically during executor heartbeats. Implement this method to update the state of the tasks which the executor knows about. Optionally, attempting to execute queued tasks that have been received from the scheduler.
* ``execute_async``: Executes a *workload* asynchronously. This method is called (after a few layers) during executor heartbeat which is run periodically by the scheduler. In practice, this method often just enqueues tasks into an internal or external queue of tasks to be run (e.g. ``KubernetesExecutor``). But can also execute the tasks directly as well (e.g. ``LocalExecutor``). This will depend on the executor.


Optional Interface Methods to Implement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following methods aren't required to override to have a functional Airflow executor. However, some powerful capabilities and stability can come from implementing them:

* ``start``: The Airflow scheduler job will call this method after it initializes the executor object. Any additional setup required by the executor can be completed here.
* ``end``: The Airflow scheduler job will call this method as it is tearing down. Any synchronous cleanup required to finish running jobs should be done here.
* ``terminate``: More forcefully stop the executor, even killing/stopping in-flight tasks instead of synchronously waiting for completion.
* ``try_adopt_task_instances``: Tasks that have been abandoned (e.g. from a scheduler job that died) are provided to the executor to adopt or otherwise handle them via this method. Any tasks that cannot be adopted (by default the BaseExecutor assumes all cannot be adopted) should be returned.
* ``get_cli_commands``: Executors may vend CLI commands to users by implementing this method, see the `CLI`_ section below for more details.
* ``get_task_log``: Executors may vend log messages to Airflow task logs by implementing this method, see the `Logging`_ section below for more details.

Compatibility Attributes
^^^^^^^^^^^^^^^^^^^^^^^^

The ``BaseExecutor`` class interface contains a set of attributes that Airflow core code uses to check the features that your executor is compatible with. When writing your own Airflow executor be sure to set these correctly for your use case. Each attribute is simply a boolean to enable/disable a feature or indicate that a feature is supported/unsupported by the executor:

* ``supports_pickling``: Whether or not the executor supports reading pickled Dags from the Database before execution (rather than reading the Dag definition from the file system).
* ``sentry_integration``: If the executor supports `Sentry <https://sentry.io>`_, this should be a import path to a callable that creates the integration. For example, ``CeleryExecutor`` sets this to ``"sentry_sdk.integrations.celery.CeleryIntegration"``.
* ``is_local``: Whether or not the executor is remote or local. See the `Executor Types`_ section above.
* ``is_single_threaded``: Whether or not the executor is single threaded. This is particularly relevant to what database backends are supported. Single threaded executors can run with any backend, including SQLite.
* ``is_production``: Whether or not the executor should be used for production purposes. A UI message is displayed to users when they are using a non-production ready executor.
* ``serve_logs``: Whether or not the executor supports serving logs, see :doc:`/administration-and-deployment/logging-monitoring/logging-tasks`.

CLI
^^^

.. important::
  Starting in Airflow ``3.2.0``, provider-level CLI commands are available to manage core extensions such as auth managers and executors. Implementing provider-level CLI commands can reduce CLI startup time by avoiding heavy imports when they are not required.
  See :doc:`provider-level CLI <apache-airflow-providers:core-extensions/cli-commands>` for implementation guidance.

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
