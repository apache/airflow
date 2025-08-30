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

Tasks
=====

A Task is the basic unit of execution in Airflow. Tasks are arranged into :doc:`dags`, and then have upstream and downstream dependencies set between them in order to express the order they should run in.

There are three basic kinds of Task:

* :doc:`operators`, predefined task templates that you can string together quickly to build most parts of your Dags.

* :doc:`sensors`, a special subclass of Operators which are entirely about waiting for an external event to happen.

* A :doc:`taskflow`-decorated ``@task``, which is a custom Python function packaged up as a Task.

Internally, these are all actually subclasses of Airflow's ``BaseOperator``, and the concepts of Task and Operator are somewhat interchangeable, but it's useful to think of them as separate concepts - essentially, Operators and Sensors are *templates*, and when you call one in a Dag file, you're making a Task.


Relationships
-------------

The key part of using Tasks is defining how they relate to each other - their *dependencies*, or as we say in Airflow, their *upstream* and *downstream* tasks. You declare your Tasks first, and then you declare their dependencies second.

.. note::

    We call the *upstream* task the one that is directly preceding the other task. We used to call it a parent task before.
    Be aware that this concept does not describe the tasks that are higher in the tasks hierarchy (i.e. they are not a direct parents of the task).
    Same definition applies to *downstream* task, which needs to be a direct child of the other task.

There are two ways of declaring dependencies - using the ``>>`` and ``<<`` (bitshift) operators::

    first_task >> second_task >> [third_task, fourth_task]

Or the more explicit ``set_upstream`` and ``set_downstream`` methods::

    first_task.set_downstream(second_task)
    third_task.set_upstream(second_task)

These both do exactly the same thing, but in general we recommend you use the bitshift operators, as they are easier to read in most cases.

By default, a Task will run when all of its upstream (parent) tasks have succeeded, but there are many ways of modifying this behaviour to add branching, to only wait for some upstream tasks, or to change behaviour based on where the current run is in history. For more, see :ref:`concepts-control-flow`.

Tasks don't pass information to each other by default, and run entirely independently. If you want to pass information from one Task to another, you should use :doc:`xcoms`.


.. _concepts:task-instances:

Task Instances
--------------

Much in the same way that a Dag is instantiated into a :ref:`Dag Run <concepts-dag-run>` each time it runs, the tasks under a Dag are instantiated into *Task Instances*.

An instance of a Task is a specific run of that task for a given Dag (and thus for a given data interval). They are also the representation of a Task that has *state*, representing what stage of the lifecycle it is in.

.. _concepts:task-states:

The possible states for a Task Instance are:

* ``none``: The Task has not yet been queued for execution (its dependencies are not yet met)
* ``scheduled``: The scheduler has determined the Task's dependencies are met and it should run
* ``queued``: The task has been assigned to an Executor and is awaiting a worker
* ``running``: The task is running on a worker (or on a local/synchronous executor)
* ``success``: The task finished running without errors
* ``restarting``: The task was externally requested to restart when it was running
* ``failed``: The task had an error during execution and failed to run
* ``skipped``: The task was skipped due to branching, LatestOnly, or similar.
* ``upstream_failed``: An upstream task failed and the :ref:`Trigger Rule <concepts:trigger-rules>` says we needed it
* ``up_for_retry``: The task failed, but has retry attempts left and will be rescheduled.
* ``up_for_reschedule``: The task is a :doc:`Sensor <sensors>` that is in ``reschedule`` mode
* ``deferred``: The task has been :doc:`deferred to a trigger <../authoring-and-scheduling/deferring>`
* ``removed``: The task has vanished from the Dag since the run started

.. image:: /img/diagram_task_lifecycle.png

Ideally, a task should flow from ``none``, to ``scheduled``, to ``queued``, to ``running``, and finally to ``success``.

When any custom Task (Operator) is running, it will get a copy of the task instance passed to it; as well as being able to inspect task metadata, it also contains methods for things like :doc:`xcoms`.


Relationship Terminology
~~~~~~~~~~~~~~~~~~~~~~~~

For any given Task Instance, there are two types of relationships it has with other instances.

Firstly, it can have *upstream* and *downstream* tasks::

    task1 >> task2 >> task3

When a Dag runs, it will create instances for each of these tasks that are upstream/downstream of each other, but which all have the same data interval.

There may also be instances of the *same task*, but for different data intervals - from other runs of the same Dag. We call these *previous* and *next* - it is a different relationship to *upstream* and *downstream*!

.. note::

    Some older Airflow documentation may still use "previous" to mean "upstream". If you find an occurrence of this, please help us fix it!


.. _concepts:timeouts:

Timeouts
--------

If you want a task to have a maximum runtime, set its ``execution_timeout`` attribute to a ``datetime.timedelta`` value
that is the maximum permissible runtime. This applies to all Airflow tasks, including sensors. ``execution_timeout`` controls the
maximum time allowed for every execution. If ``execution_timeout`` is breached, the task times out and
``AirflowTaskTimeout`` is raised.

In addition, sensors have a ``timeout`` parameter. This only matters for sensors in ``reschedule`` mode. ``timeout`` controls the maximum
time allowed for the sensor to succeed. If ``timeout`` is breached, ``AirflowSensorTimeout`` will be raised and the sensor fails immediately
without retrying.

The following ``SFTPSensor`` example illustrates this. The ``sensor`` is in ``reschedule`` mode, meaning it
is periodically executed and rescheduled until it succeeds.

- Each time the sensor pokes the SFTP server, it is allowed to take maximum 60 seconds as defined by ``execution_timeout``.
- If it takes the sensor more than 60 seconds to poke the SFTP server, ``AirflowTaskTimeout`` will be raised.
  The sensor is allowed to retry when this happens. It can retry up to 2 times as defined by ``retries``.
- From the start of the first execution, till it eventually succeeds (i.e. after the file 'root/test' appears),
  the sensor is allowed maximum 3600 seconds as defined by ``timeout``. In other words, if the file
  does not appear on the SFTP server within 3600 seconds, the sensor will raise ``AirflowSensorTimeout``.
  It will not retry when this error is raised.
- If the sensor fails due to other reasons such as network outages during the 3600 seconds interval,
  it can retry up to 2 times as defined by ``retries``. Retrying does not reset the ``timeout``. It will
  still have up to 3600 seconds in total for it to succeed.

.. code-block:: python

    sensor = SFTPSensor(
        task_id="sensor",
        path="/root/test",
        execution_timeout=timedelta(seconds=60),
        timeout=3600,
        retries=2,
        mode="reschedule",
    )


SLAs
----

The SLA feature from Airflow 2 has been removed in 3.0 and was replaced in Airflow 3.1 with :doc:`Deadlines Alerts <../howto/deadline-alerts>`.

Special Exceptions
------------------

If you want to control your task's state from within custom Task/Operator code, Airflow provides two special exceptions you can raise:

* ``AirflowSkipException`` will mark the current task as skipped
* ``AirflowFailException`` will mark the current task as failed *ignoring any remaining retry attempts*

These can be useful if your code has extra knowledge about its environment and wants to fail/skip faster - e.g., skipping when it knows there's no data available, or fast-failing when it detects its API key is invalid (as that will not be fixed by a retry).

.. _concepts:task-instance-heartbeat-timeout:

Task Instance Heartbeat Timeout
-------------------------------

No system runs perfectly, and task instances are expected to die once in a while.

``TaskInstances`` may get stuck in a ``running`` state despite their associated jobs being inactive
(for example if the ``TaskInstance``'s worker ran out of memory). Such tasks were formerly known as zombie tasks. Airflow will find these
periodically, clean them up, and mark the ``TaskInstance`` as failed or retry it if it has available retries. The ``TaskInstance``'s heartbeat can timeout for
many reasons, including:

* The Airflow worker ran out of memory and was OOMKilled.
* The Airflow worker failed its liveness probe, so the system (for example, Kubernetes) restarted the worker.
* The system (for example, Kubernetes) scaled down and moved an Airflow worker from one node to another.


Reproducing task instance heartbeat timeouts locally
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you'd like to reproduce task instance heartbeat timeouts for development/testing processes, follow the steps below:

1. Set the below environment variables for your local Airflow setup (alternatively you could tweak the corresponding config values in airflow.cfg)

.. code-block:: bash

    export AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_SEC=600
    export AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT=2
    export AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT_DETECTION_INTERVAL=5


2. Have a Dag with a task that takes about 10 minutes to complete(i.e. a long-running task). For example, you could use the below Dag:

.. code-block:: python

    from airflow.sdk import dag
    from airflow.providers.standard.operators.bash import BashOperator
    from datetime import datetime


    @dag(start_date=datetime(2021, 1, 1), schedule="@once", catchup=False)
    def sleep_dag():
        t1 = BashOperator(
            task_id="sleep_10_minutes",
            bash_command="sleep 600",
        )


    sleep_dag()


Run the above Dag and wait for a while. The ``TaskInstance`` will be marked failed after <task_instance_heartbeat_timeout> seconds.



Executor Configuration
----------------------

Some :doc:`Executors <executor/index>` allow optional per-task configuration - such as the ``KubernetesExecutor``, which lets you set an image to run the task on.

This is achieved via the ``executor_config`` argument to a Task or Operator. Here's an example of setting the Docker image for a task that will run on the ``KubernetesExecutor``::

    MyOperator(...,
        executor_config={
            "KubernetesExecutor":
                {"image": "myCustomDockerImage"}
        }
    )

The settings you can pass into ``executor_config`` vary by executor, so read the :doc:`individual executor documentation <executor/index>` in order to see what you can set.
