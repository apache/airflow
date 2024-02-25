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

Deferrable Operators & Triggers
===============================

Standard :doc:`Operators </core-concepts/operators>` and :doc:`Sensors <../core-concepts/sensors>` take up a full *worker slot* for the entire time they are running, even if they are idle. For example, if you only have 100 worker slots available to run tasks, and you have 100 DAGs waiting on a sensor that's currently running but idle, then you *cannot run anything else* - even though your entire Airflow cluster is essentially idle. ``reschedule`` mode for sensors solves some of this, by allowing sensors to only run at fixed intervals, but it is inflexible and only allows using time as the reason to resume, not other criteria.

This is where *Deferrable Operators* can be used. When it has nothing to do but wait, an operator can suspend itself and free up the worker for other processes by *deferring*. When an operator defers, execution moves to the triggerer, where the trigger specified by the operator will run.  The trigger can do the polling or waiting required by the operator. Then, when the trigger finishes polling or waiting, it sends a signal for the operator to resume its execution. During the deferred phase of execution, since work has been offloaded to the triggerer, the task no longer occupies a worker slot, and you have more free workload capacity. By default, tasks in a deferred state don't occupy pool slots. If you would like them to, you can change this by editing the pool in question.

*Triggers* are small, asynchronous pieces of Python code designed to run in a single Python process. Because they are asynchronous, they can all co-exist efficiently in the *triggerer* Airflow component.

An overview of how this process works:

* A task instance (running operator) reaches a point where it has to wait for other operations or conditions, and defers itself with a trigger tied to an event to resume it. This frees up the worker to run something else.
* The new trigger instance is registered by Airflow, and picked up by a triggerer process.
* The trigger runs until it fires, at which point its source task is re-scheduled by the scheduler.
* The scheduler queues the task to resume on a worker node.

You can either use pre-written deferrable operators as a DAG author or write your own. Writing them, however, requires that they meet certain design criteria.

Using Deferrable Operators
--------------------------

If you want to use pre-written deferrable operators that come with Airflow, such as ``TimeSensorAsync``, then you only need to complete two steps:

* Ensure your Airflow installation runs at least one ``triggerer`` process, as well as the normal ``scheduler``
* Use deferrable operators/sensors in your DAGs

Airflow automatically handles and implements the deferral processes for you.

If you're upgrading existing DAGs to use deferrable operators, Airflow contains API-compatible sensor variants, like ``TimeSensorAsync`` for ``TimeSensor``. Add these variants into your DAG to use deferrable operators with no other changes required.

Note that you can't use the deferral ability from inside custom PythonOperator or TaskFlow Python functions. Deferral is only available to traditional, class-based operators.

.. _deferring/writing:

Writing Deferrable Operators
----------------------------

When writing a deferrable operators these are the main points to consider:

* Your operator must defer itself with a trigger. You can use a trigger included in core Airflow, or you can write a custom one.
* Your operator will be stopped and removed from its worker while deferred, and no state persists automatically. You can persist state by instructing Airflow to resume the operator at a certain method or by passing certain kwargs.
* You can defer multiple times, and you can defer before or after your operator does significant work. Or, you can defer if certain conditions are met. For example, if a system does not have an immediate answer. Deferral is entirely under your control.
* Any operator can defer; no special marking on its class is needed, and it's not limited to sensors.
* In order for any changes to a trigger to be reflected, the *triggerer* needs to be restarted whenever the trigger is modified.
* If you want to add an operator or sensor that supports both deferrable and non-deferrable modes, it's suggested to add ``deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False)`` to the ``__init__`` method of the operator and use it to decide whether to run the operator in deferrable mode. You can configure the default value of ``deferrable`` for all the operators and sensors that support switching between deferrable and non-deferrable mode through ``default_deferrable`` in the ``operator`` section. Here's an example of a sensor that supports both modes.

.. code-block:: python

    import time
    from datetime import timedelta
    from typing import Any

    from airflow.configuration import conf
    from airflow.sensors.base import BaseSensorOperator
    from airflow.triggers.temporal import TimeDeltaTrigger
    from airflow.utils.context import Context


    class WaitOneHourSensor(BaseSensorOperator):
        def __init__(
            self, deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False), **kwargs
        ) -> None:
            super().__init__(**kwargs)
            self.deferrable = deferrable

        def execute(self, context: Context) -> None:
            if self.deferrable:
                self.defer(
                    trigger=TimeDeltaTrigger(timedelta(hours=1)),
                    method_name="execute_complete",
                )
            else:
                time.sleep(3600)

        def execute_complete(
            self,
            context: Context,
            event: dict[str, Any] | None = None,
        ) -> None:
            # We have no more work to do here. Mark as complete.
            return

Triggering Deferral
~~~~~~~~~~~~~~~~~~~

If you want to trigger deferral, at any place in your operator, you can call ``self.defer(trigger, method_name, kwargs, timeout)``. This raises a special exception for Airflow. The arguments are:

* ``trigger``: An instance of a trigger that you want to defer to. It will be serialized into the database.
* ``method_name``: The method name on your operator that you want Airflow to call when it resumes.
* ``kwargs``: (Optional) Additional keyword arguments to pass to the method when it is called. Defaults to ``{}``.
* ``timeout``: (Optional) A timedelta that specifies a timeout after which this deferral will fail, and fail the task instance. Defaults to ``None``, which means no timeout.

When you opt to defer, your operator will stop executing at that point and be removed from its current worker. No state will persist, such as local variables or attributes set on ``self``. When your operator resumes, it resumes as a new instance of it. The only way you can pass state from the old instance of the operator to the new one is with ``method_name`` and ``kwargs``.

When your operator resumes, Airflow adds an ``event`` object to the kwargs passed to the ``method_name`` method. This ``event`` object contains the payload from the trigger event that resumed your operator. Depending on the trigger, this can be useful to your operator, like it's a status code or URL to fetch results. Or, it might be unimportant information, like a datetime. Your ``method_name`` method, however, *must* accept ``event`` as a keyword argument.

If your operator returns from either its first ``execute()`` method when it's new, or a subsequent method specified by ``method_name``, it will be considered complete and finish executing.

You can set ``method_name`` to ``execute`` if you want your operator to have one entrypoint, but it must also accept ``event`` as an optional keyword argument.

Here's a basic example of how a sensor might trigger deferral:

.. code-block:: python

    from datetime import timedelta
    from typing import Any

    from airflow.sensors.base import BaseSensorOperator
    from airflow.triggers.temporal import TimeDeltaTrigger
    from airflow.utils.context import Context


    class WaitOneHourSensor(BaseSensorOperator):
        def execute(self, context: Context) -> None:
            self.defer(trigger=TimeDeltaTrigger(timedelta(hours=1)), method_name="execute_complete")

        def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
            # We have no more work to do here. Mark as complete.
            return

This sensor is just a thin wrapper around the trigger. It defers to the trigger, and specifies a different method to come back to when the trigger fires.  When it returns immediately, it marks the sensor as successful.

The ``self.defer`` call raises the ``TaskDeferred`` exception, so it can work anywhere inside your operator's code, even when nested many calls deep inside ``execute()``. You can also raise ``TaskDeferred`` manually, which uses the same arguments as ``self.defer``.

``execution_timeout`` on operators is determined from the *total runtime*, not individual executions between deferrals. This means that if ``execution_timeout`` is set, an operator can fail while it's deferred or while it's running after a deferral, even if it's only been resumed for a few seconds.

Writing Triggers
~~~~~~~~~~~~~~~~

A *Trigger* is written as a class that inherits from ``BaseTrigger``, and implements three methods:

* ``__init__``: A method to receive arguments from operators instantiating it.
* ``run``: An asynchronous method that runs its logic and yields one or more ``TriggerEvent`` instances as an asynchronous generator.
* ``serialize``: Returns the information needed to re-construct this trigger, as a tuple of the classpath, and keyword arguments to pass to ``__init__``.

There's some design constraints to be aware of when writing your own trigger:

* The ``run`` method *must be asynchronous* (using Python's asyncio), and correctly ``await`` whenever it does a blocking operation.
* ``run`` must ``yield`` its TriggerEvents, not return them. If it returns before yielding at least one event, Airflow will consider this an error and fail any Task Instances waiting on it. If it throws an exception, Airflow will also fail any dependent task instances.
* You should assume that a trigger instance can run *more than once*. This can happen if a network partition occurs and Airflow re-launches a trigger on a separated machine. So, you must be mindful about side effects. For example you might not want to use a trigger to insert database rows.
* If your trigger is designed to emit more than one event (not currently supported), then each emitted event *must* contain a payload that can be used to deduplicate events if the trigger is running in multiple places. If you only fire one event and don't need to pass information back to the operator, you can just set the payload to ``None``.
* A trigger can suddenly be removed from one triggerer service and started on a new one. For example, if subnets are changed and a network partition results or if there is a deployment. If desired, you can implement the ``cleanup`` method, which is always called after ``run``, whether the trigger exits cleanly or otherwise.

.. note::

    Currently triggers are only used until their first event, because they are only used for resuming deferred tasks, and tasks resume after the first event fires. However, Airflow plans to allow DAGs to be launched from triggers in future, which is where multi-event triggers will be more useful.


This example shows the structure of a basic trigger, a very simplified version of Airflow's ``DateTimeTrigger``:

.. code-block:: python

    import asyncio

    from airflow.triggers.base import BaseTrigger, TriggerEvent
    from airflow.utils import timezone


    class DateTimeTrigger(BaseTrigger):
        def __init__(self, moment):
            super().__init__()
            self.moment = moment

        def serialize(self):
            return ("airflow.triggers.temporal.DateTimeTrigger", {"moment": self.moment})

        async def run(self):
            while self.moment > timezone.utcnow():
                await asyncio.sleep(1)
            yield TriggerEvent(self.moment)


The code example shows several things:

* ``__init__`` and ``serialize`` are written as a pair. The trigger is instantiated once when it is submitted by the operator as part of its deferral request, then serialized and re-instantiated on any triggerer process that runs the trigger.
* The ``run`` method is declared as an ``async def``, as it *must* be asynchronous, and uses ``asyncio.sleep`` rather than the regular ``time.sleep`` (because that would block the process).
* When it emits its event it packs ``self.moment`` in there, so if this trigger is being run redundantly on multiple hosts, the event can be de-duplicated.

Triggers can be as complex or as simple as you want, provided they meet the design constraints. They can run in a highly-available fashion, and are auto-distributed among hosts running the triggerer. We encourage you to avoid any kind of persistent state in a trigger. Triggers should get everything they need from their ``__init__``, so they can be serialized and moved around freely.

If you are new to writing asynchronous Python, be very careful when writing your ``run()`` method. Python's async model means that code can block the entire process if it does not correctly ``await`` when it does a blocking operation. Airflow attempts to detect process blocking code and warn you in the triggerer logs when it happens. You can enable extra checks by Python by setting the variable ``PYTHONASYNCIODEBUG=1`` when you are writing your trigger to make sure you're writing non-blocking code. Be especially careful when doing filesystem calls, because if the underlying filesystem is network-backed, it can be blocking.

Sensitive information in triggers
'''''''''''''''''''''''''''''''''

Triggers are serialized and stored in the database, so they can be re-instantiated on any triggerer process. This means that any sensitive information you pass to a trigger will be stored in the database.
If you want to pass sensitive information to a trigger, you can encrypt it before passing it to the trigger, and decrypt it inside the trigger, or update the argument name in the ``serialize`` method by adding ``encrypted__`` as a prefix, and Airflow will automatically encrypt the argument before storing it in the database, and decrypt it when it is read from the database.

.. code-block:: python

    class MyTrigger(BaseTrigger):
        def __init__(self, param, secret):
            super().__init__()
            self.param = param
            self.secret = secret

        def serialize(self):
            return (
                "airflow.triggers.MyTrigger",
                {
                    "param": self.param,
                    "encrypted__secret": self.secret,
                },
            )

        async def run(self):
            # self.my_secret will be decrypted here
            ...

High Availability
-----------------

Triggers are designed to work in a high availability (HA) architecture. If you want to run a high availability setup, run multiple copies of ``triggerer`` on multiple hosts. Much like ``scheduler``, they automatically co-exist with correct locking and HA.

Depending on how much work the triggers are doing, you can fit hundreds to tens of thousands of triggers on a single ``triggerer`` host. By default, every ``triggerer`` has a capacity of 1000 triggers that it can try to run at once. You can change the number of triggers that can run simultaneously with the ``--capacity`` argument. If you have more triggers trying to run than you have capacity across all of your ``triggerer`` processes, some triggers will be delayed from running until others have completed.

Airflow tries to only run triggers in one place at once, and maintains a heartbeat to all ``triggerers`` that are currently running. If a ``triggerer`` dies, or becomes partitioned from the network where Airflow's database is running, Airflow automatically re-schedules triggers that were on that host to run elsewhere. Airflow waits (2.1 * ``triggerer.job_heartbeat_sec``) seconds for the machine to re-appear before rescheduling the triggers.

This means it's possible, but unlikely, for triggers to run in multiple places at once. This behavior is designed into the trigger contract, however, and is expected behavior. Airflow de-duplicates events fired when a trigger is running in multiple places simultaneously, so this process is transparent to your operators.

Note that every extra ``triggerer`` you run results in an extra persistent connection to your database.

Difference between Mode='reschedule' and Deferrable=True in Sensors
-------------------------------------------------------------------

In Airflow, sensors wait for specific conditions to be met before proceeding with downstream tasks. Sensors have two options for managing idle periods: ``mode='reschedule'`` and ``deferrable=True``. Because ``mode='reschedule'`` is a parameter specific to the BaseSensorOperator in Airflow, it allows the sensor to reschedule itself if the condition is not met. ``'deferrable=True'`` is a convention used by some operators to indicate that the task can be retried (or deferred) later, but it is not a built-in parameter or mode in Airflow. The actual behavior of retrying the task varies depending on the specific operator implementation.

+--------------------------------------------------------+--------------------------------------------------------+
|           mode='reschedule'                            |          deferrable=True                               |
+========================================================+========================================================+
| Continuously reschedules itself until condition is met |  Pauses execution when idle, resumes when condition    |
|                                                        |  changes                                               |
+--------------------------------------------------------+--------------------------------------------------------+
| Resource use is higher (repeated execution)            |  Resource use is lower (pauses when idle, frees        |
|                                                        |  up worker slots)                                      |
+--------------------------------------------------------+--------------------------------------------------------+
| Conditions expected to change over time                |  Waiting for external events or resources              |
| (e.g. file creation)                                   |  (e.g. API response)                                   |
+--------------------------------------------------------+--------------------------------------------------------+
| Built-in functionality for rescheduling                |  Requires custom logic to defer task and handle        |
|                                                        |  external changes                                      |
+--------------------------------------------------------+--------------------------------------------------------+
