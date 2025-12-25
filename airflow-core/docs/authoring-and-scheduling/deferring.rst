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

Standard :doc:`Operators </core-concepts/operators>` and :doc:`Sensors <../core-concepts/sensors>` take up a full *worker slot* for the entire time they are running, even if they are idle. For example, if you only have 100 worker slots available to run tasks, and you have 100 Dags waiting on a sensor that's currently running but idle, then you *cannot run anything else* - even though your entire Airflow cluster is essentially idle. ``reschedule`` mode for sensors solves some of this, by allowing sensors to only run at fixed intervals, but it is inflexible and only allows using time as the reason to resume, not other criteria.

This is where *Deferrable Operators* can be used. When it has nothing to do but wait, an operator can suspend itself and free up the worker for other processes by *deferring*. When an operator defers, execution moves to the triggerer, where the trigger specified by the operator will run.  The trigger can do the polling or waiting required by the operator. Then, when the trigger finishes polling or waiting, it sends a signal for the operator to resume its execution. During the deferred phase of execution, since work has been offloaded to the triggerer, the task no longer occupies a worker slot, and you have more free workload capacity. By default, tasks in a deferred state don't occupy pool slots. If you would like them to, you can change this by editing the pool in question.

*Triggers* are small, asynchronous pieces of Python code designed to run in a single Python process. Because they are asynchronous, they can all co-exist efficiently in the *triggerer* Airflow component.

An overview of how this process works:

* A task instance (running operator) reaches a point where it has to wait for other operations or conditions, and defers itself with a trigger tied to an event to resume it. This frees up the worker to run something else.
* The new trigger instance is registered by Airflow, and picked up by a triggerer process.
* The trigger runs until it fires, at which point its source task is re-scheduled by the scheduler.
* The scheduler queues the task to resume on a worker node.

You can either use pre-written deferrable operators as a Dag author or write your own. Writing them, however, requires that they meet certain design criteria.

Using Deferrable Operators
--------------------------

If you want to use pre-written deferrable operators that come with Airflow, such as ``TimeSensor``, then you only need to complete two steps:

* Ensure your Airflow installation runs at least one ``triggerer`` process, as well as the normal ``scheduler``
* Use deferrable operators/sensors in your Dags

Airflow automatically handles and implements the deferral processes for you.

If you're upgrading existing Dags to use deferrable operators, Airflow contains API-compatible sensor variants. Add these variants into your Dag to use deferrable operators with no other changes required.

Note that you can't use the deferral ability from inside custom PythonOperator or TaskFlow Python functions. Deferral is only available to traditional, class-based operators.

.. _deferring/writing:

Writing Deferrable Operators
----------------------------

When writing a deferrable operators these are the main points to consider:

* Your operator must defer itself with a trigger. You can use a trigger included in core Airflow, or you can write a custom one.
* Your operator will be stopped and removed from its worker while deferred, and no state persists automatically. You can persist state by instructing Airflow to resume the operator at a certain method or by passing certain kwargs.
* You can defer multiple times, and you can defer before or after your operator does significant work. Or, you can defer if certain conditions are met. For example, if a system does not have an immediate answer. Deferral is entirely under your control.
* Any operator can defer; no special marking on its class is needed, and it's not limited to sensors.
* If you want to add an operator or sensor that supports both deferrable and non-deferrable modes, it's suggested to add ``deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False)`` to the ``__init__`` method of the operator and use it to decide whether to run the operator in deferrable mode. You can configure the default value of ``deferrable`` for all the operators and sensors that support switching between deferrable and non-deferrable mode through ``default_deferrable`` in the ``operator`` section. Here's an example of a sensor that supports both modes.

.. code-block:: python

    import time
    from datetime import timedelta
    from typing import Any

    from airflow.configuration import conf
    from airflow.sdk import BaseSensorOperator, Context
    from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger


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


Writing Triggers
~~~~~~~~~~~~~~~~

A *Trigger* is written as a class that inherits from ``BaseTrigger``, and implements three methods:

* ``__init__``: A method to receive arguments from operators instantiating it. Since 2.10.0, we're able to start task execution directly from a pre-defined trigger. To utilize this feature, all the arguments in ``__init__`` must be serializable.
* ``run``: An asynchronous method that runs its logic and yields one or more ``TriggerEvent`` instances as an asynchronous generator.
* ``serialize``: Returns the information needed to re-construct this trigger, as a tuple of the classpath, and keyword arguments to pass to ``__init__``.

This example shows the structure of a basic trigger, a very simplified version of Airflow's ``DateTimeTrigger``:

.. code-block:: python

    import asyncio

    from airflow.triggers.base import BaseTrigger, TriggerEvent
    from airflow.sdk.timezone import utcnow


    class DateTimeTrigger(BaseTrigger):
        def __init__(self, moment):
            super().__init__()
            self.moment = moment

        def serialize(self):
            return ("airflow.providers.standard.triggers.temporal.DateTimeTrigger", {"moment": self.moment})

        async def run(self):
            while self.moment > utcnow():
                await asyncio.sleep(1)
            yield TriggerEvent(self.moment)

The code example shows several things:

* ``__init__`` and ``serialize`` are written as a pair. The trigger is instantiated once when it is submitted by the operator as part of its deferral request, then serialized and re-instantiated on any triggerer process that runs the trigger.
* The ``run`` method is declared as an ``async def``, as it *must* be asynchronous, and uses ``asyncio.sleep`` rather than the regular ``time.sleep`` (because that would block the process).
* When it emits its event it packs ``self.moment`` in there, so if this trigger is being run redundantly on multiple hosts, the event can be de-duplicated.

Triggers can be as complex or as simple as you want, provided they meet the design constraints. They can run in a highly-available fashion, and are auto-distributed among hosts running the triggerer. We encourage you to avoid any kind of persistent state in a trigger. Triggers should get everything they need from their ``__init__``, so they can be serialized and moved around freely.

If you are new to writing asynchronous Python, be very careful when writing your ``run()`` method. Python's async model means that code can block the entire process if it does not correctly ``await`` when it does a blocking operation. Airflow attempts to detect process blocking code and warn you in the triggerer logs when it happens. You can enable extra checks by Python by setting the variable ``PYTHONASYNCIODEBUG=1`` when you are writing your trigger to make sure you're writing non-blocking code. Be especially careful when doing filesystem calls, because if the underlying filesystem is network-backed, it can be blocking.

There's some design constraints to be aware of when writing your own trigger:

* The ``run`` method *must be asynchronous* (using Python's asyncio), and correctly ``await`` whenever it does a blocking operation.
* ``run`` must ``yield`` its TriggerEvents, not return them. If it returns before yielding at least one event, Airflow will consider this an error and fail any Task Instances waiting on it. If it throws an exception, Airflow will also fail any dependent task instances.
* You should assume that a trigger instance can run *more than once*. This can happen if a network partition occurs and Airflow re-launches a trigger on a separated machine. So, you must be mindful about side effects. For example you might not want to use a trigger to insert database rows.
* If your trigger is designed to emit more than one event (not currently supported), then each emitted event *must* contain a payload that can be used to deduplicate events if the trigger is running in multiple places. If you only fire one event and don't need to pass information back to the operator, you can just set the payload to ``None``.
* A trigger can suddenly be removed from one triggerer service and started on a new one. For example, if subnets are changed and a network partition results or if there is a deployment. If desired, you can implement the ``cleanup`` method, which is always called after ``run``, whether the trigger exits cleanly or otherwise.
* In order for any changes to a trigger to be reflected, the *triggerer* needs to be restarted whenever the trigger is modified.
* Your trigger must not come from a Dag bundle - anywhere else on ``sys.path`` is fine. The triggerer does not initialize any bundles when running a trigger.

.. note::

    Currently triggers are only used until their first event, because they are only used for resuming deferred tasks, and tasks resume after the first event fires. However, Airflow plans to allow dags to be launched from triggers in future, which is where multi-event triggers will be more useful.


Sensitive information in triggers
'''''''''''''''''''''''''''''''''
Since Airflow 2.9.0, triggers kwargs are serialized and encrypted before being stored in the database. This means that any sensitive information you pass to a trigger will be stored in the database in an encrypted form, and decrypted when it is read from the database.

Triggering Deferral
~~~~~~~~~~~~~~~~~~~

If you want to trigger deferral, at any place in your operator, you can call ``self.defer(trigger, method_name, kwargs, timeout)``. This raises a special exception for Airflow. The arguments are:

* ``trigger``: An instance of a trigger that you want to defer to. It will be serialized into the database.
* ``method_name``: The method name on your operator that you want Airflow to call when it resumes.
* ``kwargs``: (Optional) Additional keyword arguments to pass to the method when it is called. Defaults to ``{}``.
* ``timeout``: (Optional) A timedelta that specifies a timeout after which this deferral will fail, and fail the task instance. Defaults to ``None``, which means no timeout.

Here's a basic example of how a sensor might trigger deferral:

.. code-block:: python

    from __future__ import annotations

    from datetime import timedelta
    from typing import Any

    from airflow.sdk import BaseSensorOperator, Context
    from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger


    class WaitOneHourSensor(BaseSensorOperator):
        def execute(self, context: Context) -> None:
            self.defer(trigger=TimeDeltaTrigger(timedelta(hours=1)), method_name="execute_complete")

        def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
            # We have no more work to do here. Mark as complete.
            return


When you opt to defer, your operator will stop executing at that point and be removed from its current worker. No state will persist, such as local variables or attributes set on ``self``. When your operator resumes, it resumes as a new instance of it. The only way you can pass state from the old instance of the operator to the new one is with ``method_name`` and ``kwargs``.

When your operator resumes, Airflow adds a ``context`` object and an ``event`` object to the kwargs passed to the ``method_name`` method. This ``event`` object contains the payload from the trigger event that resumed your operator. Depending on the trigger, this can be useful to your operator, like it's a status code or URL to fetch results. Or, it might be unimportant information, like a datetime. Your ``method_name`` method, however, *must* accept ``context`` and ``event`` as a keyword argument.

If your operator returns from either its first ``execute()`` method when it's new, or a subsequent method specified by ``method_name``, it will be considered complete and finish executing.

Let's take a deeper look into the ``WaitOneHourSensor`` example above. This sensor is just a thin wrapper around the trigger. It defers to the trigger, and specifies a different method to come back to when the trigger fires.  When it returns immediately, it marks the sensor as successful.

The ``self.defer`` call raises the ``TaskDeferred`` exception, so it can work anywhere inside your operator's code, even when nested many calls deep inside ``execute()``. You can also raise ``TaskDeferred`` manually, which uses the same arguments as ``self.defer``.

``execution_timeout`` on operators is determined from the *total runtime*, not individual executions between deferrals. This means that if ``execution_timeout`` is set, an operator can fail while it's deferred or while it's running after a deferral, even if it's only been resumed for a few seconds.

Deferring multiple times
~~~~~~~~~~~~~~~~~~~~~~~~

Imagine a scenario where you would like your operator to iterate over a list of items that could vary in length, and defer processing of each item.

For example, submitting multiple queries to a database, or processing multiple files.

You can set ``method_name`` to ``execute`` if you want your operator to have one entrypoint, but it must also accept ``event`` as an optional keyword argument.

Below is an outline of how you can achieve this.

.. code-block:: python

    import asyncio

    from airflow.sdk import BaseOperator
    from airflow.triggers.base import BaseTrigger, TriggerEvent


    class MyItemTrigger(BaseTrigger):
        def __init__(self, item):
            super().__init__()
            self.item = item

        def serialize(self):
            return (self.__class__.__module__ + "." + self.__class__.__name__, {"item": self.item})

        async def run(self):
            result = None
            try:
                # Somehow process the item to calculate the result
                ...
                yield TriggerEvent({"result": result})
            except Exception as e:
                yield TriggerEvent({"error": str(e)})


    class MyItemsOperator(BaseOperator):
        def __init__(self, items, **kwargs):
            super().__init__(**kwargs)
            self.items = items

        def execute(self, context, current_item_index=0, event=None):
            last_result = None
            if event is not None:
                # execute method was deferred
                if "error" in event:
                    raise Exception(event["error"])
                last_result = event["result"]
                current_item_index += 1

            try:
                current_item = self.items[current_item_index]
            except IndexError:
                return last_result

            self.defer(
                trigger=MyItemTrigger(item),
                method_name="execute",  # The trigger will call this same method again
                kwargs={"current_item_index": current_item_index},
            )


Triggering Deferral from Task Start
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 .. versionadded:: 2.10.0

If you want to defer your task directly to the triggerer without going into the worker, you can set class level attribute ``start_from_trigger`` to ``True`` and add a class level attribute ``start_trigger_args`` with an ``StartTriggerArgs`` object with the following 4 attributes to your deferrable operator:

* ``trigger_cls``: An importable path to your trigger class.
* ``trigger_kwargs``: Keyword arguments to pass to the ``trigger_cls`` when it's initialized. **Note that all the arguments need to be serializable by Airflow. It's the main limitation of this feature.**
* ``next_method``: The method name on your operator that you want Airflow to call when it resumes.
* ``next_kwargs``: Additional keyword arguments to pass to the ``next_method`` when it is called.
* ``timeout``: (Optional) A timedelta that specifies a timeout after which this deferral will fail, and fail the task instance. Defaults to ``None``, which means no timeout.

In the sensor part, we'll need to provide the path to ``TimeDeltaTrigger`` as ``trigger_cls``.

.. code-block:: python

    from __future__ import annotations

    from datetime import timedelta
    from typing import Any

    from airflow.sdk import BaseSensorOperator, Context, StartTriggerArgs


    class WaitOneHourSensor(BaseSensorOperator):
        start_trigger_args = StartTriggerArgs(
            trigger_cls="airflow.providers.standard.triggers.temporal.TimeDeltaTrigger",
            trigger_kwargs={"moment": timedelta(hours=1)},
            next_method="execute_complete",
            next_kwargs=None,
            timeout=None,
        )
        start_from_trigger = True

        def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
            # We have no more work to do here. Mark as complete.
            return


``start_from_trigger`` and ``trigger_kwargs`` can also be modified at the instance level for more flexible configuration.

.. code-block:: python

    from __future__ import annotations

    from datetime import timedelta
    from typing import Any

    from airflow.sdk import BaseSensorOperator, Context, StartTriggerArgs


    class WaitHoursSensor(BaseSensorOperator):
        start_trigger_args = StartTriggerArgs(
            trigger_cls="airflow.providers.standard.triggers.temporal.TimeDeltaTrigger",
            trigger_kwargs={"moment": timedelta(hours=1)},
            next_method="execute_complete",
            next_kwargs=None,
            timeout=None,
        )
        start_from_trigger = True

        def __init__(self, *args: list[Any], **kwargs: dict[str, Any]) -> None:
            super().__init__(*args, **kwargs)
            self.start_trigger_args.trigger_kwargs = {"hours": 2}
            self.start_from_trigger = True

        def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
            # We have no more work to do here. Mark as complete.
            return


The initialization stage of mapped tasks occurs after the scheduler submits them to the executor. Thus, this feature offers limited dynamic task mapping support and its usage differs from standard practices. To enable dynamic task mapping support, you need to define ``start_from_trigger`` and ``trigger_kwargs`` in the ``__init__`` method. **Note that you don't need to define both of them to use this feature, but you need to use the exact same parameter name.** For example, if you define an argument as ``t_kwargs`` and assign this value to ``self.start_trigger_args.trigger_kwargs``, it will not have any effect. The entire ``__init__`` method will be skipped when mapping a task whose ``start_from_trigger`` is set to True. The scheduler will use the provided ``start_from_trigger`` and ``trigger_kwargs`` from ``partial`` and ``expand`` (with a fallback to the ones from class attributes if not provided) to determine whether and how to submit tasks to the executor or the triggerer. Note that XCom values won't be resolved at this stage.

After the trigger has finished executing, the task may be sent back to the worker to execute the ``next_method``, or the task instance may end directly. (Refer to :ref:`Exiting deferred task from Triggers<deferring/exiting_from_trigger>`) If the task is sent back to the worker, the arguments in the ``__init__`` method will still take effect before the ``next_method`` is executed, but they will not affect the execution of the trigger.


.. code-block:: python

    from __future__ import annotations

    from datetime import timedelta
    from typing import Any

    from airflow.sdk import BaseSensorOperator, Context, StartTriggerArgs


    class WaitHoursSensor(BaseSensorOperator):
        start_trigger_args = StartTriggerArgs(
            trigger_cls="airflow.providers.standard.triggers.temporal.TimeDeltaTrigger",
            trigger_kwargs={"moment": timedelta(hours=1)},
            next_method="execute_complete",
            next_kwargs=None,
            timeout=None,
        )
        start_from_trigger = True

        def __init__(
            self,
            *args: list[Any],
            trigger_kwargs: dict[str, Any] | None,
            start_from_trigger: bool,
            **kwargs: dict[str, Any],
        ) -> None:
            # This whole method will be skipped during dynamic task mapping.

            super().__init__(*args, **kwargs)
            self.start_trigger_args.trigger_kwargs = trigger_kwargs
            self.start_from_trigger = start_from_trigger

        def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
            # We have no more work to do here. Mark as complete.
            return


This will be expanded into 2 tasks, with their "hours" arguments set to 1 and 2 respectively.

.. code-block:: python

    WaitHoursSensor.partial(task_id="wait_for_n_hours", start_from_trigger=True).expand(
        trigger_kwargs=[{"hours": 1}, {"hours": 2}]
    )


.. _deferring/exiting_from_trigger:

Exiting deferred task from Triggers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 .. versionadded:: 2.10.0

If you want to exit your task directly from the triggerer without going into the worker, you can specify the instance level attribute ``end_from_trigger`` with the attributes of your deferrable operator, as discussed above. This can save some resources needed to start a new worker.

Triggers can have two options: they can either send execution back to the worker or end the task instance directly. If the trigger ends the task instance itself, the ``method_name`` does not matter and can be ``None``. Otherwise, provide ``method_name`` that should be used when resuming execution in the task.

.. code-block:: python

    class WaitFiveHourSensorAsync(BaseSensorOperator):
        # this sensor always exits from trigger.
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            self.end_from_trigger = True

        def execute(self, context: Context) -> NoReturn:
            self.defer(
                method_name=None,
                trigger=WaitFiveHourTrigger(duration=timedelta(hours=5), end_from_trigger=self.end_from_trigger),
            )


``TaskSuccessEvent`` and ``TaskFailureEvent`` are the two events that can be used to end the task instance directly. This marks the task with the state ``task_instance_state`` and optionally pushes xcom if applicable. Here's an example of how to use these events:

.. code-block:: python


    class WaitFiveHourTrigger(BaseTrigger):
        def __init__(self, duration: timedelta, *, end_from_trigger: bool = False):
            super().__init__()
            self.duration = duration
            self.end_from_trigger = end_from_trigger

        def serialize(self) -> tuple[str, dict[str, Any]]:
            return (
                "your_module.WaitFiveHourTrigger",
                {"duration": self.duration, "end_from_trigger": self.end_from_trigger},
            )

        async def run(self) -> AsyncIterator[TriggerEvent]:
            await asyncio.sleep(self.duration.total_seconds())
            if self.end_from_trigger:
                yield TaskSuccessEvent()
            else:
                yield TriggerEvent({"duration": self.duration})

In the above example, the trigger will end the task instance directly if ``end_from_trigger`` is set to ``True`` by yielding ``TaskSuccessEvent``. Otherwise, it will resume the task instance with the method specified in the operator.

.. note::
    Exiting from the trigger works only when listeners are not integrated for the deferrable operator. Currently, when deferrable operator has the ``end_from_trigger`` attribute set to ``True`` and listeners are integrated it raises an exception during parsing to indicate this limitation. While writing the custom trigger, ensure that the trigger is not set to end the task instance directly if the listeners are added from plugins. If the ``end_from_trigger`` attribute is changed to different attribute by author of trigger, the Dag parsing would not raise any exception and the listeners dependent on this task would not work. This limitation will be addressed in future releases.


High Availability
-----------------

Triggers are designed to work in a high availability (HA) architecture. If you want to run a high availability setup, run multiple copies of ``triggerer`` on multiple hosts. Much like ``scheduler``, they automatically co-exist with correct locking and HA.

Depending on how much work the triggers are doing, you can fit hundreds to tens of thousands of triggers on a single ``triggerer`` host. By default, every ``triggerer`` has a capacity of 1000 triggers that it can try to run at once. You can change the number of triggers that can run simultaneously with the ``--capacity`` argument. If you have more triggers trying to run than you have capacity across all of your ``triggerer`` processes, some triggers will be delayed from running until others have completed.

Airflow tries to only run triggers in one place at once, and maintains a heartbeat to all ``triggerers`` that are currently running. If a ``triggerer`` dies, or becomes partitioned from the network where Airflow's database is running, Airflow automatically re-schedules triggers that were on that host to run elsewhere. Airflow waits (2.1 * ``triggerer.job_heartbeat_sec``) seconds for the machine to re-appear before rescheduling the triggers.

This means it's possible, but unlikely, for triggers to run in multiple places at once. This behavior is designed into the trigger contract, however, and is expected behavior. Airflow de-duplicates events fired when a trigger is running in multiple places simultaneously, so this process is transparent to your operators.

Note that every extra ``triggerer`` you run results in an extra persistent connection to your database.

Balance the workload for HA Triggerers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 3.2.0

A Triggerer will select only ``[triggerer] max_trigger_to_select_per_loop`` triggers per loop to avoid starving other Triggerers in HA deployments. It is recommended to set this value significantly lower than ``[triggerer] capacity`` to help keep the load balanced across Triggerers. Currently, the default value of ``max_trigger_to_select_per_loop`` is ``50``, while the default ``capacity`` is ``1000``.

According to `benchmarks <https://github.com/apache/airflow/pull/58803#pullrequestreview-3549403487>`_, two Triggerers can still claim 1,000 triggers within one second while maintaining an almost even load distribution with the default settings.

You can determine a suitable value for your deployment by creating a large number of triggers (for example, by triggering a Dag with many deferrable tasks) and observing both how the load is distributed across Triggerers in your environment and how long it takes for all Triggerers to pick up the triggers.


Controlling Triggerer Host Assignment Per Trigger
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 3.2.0

Under some circumstances, it may be desirable to assign a Trigger to a specific subset of ``triggerer`` hosts. Some examples of when this might be desirable are:

* In a multi-tenant Airflow system where you run a distinct set of ``triggerers`` per team.
* Running distinct sets of ``triggerers`` hosts, where each set of hosts are configured for different trigger operations (e.g. each set of triggerers may have different cloud permissions).

To achieve trigger assignment, you may use the optional "trigger queues" feature.

To use trigger queues, do the following:

1. Set the :ref:`config:triggerer__queues_enabled` config value to ``true``. This will ensure when tasks defer, they pass their assigned task queue to the newly registered trigger instance.
2. For a given ``triggerer`` host(s), add ``--queues=<comma-separated string of task queue names to consume from>`` to the Triggerers' startup command. This option ensures the triggerer will only pick up ``trigger`` instances deferred by tasks from the specified task queues.

For example, let's say you are running two triggerer hosts (labeled "X", and "Y") with the following commands:

.. code-block:: bash

      # triggerer "X" startup command
      airflow triggerer --queues=alice,bob
      # triggerer "Y" startup command
      airflow triggerer --queues=test_q

In this scenario, triggerer "X" will exclusively run triggers deferred from tasks originating from task queue ``"alice"`` or ``"bob"``.
Similarly, triggerer "Y" will exclusively run triggers deferred from tasks originating from task queue ``"test_q"``.

Trigger Queue Assignment Caveats
''''''''''''''''''''''''''''''''

This feature is only compatible with executors which utilize the task ``queue`` concept (such as the CeleryExecutor).
Similarly, queue assignment is only compatible with triggers associated with a task. Triggers associated with non-task
entities like assets or :doc:`administration-and-deployment/logging-monitoring/callbacks.rst` will have their ``queue`` value set to ``None``.

.. note::
    To enable this feature, you must set the ``--queues`` option on the triggerers' startup command.
    If you set the ``queue`` value of a trigger instance to some value which is not present in the ``--queues`` option, that trigger will never run.
    Similarly, all ``triggerer`` instances running without the ``--queues`` option will only consume trigger instances registered without a ``queue`` value.


The following example shows how to only assign some triggers to a specific triggerer host, while leaving other trigger instances unconstrained:

.. code-block:: bash

      # triggerer "A" startup command, consumes only from trigger queues "alice" and "bob"
      airflow triggerer --queues=alice,bob
      # triggerer "B" startup command, consumes only from triggers which have no assigned queue value (the default behavior).
      airflow triggerer

In this scenario, triggerer "A" will only run trigger instances with an assigned ``queue`` value of ``"alice"``, or ``"bob"``; whereas,
triggerer "B" will only run trigger instances with no ``queue`` value assigned.


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
