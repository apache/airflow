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

Sensors
========

Sensors are a special type of :doc:`Operator <operators>` that are designed to do exactly one thing - wait for something to occur. It can be time-based, or waiting for a file, or an external event, but all they do is wait until something happens, and then *succeed* so their downstream tasks can run.
Or, in the case when that thing does _not_ happen within the configured timeout, *fails* so that you can be alerted to the failure through the usual mechanisms.

Because they are primarily idle, Sensors have two different modes of running so you can be a bit more efficient about using them:

* ``poke`` (default): The Sensor takes up a worker slot for its entire runtime
* ``reschedule``: The Sensor takes up a worker slot only when it is checking, and sleeps for a set duration between checks

The ``poke`` and ``reschedule`` modes can be configured directly when you instantiate the sensor; generally, the trade-off between them is latency. Something that is checking every second should be in ``poke`` mode, while something that is checking every minute should be in ``reschedule`` mode.

Much like Operators, Airflow has a large set of pre-built Sensors you can use, both in core Airflow as well as via our *providers* system.

.. seealso:: :doc:`../authoring-and-scheduling/deferring`

BaseSensorOperator
-------------------

All sensors share common functionality through the ``BaseSensorOperator`` class. This base class provides
standardized parameters that control timing behavior, execution modes, and failure handling across
every sensor implementation.

.. note::
   In the Task SDK architecture, ``BaseSensorOperator`` is defined in the ``airflow.sdk`` package.
   Provider documentation is generated separately from the core documentation, so sensor parameters
   may not always appear in individual provider sensor API reference pages. However, these parameters
   are universally available to all sensors.

Common Sensor Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~

The following parameters control sensor behavior across all implementations:

``poke_interval``
    Specifies how long to wait (in seconds) between successive checks. In ``poke`` mode, the sensor
    holds a worker slot while waiting. In ``reschedule`` mode, the task is freed and rescheduled after
    each interval. Accepts either a ``float`` (seconds) or ``timedelta`` object.

``timeout``
    Maximum time (in seconds) before the sensor fails due to exceeding its allowed runtime.
    The timeout is calculated from the first execution attempt, not from individual poke attempts.
    This differs from ``execution_timeout`` which only measures active task execution time.
    Accepts either a ``float`` (seconds) or ``timedelta`` object.

``mode``
    Controls how the sensor consumes worker resources:

    * ``poke`` (default): The sensor occupies a worker slot continuously while waiting
    * ``reschedule``: The sensor releases the worker slot between checks, freeing resources for other tasks

``soft_fail``
    When ``True``, timeout causes the sensor to be marked as ``SKIPPED`` instead of ``FAILED``.
    This is useful when you want non-critical sensors to timeout gracefully without failing the DAG.

``exponential_backoff``
    When enabled, increases the wait time between pokes exponentially, rather than using a fixed
    interval. This is helpful when checking external systems that may have temporary availability
    issues or rate limiting.

``max_wait``
    Sets an upper limit on the wait interval when ``exponential_backoff`` is enabled. Prevents
    excessively long gaps between checks. Accepts either a ``float`` (seconds) or ``timedelta`` object.

``silent_fail``
    When ``True``, exceptions during poke (other than timeout-related exceptions) are logged but
    do not cause sensor failure. The sensor continues checking.

``never_fail``
    When ``True``, any exception during poke causes the sensor to be marked as ``SKIPPED`` instead of
    ``FAILED``. Mutually exclusive with ``soft_fail``.

For detailed API documentation, refer to the Task SDK reference:

https://airflow.apache.org/docs/task-sdk/stable/api.html#airflow.sdk.BaseSensorOperator
