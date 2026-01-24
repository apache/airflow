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

BaseSensorOperator parameters
-----------------------------

All sensors in Airflow ultimately inherit from ``BaseSensorOperator`` (directly or indirectly).
This base class defines the common behavior and parameters that control
how a sensor waits, retries, and manages worker resources.

As of the Task SDK refactor, ``BaseSensorOperator`` is implemented in the
Task SDK. Because provider documentation is generated separately, these
parameters may not always be directly visible on individual provider
sensor API pages. However, they apply to *all* sensors.

Common parameters
^^^^^^^^^^^^^^^^^

The following parameters are provided by ``BaseSensorOperator`` and are
available on all sensors:

``poke_interval``
    Time in seconds between successive checks. In ``poke`` mode, the sensor
    sleeps between checks while occupying a worker slot. In ``reschedule``
    mode, the task is deferred and rescheduled after this interval.

``timeout``
    Maximum time in seconds the sensor is allowed to run before failing.
    This timeout is measured from the first execution attempt, not per poke.

``mode``
    Determines how the sensor occupies worker resources.

    * ``poke`` (default): occupies a worker slot for the entire duration
    * ``reschedule``: releases the worker slot between checks

``soft_fail``
    If set to ``True``, the sensor will be marked as ``SKIPPED`` instead of
    ``FAILED`` when the timeout is reached.

``exponential_backoff``
    If enabled, the time between checks increases exponentially up to
    ``max_wait``. This is useful when polling external systems with
    unpredictable availability.

``max_wait``
    Upper bound (in seconds) for the delay between checks when
    ``exponential_backoff`` is enabled.

For the authoritative API reference, see the Task SDK documentation for
``BaseSensorOperator``:

https://airflow.apache.org/docs/task-sdk/stable/api.html#airflow.sdk.BaseSensorOperator

Example
^^^^^^^

.. code-block:: python

    BashSensor(
        task_id="wait_for_file",
        bash_command="test -f /data/input.csv",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )
