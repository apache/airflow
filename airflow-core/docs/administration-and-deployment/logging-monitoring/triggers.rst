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

Monitoring triggers
===================

The triggerer runs three kinds of trigger: the trigger of a :doc:`deferred task </authoring-and-scheduling/deferring>`,
a triggerer callback (for example a deadline alert's ``AsyncCallback``), and an
:doc:`asset watcher </authoring-and-scheduling/event-scheduling>`. ``GET /api/v2/triggers`` lists all of them the same
way: what each one runs for, which triggerer runs it, and how it is doing. Trigger arguments are never returned.

Status
------

``pending``
    Not picked up by a triggerer yet.

``running``
    On a live triggerer. For an asset watcher this does not mean the source it watches is healthy, only that
    its triggerer is running it.

``orphaned``
    Its triggerer stopped sending heartbeats. Another triggerer takes it over within
    ``[triggerer] triggerer_health_check_threshold`` seconds.

``triggerer_hostname`` is the host of the triggerer the trigger is assigned to; each live triggerer's heartbeat is
on the health endpoint.

A trigger is listed only while its owner still refers to it. Once a deferred task resumes or fails, a callback
finishes, or a watcher is removed, the trigger is deleted on the triggerer's next cleanup, and its outcome stays
with its owner: the task instance's state and logs, the callback's state and logs.

Owner
-----

Each trigger names what it runs for: ``task_instance`` (Dag, run, task, map index, try and state), ``callback``
(its id, Dag, run and state), or ``asset_watchers`` (each asset and watcher name; several assets that declare the
same trigger share one). ``kind`` is worked out from the owner, and ``dag_id`` is the Dag of the task instance or
of the Dag run whose deadline the callback belongs to. In the other direction, a task instance's ``trigger``, an
asset's ``watchers`` (with ``status`` and ``trigger_id``) and a running deadline callback's ``callback_trigger_id``
link to the trigger.

Filtering
---------

``kind`` (``task``, ``callback`` or ``asset_watcher``), ``status``, ``triggerer_hostname``, ``dag_id`` and
``asset_id`` can be combined; ``kind`` and ``status`` take several values. For example, to find the triggers whose
triggerer went away or that no triggerer picked up yet:

.. code-block:: bash

    curl -H "Authorization: Bearer $TOKEN" \
      "$AIRFLOW_URL/api/v2/triggers?status=orphaned&status=pending"

and an asset's watchers, with their status, come with the asset itself:

.. code-block:: bash

    curl -H "Authorization: Bearer $TOKEN" "$AIRFLOW_URL/api/v2/assets/12"

``order_by`` takes ``id`` (the default) and ``created_date``, ``-`` first for descending.

``GET /api/v2/triggers/{trigger_id}`` returns one trigger.

Who can see a trigger
---------------------

A trigger is listed for whoever can see its owner: read access to the Dag of its task instance or callback, or
read access to an asset one of its watchers watches. There is no permission of its own, so the generated
permission tables show the routes as needing none. With ``[core] multi_team`` on, only triggers that are global
or in one of the caller's teams are listed. A trigger shared by several assets lists only the watchers on assets
the caller can read, and an asset's watcher has a ``status`` only if the caller can see its trigger (otherwise
it is null).

Identity
--------

A trigger is the unit: its id changes when its arguments change, when a task defers again, or when all the Dags
scheduled on an asset are paused and unpaused. An asset watcher is identified by its asset and its name.
