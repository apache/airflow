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
    On a live triggerer and not failed since it last recovered. For an asset watcher this does not mean the
    source it watches is healthy, only that the trigger is not raising; ``last_event_at`` says when it last fired.

``failing``
    On a live triggerer, and it raised or ended without yielding an event since it last recovered.
    ``consecutive_failures``, ``first_failed_at`` and ``last_error`` say how often, since when and why. A trigger
    that is still wanted, such as an asset watcher, is re-created within seconds and stays ``failing`` until it
    recovers: it yields an event, or stays up for two minutes.

``orphaned``
    Its triggerer stopped sending heartbeats. Another triggerer takes it over within
    ``[triggerer] triggerer_health_check_threshold`` seconds.

``triggerer_hostname`` is the host of the triggerer the trigger is assigned to; each live triggerer's heartbeat is
on the health endpoint. The failure count is kept on the trigger, so it carries over when the triggerer restarts
or another triggerer takes the trigger over. ``last_error`` is kept after a recovery.

A trigger is listed only while its owner still refers to it. Once a deferred task resumes or fails, a callback
finishes, or a watcher is removed, the trigger is deleted on the triggerer's next cleanup, and its outcome stays
with its owner: the task instance's state and logs, the callback's state and logs.

During a rolling upgrade from a version that does not record trigger owners, a deferred task's or callback's
trigger created by the older components is listed only once a triggerer of this version picks it up and records its
owner, and a trigger still run by an older triggerer is never ``failing``, since older triggerers do not record
failures.

Owner
-----

Each trigger names what it runs for: ``task_instance`` (Dag, run, task, map index, try and state), ``callback``
(its id, Dag, run and state), or ``asset_watchers`` (each asset and watcher name; several assets that declare the
same trigger share one). ``kind`` is worked out from the owner, and ``dag_id`` is the Dag of the task instance, or
of the Dag run whose deadline the callback belongs to. In the other direction, a task instance's ``trigger`` and an
asset's ``watchers`` (with ``status`` and ``trigger_id``) link to the trigger.

Filtering
---------

``kind`` (``task``, ``callback`` or ``asset_watcher``), ``status``, ``triggerer_hostname``, ``dag_id``,
``asset_id`` and ``teams`` can be combined; ``kind``, ``status`` and ``teams`` take several values. For example,
to find the triggers that need attention:

.. code-block:: bash

    curl -H "Authorization: Bearer $TOKEN" \
      "$AIRFLOW_URL/api/v2/triggers?status=failing&status=orphaned"

and an asset's watchers, with their status, come with the asset itself:

.. code-block:: bash

    curl -H "Authorization: Bearer $TOKEN" "$AIRFLOW_URL/api/v2/assets/12"

``order_by`` takes ``id`` (the default), ``created_date``, ``consecutive_failures``, ``last_failed_at`` and
``last_event_at``, ``-`` first for descending. A trigger that never failed or never fired sorts first ascending and
last descending, on every database.

``GET /api/v2/triggers/{trigger_id}`` returns one trigger.

Who can see a trigger
---------------------

Each trigger is gated like the route that shows its owner:

* A deferred task's trigger needs read access to the Dag's task instances.
* A callback's trigger needs read access to the Dag's runs.
* An asset watcher's trigger needs read access to one of the assets it watches.
  A trigger shared by several assets lists only the watchers on assets the caller can read.

A caller who can read none of these gets a 403. There is no trigger permission of its own.

With ``[core] multi_team`` on, team access comes with the Dag. Assets have no team, so an
asset's watchers are listed for whoever can read the asset.

``last_error`` holds only the exception type and message, with known secrets masked and cut
to 1,000 characters. The full traceback stays in the logs.

Identity
--------

A trigger is the unit: its id changes when its arguments change, when a task defers again, or when all the Dags
scheduled on an asset are paused and unpaused, and a new trigger starts with no failures recorded. An asset
watcher is identified by its asset and its name.

Metrics and logs
----------------

``triggers.failed`` and ``triggers.succeeded`` are tagged with ``trigger_kind``, and the ``triggers.failing``
gauge counts, per triggerer and ``trigger_kind``, the triggers that are failing. When a failing trigger
recovers, the triggerer logs ``Trigger recovered`` with its ``trigger_id`` and ``after_failures``.

Logs
----

``GET /api/v2/triggers/{trigger_id}/logs?tail_lines=200`` returns a trigger's latest lines, in JSON or, with
``Accept: application/x-ndjson``, as NDJSON, in the same shape as task logs:

* for a deferred task's trigger, the trigger part of that task try's log, ``<try log>.trigger.<triggerer job
  id>.log``;
* for a callback's trigger, the callback's log;
* for an asset watcher, its own files from every triggerer that ran it (see
  :ref:`asset-watcher-logs`), which also get the triggerer's ``Trigger recovered`` line for it.

Each is read live from the trigger's triggerer through its log server, as live task logs are, from remote
logging, and from the API server's own log folder, and merged in time order. Nothing is stored twice: the owner's
log stays the place to read a task's or callback's whole story.

Reading a trigger's logs needs the trigger to be visible, by the rules above for its kind, and what reading its
owner's logs needs: task log access to the Dag of its task instance or callback. An asset watcher belongs to no single Dag, so its logs need read access to one of its assets
and task log access to a Dag scheduled on it (directly or on ``Asset.ref(name=...)`` / ``Asset.ref(uri=...)``
of it), in the trigger's team. A trigger you can see but whose logs you can't read returns 403.

Limits:

* On handlers that upload files (S3, GCS, Azure Blob Storage), each rotated part of a watcher's file is uploaded
  once the file reaches ``[triggerer] watcher_log_max_bytes``, but the current part is uploaded only when the
  watcher leaves its triggerer or the triggerer stops. Until then it is read live.
* After a triggerer is killed, the lines it had not uploaded yet stay on its disk until a triggerer starts on the
  same disk, which uploads them. On a disk that does not outlive the triggerer they are lost, apart from the
  standard output copy.
* Lines are kept per trigger id, so a new trigger (see `Identity`_) starts a new history.
* A watcher's files are read from its current triggerer first, then from the triggerers that heartbeat last,
  and by rotated file, newest first. Reading stops once ``tail_lines`` lines are found and the next triggerer
  stopped before the oldest of them, or once 64 MiB were read; the sources then say that older logs were not
  read. The triggerers are the ones that heartbeat since the trigger was created, at most the 50 that heartbeat
  last: files of triggerer jobs that ``airflow db clean`` removed are read only if none of its triggerers is
  left. Once a remote read fails, the older triggerers' files are read only live and from the local disk.
* To read watcher files back from remote storage, a remote log handler has to treat a path ending in ``/`` as a
  prefix. S3 and GCS do; Elasticsearch, OpenSearch and Cloud Logging key logs by task instance and don't return
  watcher logs.
