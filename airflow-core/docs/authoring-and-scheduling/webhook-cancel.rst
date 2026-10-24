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

.. _webhook-cancel:

Cancelling Runs from Inbound Webhooks
=====================================

|experimental|

.. versionadded:: 3.6.0

.. warning::

    Cancelling runs from inbound webhooks is experimental. ``WebhookMatch``, ``cancel_on``,
    ``cancel_state``, which runs a delivery cancels and what a cancel does to them may change in a minor
    release, so check the release notes when you upgrade.

A Dag or a task can declare which deliveries to Airflow's inbound webhooks cancel its runs: a Dag that
deploys a preview of a pull request, for example, can have its runs stopped when the pull request is
closed. It declares ``cancel_on``, a list of :class:`~airflow.sdk.WebhookMatch` conditions, each naming a
webhook, the events and the attributes of the deliveries that cancel. When a delivery that has a
*correlation key*, a value that says what the delivery is about, such as a pull request, matches a
condition, Airflow records a *cancel request* for the Dag before it answers the sender. The scheduler then
applies the request, within a second or two, to the Dag's queued and running runs that are about the same
correlation key, through an event that an earlier delivery recorded, including runs the scheduler creates
later from such an event, or through their conf if they were queued before the delivery:

* a Dag's ``cancel_on`` fails those runs, as marking them failed does;
* a task's ``cancel_on`` ends that task in those runs, in the task's ``cancel_state``, ``failed`` by
  default or ``skipped``, and the runs go on.

A run is about a correlation key when a delivery with that key started it, through a
:doc:`webhook topic <webhook-topics>` or an asset route, or when it was triggered with the key in its
``conf``, under the conf key that the condition names.

The Deployment Manager sets up the webhook itself, as described in
:doc:`/administration-and-deployment/webhooks`. Cancelling needs Airflow 3.6 or later, in the Airflow core
of the API servers, the scheduler and the Dag processor, and in the Task SDK of the Dag processor and the
workers, which parse the Dag files: an older Task SDK does not know ``cancel_on``, and fails to import a
Dag that uses it.

Example: a preview environment
------------------------------

This Dag deploys a preview of a pull request in ``my-org/web`` each time the label ``deploy-preview`` is
added to it, and serves it while the end-to-end tests run against it. It needs the ``github`` webhook,
connected to the repository with "Pull requests" events as described in the GitHub provider's
:ref:`Inbound Webhooks <apache-airflow-providers-github:howto/webhooks:github>` page.

.. code-block:: python

    from airflow.sdk import Param, WebhookMatch, WebhookTopic, dag, task

    REPOSITORY = "my-org/web"

    preview_requested = WebhookTopic(
        webhook="github", path=REPOSITORY, event="pull_request", action="labeled", detail="deploy-preview"
    )
    pr_closed = WebhookMatch(
        webhook="github",
        events=["pull_request"],
        match={"repository": REPOSITORY, "action": "closed"},
        correlation_key_conf="pr",
    )
    new_commit = WebhookMatch(
        webhook="github",
        events=["pull_request"],
        match={"repository": REPOSITORY, "action": "synchronize"},
    )


    def tear_down(context):
        if context["reason"] == "cancelled":
            print("The pull request was closed: tearing down the preview of", context["run_id"])


    @dag(
        schedule=preview_requested,
        catchup=False,
        cancel_on=[pr_closed],
        on_failure_callback=tear_down,
        params={"pr": Param(None, type=["null", "string"])},
    )
    def deploy_preview():
        @task
        def deploy(*, triggering_asset_events, params) -> str:
            events = triggering_asset_events.get(preview_requested, [])
            pull_request = events[-1].extra["correlation_key"] if events else params["pr"]
            print("Deploying a preview of", pull_request)
            return pull_request

        @task
        def serve_preview(pull_request: str): ...

        @task(cancel_on=[new_commit], cancel_state="skipped")
        def run_e2e_tests(pull_request: str): ...

        pull_request = deploy()
        serve_preview(pull_request)
        run_e2e_tests(pull_request)


    deploy_preview()

The GitHub source type gives every delivery about a pull request or an issue the correlation key
``<repository>#<number>``, whatever its action, and each topic event keeps the key of its delivery in its
``extra``. So with these runs of ``deploy_preview``:

.. list-table::
   :header-rows: 1
   :widths: 30 25 45

   * - Run
     - Its correlation key
     - What cancels it
   * - A, started by the label on pull request 42
     - ``my-org/web#42``, from the topic event that started it
     - Closing pull request 42 fails run A. A new commit pushed to pull request 42 skips its
       ``run_e2e_tests``, and run A goes on.
   * - B, started by the label on pull request 57
     - ``my-org/web#57``
     - Only the deliveries about pull request 57.
   * - C, triggered with the conf ``{"pr": "my-org/web#42"}``
     - ``my-org/web#42``, through ``pr_closed.correlation_key_conf``
     - Closing pull request 42. ``new_commit`` names no conf key, so it never applies to run C.
   * - D, triggered without ``pr``
     - None
     - Nothing.

Closing pull request 42 runs ``tear_down`` for run A and run C, with ``context["reason"]`` set to
``cancelled``. A run started by a label added to pull request 42 after it was closed, or triggered with its
key after it was closed, is not cancelled: a delivery cancels only the runs whose event, or whose queueing
with the key in their ``conf``, came before it.

Conditions
----------

``WebhookMatch(webhook, events=None, match=None, correlation_key_conf=None)`` describes the deliveries to
one webhook that cancel:

* ``webhook`` is the webhook's name, as ``[webhooks] names`` lists it, such as ``github``.
* ``events`` and ``match`` follow the rules of a webhook wait, described in
  :ref:`webhook-waits-match`: ``events`` is None, for any event, or a non-empty list of the events that
  match; ``match`` maps the names of the attributes that the webhook's source type declares to a value, or
  a list of values of which one must match, and every name in it must match. Values are strings or
  integers and compare as strings, and an empty or missing ``match`` accepts every delivery that
  ``events`` accepts. Unlike those of ``WaitForWebhookOperator``, these values are not templates: they are
  fixed when the Dag file is parsed.
* ``correlation_key_conf`` names a top-level key of a run's ``conf`` whose value is the run's correlation
  key, so that a run triggered with a ``conf``, such as a manual run, can be cancelled too. The value is
  read as a string from a string or an integer; it is not a dotted path, and no template is rendered. With
  None, the default, the condition cancels only runs that a delivery started.

A Dag declares conditions with ``cancel_on`` in ``DAG(...)`` or ``@dag(...)``, which fails its runs, and a
task with ``cancel_on`` among its arguments, which ends that task, together with ``cancel_state``. A task
takes them as any operator argument: in ``@task(...)``, ``.override()``, ``.partial()`` for a mapped task,
whose every map index they then apply to, and the ``default_args`` of the Dag or of a task group. A
``cancel_on`` in ``default_args`` gives each task the conditions, so a matching delivery ends every
unfinished task of the run in its ``cancel_state`` rather than failing the run as the Dag's ``cancel_on``
does. ``cancel_on`` takes one condition or a list of them, and conditions that are equal, such as two that
differ only in the order of their values, count once.

These are refused when the Dag file is parsed, so that the Dag fails to import rather than never
cancelling:

* a condition that breaks the rules above, which raises ``ValueError``: such as an empty ``events`` list,
  a value that is neither a string nor an integer, a webhook name that is not 1 to 64 lowercase letters,
  digits, ``-`` and ``_`` starting with a letter or a digit, or a ``correlation_key_conf`` that is blank or
  longer than 250 characters;
* a ``cancel_on`` that is not a ``WebhookMatch`` or a list of them, which raises ``TypeError``;
* ``cancel_on`` on a teardown task, which runs however its run ends. If the teardown gets it from
  ``default_args``, pass ``cancel_on=None`` to the teardown;
* ``cancel_state`` without ``cancel_on``, unless it comes from ``default_args``, and a ``cancel_state``
  other than ``"failed"`` or ``"skipped"``;
* ``cancel_on`` or ``cancel_state`` in ``.expand()``: pass them to ``.partial()``.

Airflow does not check that ``[webhooks] names`` lists the webhook, or that its source type declares the
names in ``match``, so a misspelled name makes a condition that no delivery matches.
``airflow webhooks check`` lists the conditions on each webhook and reports a webhook that is not listed
(see :ref:`inbound-webhooks-cli`).

Which runs a delivery cancels
-----------------------------

Only a delivery that has a correlation key requests a cancel, and only its source type sets one:

* The GitHub source type gives every delivery about a pull request or an issue the key
  ``<repository>#<number>``, such as ``my-org/web#42``. A ``push``, a ``workflow_run`` or an organization
  event has none.
* The generic ``hmac_json`` and ``token_header`` source types read it from the field that the webhook
  connection's ``correlation_key_field`` names, and a webhook whose connection does not set it has none.

When such a delivery matches conditions of a Dag, Airflow records one cancel request for the Dag, in the
transaction that records the delivery. Its *targets* are the whole run if a condition of the Dag itself
matched, and each task whose condition matched, with that task's ``cancel_state``. The scheduler applies
the request to the Dag's runs that are queued or running and about the delivery's correlation key:

* **Runs that a delivery started.** The run consumed an asset event, a topic event or an asset route
  event, that a delivery to the same webhook with the same correlation key recorded before the cancelling
  delivery was received. Only events that a delivery recorded count: an event that a task emitted, or that
  the REST API or an asset watcher's trigger created, never makes a run about a key, whatever its
  ``extra`` says.
* **Runs triggered with a conf.** The run was queued before the delivery was received, and its ``conf``
  holds the correlation key, as a string or an integer, under the ``correlation_key_conf`` of a matched
  condition. Clearing a run queues it again, so a run cleared after the delivery was received is no
  longer about it.

For each run, the whole-run target applies if a matched condition of the Dag itself makes the run about
the key: any of them for a run that a delivery started, and for a run triggered with a conf, one whose
``correlation_key_conf`` names the conf key that holds the key. Otherwise the task targets whose matched
conditions make the run about the key apply, in the same way.
In the example, ``new_commit`` names no conf key, so it ends ``run_e2e_tests`` in runs that a delivery
started, never in runs triggered with ``pr``.

A delivery never cancels:

* a run that has finished, nor a task instance that has;
* a run that its own events start, or any run started from an event newer than the delivery;
* a run triggered after the delivery was received, or a run that only its ``conf`` makes about the key and
  that was cleared after the delivery was received.

A run that a delivery started is cancelled if it is queued or running when the scheduler applies the request,
even if it was cleared after the cancelling delivery was received.

A Dag that is paused, draining or stale keeps its conditions, and its runs can still be cancelled, since a
cancel only stops work. Applying a request changes each run at most once, however many ways the run is about
its key.

What a cancel does
------------------

A request changes the unfinished task instances of each run it applies to, by their state:

.. list-table::
   :header-rows: 1
   :widths: 25 40 35

   * - Task instance state
     - The Dag's ``cancel_on``
     - A task's ``cancel_on``
   * - ``running``
     - ``failed``. The worker's next heartbeat, within
       :ref:`[workers] min_heartbeat_interval <config:workers__min_heartbeat_interval>` seconds, finds the
       change and stops the task: its ``on_kill`` runs, and the task is killed
       :ref:`[core] killed_task_cleanup_time <config:core__killed_task_cleanup_time>` seconds later if it
       is still running.
     - ``cancel_state``, and the task is stopped the same way.
   * - ``deferred``
     - ``failed``. The triggerer stops its trigger, as when a deferred task is marked failed.
     - ``cancel_state``, the same way.
   * - ``awaiting_input``
     - ``failed``. Its open :doc:`webhook wait <webhook-waits>` ends, with the status ``ended``, and a
       Human-in-the-loop request it waits for is left unanswered.
     - ``cancel_state``, the same way.
   * - ``up_for_reschedule``
     - ``failed``.
     - ``cancel_state``.
   * - No state yet, ``scheduled``, ``queued``, ``up_for_retry`` or ``restarting``
     - ``skipped``, so it never runs, and a retry does not happen.
     - ``cancel_state``, so it never runs.
   * - Finished
     - Left as it is.
     - Left as it is.
   * - A teardown task
     - Left to run: the run ends when it finishes.
     - Cannot have ``cancel_on``.

The Dag's ``cancel_on`` then fails the run, unless a teardown still has to run. A task's ``cancel_on``
leaves the run running, and the tasks downstream of the task follow their trigger rules: with the default
``all_success``, they become ``upstream_failed`` downstream of a failed task, and ``skipped`` downstream of
a skipped one. The run ends as any run does, once its tasks have finished.

A cancel is a failure:

* **No cancelled state.** A run cancelled by the Dag's ``cancel_on`` is ``failed``, and shows red in the
  UI. Airflow has no state for a cancelled run or task.
* **Callbacks.** The Dag's ``on_failure_callback`` runs, in the Dag processor, with ``context["reason"]``
  set to ``cancelled``, as it is set to ``timed_out`` for a run that timed out, so the callback can tell a
  cancel from a failure. Its context describes the latest started of the task instances that the cancel
  stopped, or, if it stopped none, has only ``dag``, ``run_id`` and ``reason``. A task's ``cancel_on``
  sends no Dag callback of its own. Listeners get ``on_dag_run_failed`` with the message ``cancelled`` for
  a run that the cancel failed, and ``on_task_instance_failed`` or ``on_task_instance_skipped`` for each
  task instance that it stopped.
* **Auto-pause.** A cancelled run is ``failed``, and counts toward ``max_consecutive_failed_dag_runs`` when
  a later run fails. A Dag whose runs are often cancelled can then be paused by a few failures, so give it
  a ``max_consecutive_failed_dag_runs`` that allows for its cancels, or none.
* **Teardowns.** A run held by a teardown ends when the teardown finishes, in the state its leaf tasks give it
  as for any run: ``failed`` with the reason ``task_failure`` if a leaf task failed, as when the killed
  running task is a leaf, or ``success``, with ``on_success_callback``, if every remaining leaf was skipped,
  as when the cancel skipped the tasks downstream of the killed task. It gets no ``cancelled`` callback. It
  gets the note and the audit log entry described in `Following a cancel`_ only if the cancel ended some of
  its tasks: a run whose only unfinished tasks are teardowns is left as it is, with no note, no audit log
  entry and no entry in the request's result.
* **Active runs.** A run that the Dag's ``cancel_on`` fails no longer counts toward the Dag's
  ``max_active_runs``, so the Dag's next run can start.
* **Same-delivery waits.** A delivery that resumes a webhook wait and also requests the cancel of its run
  resumes the wait at once. The scheduler then cancels the run, ending the resumed task as ``failed`` if it
  is running, or ``skipped`` if it has not started yet; a task's ``cancel_on`` ends it in its
  ``cancel_state`` instead.

Runs created later
------------------

A request can be applied before the run that it is about exists. The scheduler holds an asset-triggered
run back while its Dag has ``max_active_runs`` runs active, and creates the run of a Dag scheduled with
``AssetAndTimeSchedule`` only when its time has come, which is usually well after the event. So when the
scheduler creates an asset-triggered run, partitioned or not, or a run of an ``AssetAndTimeSchedule``, of a
Dag that declares any ``cancel_on``, it applies to it at once the requests about the correlation keys of
the delivery events it consumed that were received after those events, ``pending`` or ``applied``, but
not ``failed`` ones. That is why a request is kept after it is applied, until ``airflow db clean`` removes
it with its delivery's record. The run is then failed, or its tasks are given their ``cancel_state``,
before any of its tasks runs, and, unless another scheduler is applying the request at that moment, the
run is added to the request's result. If applying a request to a new run fails, the scheduler logs the
error and leaves the run as it was created.

This has some consequences:

* A pull request closed and then reopened while its label's event waited still cancels the run that the
  event starts: deliveries arrive in any order, and the close was received after the label.
* A run that consumed the events of several deliveries, as with
  :ref:`config:scheduler__batch_asset_events` or a schedule on several assets, is about each of their keys,
  and a request about any of them cancels it. ``cancel_on`` assumes one run per event, the default.
* With more than one scheduler, a request committed after a creating scheduler read the requests, and applied
  by another scheduler before the new run commits, is not applied to that run. With one scheduler, a request
  that commits while the scheduler creates a run about its key is applied to the run the next time the
  scheduler looks for pending requests. Whatever the number of schedulers, a request applied while the API
  transaction of a run triggered with the key in its ``conf`` is still open is never applied to that run.

Latency
-------

A delivery's cancel requests are committed before it is answered. The scheduler looks for pending
requests about once a second, and applies up to
:ref:`[webhooks] cancel_requests_per_loop <config:webhooks>` of them, oldest first, each in a transaction
of its own, so a request is applied within a second or two of the delivery's answer, then the task is
killed at its next heartbeat. It takes longer when many requests wait, or when the scheduler is busy.
Several schedulers share the pending requests, and never apply one twice.

While no scheduler runs, nothing is applied: requests stay ``pending``, and the tasks they are about keep
running. A scheduler that starts again applies them to the runs that are still queued or running. A
request whose application raises an error is marked ``failed``, and is not applied again: the scheduler
logs the error, and the request's result names its class. A database error that may pass, such as a lost
connection, leaves the request ``pending`` instead, for the scheduler's next look.

Dag versions
------------

The conditions come from the latest version of the Dag that the Dag processor stored when the delivery
arrives, and the request's targets are fixed then: a change to ``cancel_on`` makes a new Dag version, and
applies to the deliveries received once the Dag processor has stored it. A request already recorded is
applied as it was recorded, even if the Dag no longer declares the condition.

Each run's own Dag version, or the latest one for a bundle without versions, decides which tasks the run has,
which are teardowns, and whether the Dag's ``on_failure_callback`` runs for the cancel, as for any failure of
the run: a targeted task missing from it is left alone. The task instance page and the Dag details page show
the latest conditions, not those of the run's version.

Teams
-----

Webhooks have no team in this version of Airflow. When :ref:`config:core__multi_team` is on, a delivery
records no cancel request for a Dag that has a team, and the scheduler cancels no run of such a Dag: a
request recorded before its Dag got a team lists, with the reason ``dag_has_team``, the runs about its key
that the scheduler finds when it applies the request; a run that the scheduler creates later, from an
earlier event, is left alone without an entry. The Dag gets no warning, but ``airflow webhooks check``
reports each Dag with a team whose ``cancel_on`` names a webhook, as a problem with that webhook.

Following a cancel
------------------

* **The delivery.** Under **Browse → Webhook Deliveries**, a delivery that recorded cancel requests has the
  outcome ``cancel_requested``, unless it also recorded topic or asset route events, which makes it
  ``accepted``, or went to a webhook wait, which makes it ``resumed_wait``, or becomes ``expired``, keeping
  its cancel requests, if newer deliveries kept for webhook waits pushed it out (see
  :ref:`inbound-webhooks-outcomes`). Its details list the requests under **Cancel Requests**: for each, the
  Dag; the scope, **Whole run** or **Tasks**, and the tasks it ends, each with the state it ends them in; the
  status, **Pending**, **Applied** or **Failed**, with what it means, and when the scheduler applied it or
  failed to; the runs and the task instances it cancelled, each linked to its page; the runs about its key
  that it left alone, with why; and the error of a failed request. A request whose record ``airflow db clean``
  removed, or that went with its Dag, is listed as gone. The REST API returns the same, as described in
  :ref:`inbound-webhooks-deliveries`.
* **The Dag and its tasks.** The Dag's **Details** tab lists its conditions in its **Cancel On** row, and
  the **Details** tab of a task instance lists those of its task, with the state that they end it in.
  Both show the Dag's latest version.
* **The run.** A run that the Dag's ``cancel_on`` cancelled gets the note
  ``Cancelled by webhook <name> delivery <id>``, where ``<id>`` is the sender's id for the delivery, or the
  id of Airflow's record of it if the sender gave none. The note is written only if the run has none.
* **The audit log.** The scheduler writes an entry with the event ``webhook_cancel`` and the owner
  ``webhook:<name>`` for each run that a request cancels, and for each task instance that a task's
  ``cancel_on`` ends, which the users allowed to read the Dag's audit log see (see
  :ref:`inbound-webhooks-audit-log`).
* **Metrics.** The scheduler counts the runs and task instances it cancels in ``webhooks.cancels``, tagged
  ``webhook`` and ``scope`` (see :ref:`inbound-webhooks-metrics`).
* **The command line.** ``airflow webhooks check`` lists which Dags and tasks each webhook cancels, as in
  ``Webhook github cancels the runs of Dag deploy_preview on pull_request deliveries with action closed,
  repository my-org/web, and manual runs by conf key pr``.

When a run was not cancelled, :ref:`inbound-webhooks-not-cancelled` lists the reasons.

Testing a Dag that cancels
--------------------------

Only a running scheduler applies cancel requests, so ``airflow dags test`` does not cancel its run. To try
``deploy_preview`` with a scheduler, trigger it as run C above:

.. code-block:: bash

    airflow dags trigger deploy_preview --conf '{"pr": "my-org/web#42"}'

and, while the run is still running, send a closed pull request to the webhooks process, here on port
8081, with ``airflow webhooks send-test``, described in :ref:`inbound-webhooks-cli`:

.. code-block:: bash

    airflow webhooks send-test github \
        --url http://localhost:8081/webhooks/github \
        --signature-header X-Hub-Signature-256 \
        --signature-prefix sha256= \
        --header 'X-GitHub-Event: pull_request' \
        --header 'X-GitHub-Delivery: test-close-42' \
        --data '{"action": "closed", "number": 42, "repository": {"full_name": "my-org/web"},
                 "pull_request": {"number": 42, "merged": false}}'

It prints ``{"status": 202, "body": {"delivery_id": "test-close-42", "outcome": "cancel_requested"}}``,
unless a topic or a webhook wait also took the delivery, and the run is ``failed`` a second or two later,
with the note ``Cancelled by webhook github delivery test-close-42``. A test delivery is processed like any
other, so it cancels every run about ``my-org/web#42`` of every Dag whose ``cancel_on`` it matches.
