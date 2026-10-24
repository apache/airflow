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

.. _webhook-waits:

Waiting for Inbound Webhooks
============================

|experimental|

.. versionadded:: 3.5.0

.. warning::

    Webhook waits are experimental. ``BaseOperator.wait_for_webhook``, the match grammar and the events
    that a waiting task resumes with may change in a minor release, so check the release notes when you
    upgrade.

A task can wait in its Dag run until a service, such as GitHub, sends one of Airflow's inbound webhooks a
delivery that matches what the task waits for: a task that opened a pull request, for example, can wait
for it to be closed. The task registers a *webhook wait* with the API server, which names the webhook,
the events that the wait accepts and the attributes that the delivery must have, and parks in the
``awaiting_input`` state. The first delivery that matches the wait resumes the task with that delivery,
and if none matches before the wait's timeout, the task resumes with a timeout event instead.

A wait resumes one task of a Dag run that is already running. To start a Dag run for each delivery
instead, schedule the Dag on a webhook topic, as described in :doc:`webhook-topics`. The Deployment
Manager sets up the webhook itself, as described in :doc:`/administration-and-deployment/webhooks`.

Waits need Airflow 3.5 or later, both on the API servers and in the Task SDK of the workers. The API
servers that serve the Execution API check each wait against its webhook, so they need the webhook listed
in their ``[webhooks] names`` too, access to its connection, and the provider that ships its source type,
such as ``apache-airflow-providers-github`` for the ``github`` source type. If any of these is missing,
they refuse every wait on the webhook with ``webhook_not_found``. Webhooks have no team in Airflow 3.5,
so when :ref:`config:core__multi_team` is on, only the tasks of Dags without a team can wait.

Ways to wait
------------

* :class:`~airflow.providers.standard.operators.webhooks.WaitForWebhookOperator`, in the standard provider,
  takes the webhook, the events and the match as arguments, and returns the delivery as its XCom.
* ``@task.webhook_wait``, in the standard provider, turns a function that returns the match into a task
  that waits as the operator does.
* ``BaseOperator.wait_for_webhook``, in the Task SDK, registers a wait and parks the task from any
  operator's code, as described in `Writing an operator that waits`_.

The standard provider's page
:ref:`Waiting for a Webhook Delivery <apache-airflow-providers-standard:howto/operator:WaitForWebhookOperator>`
describes the operator and
:ref:`the decorator <apache-airflow-providers-standard:howto/decorator:webhook_wait>` in full.

Example: a merge gate
---------------------

This Dag is triggered with the number of a pull request in ``my-org/configs``. It waits up to three days for
the pull request to be closed, and applies the change only if the pull request was merged. It needs the
``github`` webhook, connected to the repository with "Pull requests" events as described in the GitHub
provider's :ref:`Inbound Webhooks <apache-airflow-providers-github:howto/webhooks:github>` page.

.. tab-set::

    .. tab-item:: @task.webhook_wait
        :sync: taskflow

        .. code-block:: python

            from datetime import timedelta

            from airflow.sdk import Param, dag, task

            REPOSITORY = "my-org/configs"


            @dag(schedule=None, params={"pr_number": Param(type="integer")})
            def merge_gate():
                @task.webhook_wait(webhook="github", events=["pull_request"], timeout=timedelta(days=3))
                def wait_for_merge(number: str) -> dict:
                    return {"repository": REPOSITORY, "number": number, "action": "closed"}

                @task
                def apply(delivery: dict) -> str:
                    pull_request = delivery["payload"]["pull_request"]
                    if not pull_request["merged"]:
                        raise RuntimeError(f"Pull request {pull_request['number']} was not merged")
                    return pull_request["merge_commit_sha"]

                apply(wait_for_merge("{{ params.pr_number }}"))


            merge_gate()

    .. tab-item:: WaitForWebhookOperator
        :sync: operator

        .. code-block:: python

            from datetime import timedelta

            from airflow.providers.standard.operators.webhooks import WaitForWebhookOperator
            from airflow.sdk import Param, dag, task

            REPOSITORY = "my-org/configs"


            @dag(schedule=None, params={"pr_number": Param(type="integer")})
            def merge_gate():
                wait_for_merge = WaitForWebhookOperator(
                    task_id="wait_for_merge",
                    webhook="github",
                    events=["pull_request"],
                    match={"repository": REPOSITORY, "number": "{{ params.pr_number }}", "action": "closed"},
                    timeout=timedelta(days=3),
                )

                @task
                def apply(delivery: dict) -> str:
                    pull_request = delivery["payload"]["pull_request"]
                    if not pull_request["merged"]:
                        raise RuntimeError(f"Pull request {pull_request['number']} was not merged")
                    return pull_request["merge_commit_sha"]

                apply(wait_for_merge.output)


            merge_gate()

``{{ params.pr_number }}`` is rendered when ``wait_for_merge`` runs, as a string, and still matches
deliveries whose ``number`` attribute is an integer, because a wait compares values as strings. GitHub
sends ``closed`` both when a pull request is merged and when it is closed without merging, so ``apply``
reads ``merged`` from the payload, which the webhook's ``store_payload`` must keep: ``full`` and the GitHub
source type's ``summary`` both do. A pull request that is closed shortly before ``wait_for_merge``
registers its wait still resumes it, as described in `Deliveries that arrive before the wait`_. The
standard provider's page has a version of this Dag that opens the pull request itself.

.. _webhook-waits-match:

Matching a delivery
-------------------

A wait accepts a delivery to its webhook when both of these hold:

* ``events`` is None, the default, or lists the delivery's event, such as ``["pull_request"]``. A
  delivery's event is its ``event`` attribute, such as the ``X-GitHub-Event`` header of a GitHub delivery.
* For every name in ``match``, the delivery's attribute of that name equals the value, or one of the values
  if the value is a list. ``{"repository": "my-org/configs", "action": ["closed", "reopened"]}`` accepts a
  delivery about ``my-org/configs`` whose action is ``closed`` or ``reopened``. A delivery that lacks an
  attribute named in ``match`` does not match, and an empty or missing ``match`` accepts every delivery
  that ``events`` accepts.

The attributes are those that the webhook's source type declares, which the record of each delivery shows
under **Browse → Webhook Deliveries**. That page shows a webhook's deliveries only to users who may read its
connection, ``webhook_<name>``, such as the ``Op`` and ``Admin`` roles of the FAB auth manager (see
:ref:`inbound-webhooks-deliveries`). Other authors can find the attributes in the docs of the provider that
ships the source type, or ask their Deployment Manager. Those of the GitHub source type are ``event``,
``action``, ``repository``, ``organization``, ``label``, ``branch`` and ``number``, as the GitHub provider's
:ref:`Inbound Webhooks <apache-airflow-providers-github:howto/webhooks:github>` page describes, and the
generic ``hmac_json`` and ``token_header`` source types of the standard provider have ``event``, ``path``,
and those that the webhook connection's ``attribute_paths`` maps (see
:ref:`apache-airflow-providers-standard:howto/connection:inbound_webhook`). The API server does not check
that the source type declares the names in ``match``, so a misspelled name makes a wait that no delivery
matches until its timeout.

Values are strings or integers, and compare as strings, so ``4242`` and ``"4242"`` match the same
deliveries. A wait that does not follow these rules fails its task when it registers (see
`When a wait is refused`_): an empty ``events`` list, which would accept nothing, or a string instead of a
list; a value other than a string, an integer or a non-empty list of them, such as ``True``, ``1.5`` or
None; and a mapping as a value, which is reserved for match operators.

The webhook's source type verifies that its sender sent a delivery, not who chose each value in it. Many
values in a GitHub delivery, for example, are chosen by other GitHub users, such as label names and the
branches of pull requests from forks; the GitHub provider's page lists them. Anyone who holds the webhook's
shared secret can also send a delivery with any values. So match on values that only the people you trust
can set, such as the repository and number of a pull request that your Dag opened, and check the payload
before acting on it.

One wait for each key
---------------------

A wait's ``events`` and ``match`` are its *key*. Within a webhook, only one running or parked task instance
can have an open wait on a key: the API server refuses a second one with ``wait_key_taken``, and its task
fails. Writing a key differently does not make it a different key: the order of names and of list values,
repeated values, ``42`` against ``"42"`` and ``"a"`` against ``["a"]`` all leave it the same. So give each
waiting task, and each mapped task instance, a match of its own, such as one that names the pull request
it waits for. To act on one delivery in several tasks, wait in one task and pass its XCom downstream, or
schedule a Dag on a webhook topic.

One delivery can match several waits with different keys, and then resumes each of them once.

.. _webhook-waits-resume-event:

What the task resumes with
--------------------------

The first delivery that matches a wait resumes its task with this event, which
``WaitForWebhookOperator`` and ``@task.webhook_wait`` return as their XCom:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Key
     - Value
   * - ``webhook``
     - The webhook's name.
   * - ``event``
     - The delivery's event, or None if it has none.
   * - ``delivery_id``
     - The sender's id for the delivery, or None if it has none.
   * - ``received_at``
     - When Airflow first received the delivery, as an ISO 8601 string.
   * - ``attributes``
     - The delivery's attributes, as its record keeps them.
   * - ``payload``
     - What the webhook's ``store_payload`` keeps of the delivery: the payload as parsed from the body with
       ``full``, the parts that the source type keeps with ``summary``, or None with ``none``.

For example:

.. code-block:: python

    {
        "webhook": "github",
        "event": "pull_request",
        "delivery_id": "72d3162e-cc78-11e3-81ab-4c9367dc0958",
        "received_at": "2026-10-21T09:30:00+00:00",
        "attributes": {
            "event": "pull_request",
            "action": "closed",
            "repository": "my-org/configs",
            "number": 4242,
        },
        "payload": {"action": "closed", "number": 4242, "pull_request": {"merged": True}},
    }

The event is JSON: dates in it are ISO 8601 strings. A mapping key that serde, which stores the event with
the task instance, reserves for its own encoding gets one more leading underscore: ``__classname__``,
``__version__``, ``__data__``, ``__id__``, ``__cache__``, ``__type``, ``__source`` and ``__var``, and those
names with more leading underscores, so that ``__id__`` in a payload arrives as ``___id__`` and keys stay
distinct.

The payload is not left out for its size, as it is from topic events with
``[webhooks] max_event_payload_bytes``: only the webhook's ``max_body_bytes`` bounds it. Everyone who can
read the task's XCom can read it, so a webhook whose payloads carry sensitive data, such as personal data,
should keep ``summary`` or ``none`` (see :doc:`/administration-and-deployment/webhooks`).

If no delivery matches before the wait's deadline, the task resumes with
``{"error": "No matching delivery arrived before <deadline>", "error_type": "timeout"}``, with the deadline
in ISO 8601, and ``WaitForWebhookOperator`` and ``@task.webhook_wait`` raise
:class:`~airflow.providers.standard.exceptions.WebhookWaitTimeoutError`.

Timeouts
--------

``timeout`` is how long a wait lasts from its registration, as a ``timedelta`` of at most 365 days. Without
one, the API server's ``[webhooks] default_wait_timeout`` applies, in seconds; when that is not set either,
the API server refuses the wait with ``timeout_required``.

The API server holds the deadline. The task's ``execution_timeout`` does not run while it is parked, so the
wait's timeout is the only thing that ends a wait that no delivery matches. The scheduler resumes timed-out
tasks every
:ref:`[scheduler] trigger_timeout_check_interval <config:scheduler__trigger_timeout_check_interval>`
seconds, 15 by default, up to 100 task instances at a time, so a task resumes up to one interval after its
deadline, and later when many time out together. A retry registers a new wait, with a new timeout.

.. _webhook-waits-early:

Deliveries that arrive before the wait
--------------------------------------

A delivery can arrive before the task that waits for it has registered its wait, as when a pull request
is closed while the task that waits for it is still queued. Such a delivery is kept for the wait if the
delivery has a correlation key, went to no other wait and recorded no topic or asset route events. An
:ref:`asset route <inbound-webhooks-asset-routes>`, which the Deployment Manager sets in the webhook's
connection, records an event on a data asset for the deliveries that offer its topic name, so that the
Dags scheduled on the asset run.

A kept delivery's outcome is ``early``, or ``cancel_requested`` if it also recorded cancel requests for
Dags whose ``cancel_on`` it matches (see :doc:`webhook-cancel`), and the first wait that matches it and is
registered within :ref:`[webhooks] early_delivery_ttl <config:webhooks>` seconds of its receipt, 600 by
default, takes it: the task resumes as soon as it parks, without waiting. A wait that several kept
deliveries match takes the oldest.

The correlation key says what a delivery is about, and only its source type sets it:

* The GitHub source type gives every delivery about a pull request or an issue the key
  ``<repository>#<number>``, such as ``my-org/configs#4242``.
* The generic ``hmac_json`` and ``token_header`` source types read it from the field that the webhook
  connection's ``correlation_key_field`` names, such as the id of a job run.

A wait does not match on the correlation key: it only decides which deliveries are kept. So:

* A delivery without a correlation key that arrives before the wait is not kept, and no wait gets it.
* A delivery that recorded topic or asset route events for a Dag is not kept either, so a wait registered
  after it never gets it, even one registered while the delivery was being recorded. Do not schedule Dags
  on the deliveries that a wait may have to get early, and have the Deployment Manager keep asset routes
  off them, since a route that records an event for such a delivery has the same effect.
* A delivery that recorded cancel requests is kept all the same. A merge gate on a pull request therefore
  still gets the delivery that closes the pull request when the same delivery also matches the
  ``cancel_on`` of a Dag that deploys a preview of it, and cancels that Dag's runs.
* A webhook keeps at most :ref:`[webhooks] early_delivery_max_per_webhook <config:webhooks>` deliveries
  for waits, ``early`` or ``cancel_requested``, received within ``early_delivery_ttl``, 1000 by default.
  Keeping one more marks the oldest ``expired``, and drops its payload, so that no wait gets it; the
  cancel requests it recorded stay.

The sender can deliver an ``early``, ``cancel_requested`` or ``expired`` delivery again, as from the **Recent
Deliveries** page of a GitHub webhook. The repeat is processed again in full, so it goes to a wait registered
since, even after ``early_delivery_ttl``, and a Dag that subscribed or was unpaused since receives it.

Outcomes
--------

A delivery's outcome, which its record shows, tells what it did, in this order (see
:ref:`inbound-webhooks-outcomes`):

* ``resumed_wait``: it went to at least one wait, whether or not it also recorded topic or asset route
  events.
* ``accepted``: it recorded topic or asset route events and went to no wait. It is never kept for a wait.
* ``cancel_requested``: it has a correlation key, recorded no event, went to no wait, and recorded cancel
  requests for Dags whose ``cancel_on`` it matches (see :doc:`webhook-cancel`), so it is kept for a wait,
  as described above.
* ``early``: it has a correlation key, recorded no event, requested no cancel and went to no wait, so it is
  kept for a wait, as described above.
* ``expired``: it was kept for waits, ``early`` or ``cancel_requested``, until the webhook kept too many
  newer such deliveries. The cancel requests it recorded, if any, stay.
* ``dag_paused`` or ``no_subscribers``: it has no correlation key, recorded no event and went to no wait.

Whatever its outcome, a delivery's record also lists the cancel requests that it recorded.

A repeat of a ``resumed_wait`` or ``accepted`` delivery is answered as a duplicate and does nothing, while a
repeat of any other is processed again in full, without recording a second cancel request for any Dag. A
delivery with a correlation key that only paused Dags subscribe to is therefore ``early`` rather than
``dag_paused``, and its repeat, after the Dags are unpaused, starts them unless a wait has taken the delivery
meanwhile.

Retries, clears and waiting twice
---------------------------------

A wait belongs to one try of its task instance:

* A retry, or a run of a cleared task, runs the task's code again, which registers a new wait. With
  ``@task.webhook_wait``, the function runs again too.
* A try that stops while it waits, because it was cleared or marked, or failed before it parked, leaves its
  wait open: nothing closes it at once. A ``registered`` wait stays open until a matching delivery or a
  registration on the same key ends it. A ``received`` wait, which a delivery matched before the try
  stopped, is not offered other deliveries, so it stays open until a registration on the same key ends it,
  which takes its delivery back as described below, or ``airflow db clean`` removes it once its deadline
  has passed. Meanwhile the task instance's **Details** tab says that no delivery will resume it, and the
  ``webhooks.open_waits`` metric leaves such waits out.
* A matching delivery ends the ``registered`` wait of a try that no longer runs or waits, and is handled as
  if that wait did not exist: if it has a correlation key and recorded no event, it is kept ``early``, or
  ``cancel_requested`` if it recorded cancel requests, so the new try's wait can take it.
* A new try that registers a wait on the same key ends the earlier try's wait. If that wait had received a
  delivery that never resumed its task, as when the try failed between the delivery and its park, the
  delivery is taken back from it: it stays ``resumed_wait`` if other waits hold it, becomes ``accepted`` if
  it recorded topic or asset route events, so that no wait can take it, or, if it has a correlation key,
  becomes ``cancel_requested`` again if it recorded cancel requests and ``early`` again if not, so that the
  new try can take it within ``early_delivery_ttl`` of when it was first received. A delivery with none of
  these keeps the ended wait.
* A try can wait more than once, such as an operator that waits again in the method that it resumes in.
  Each registration ends the try's open wait, so a later wait never resumes with a delivery that matched
  an earlier one.
* A webhook cancel request that ends the task, through the ``cancel_on`` of its Dag or of the task itself,
  ends the try's open wait too, with the status ``ended``, and the task is not resumed: it ends ``failed``,
  or in its ``cancel_state``, as described in :doc:`webhook-cancel`. A parked task has no process, so
  neither its resume method nor its ``on_kill`` runs. A cancel of the whole run calls the Dag's
  ``on_failure_callback`` with the reason ``cancelled``, where such cleanup can go.

A delivery's record can therefore list a wait that ended before its task resumed, as when a later wait of
the same try replaced it, or when a new try took over its key and the delivery had none of the outcomes
above to go back to.

.. _webhook-waits-refused:

When a wait is refused
----------------------

The API server checks a wait when the task registers it, and a refusal fails the try, after which the
task's retries apply. The task fails with ``AirflowRuntimeError``, whose message has ``API_SERVER_ERROR``,
the status code, and the reason and message that the API server gave:

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Reason
     - Status
     - Why
   * - ``webhook_not_found``
     - 404
     - The API server does not serve the webhook: its ``[webhooks] names`` does not list it, the
       ``webhook_<name>`` connection is missing, of another type, or misconfigured, or no installed
       provider serves the connection's source type.
   * - ``webhook_unavailable``
     - 503
     - A secrets backend failed while the API server read the webhook's connection.
   * - ``wait_key_taken``
     - 409
     - Another running or parked task instance waits on the webhook with the same ``events`` and ``match``
       (see `One wait for each key`_).
   * - ``dag_has_team``
     - 403
     - :ref:`config:core__multi_team` is on, and the task's Dag has a team. Webhooks have no team in
       Airflow 3.5, so they serve only Dags without one.
   * - ``timeout_required``
     - 422
     - The wait has no timeout, and ``[webhooks] default_wait_timeout`` is not set.
   * - ``invalid_state`` or ``not_found``
     - 409, 404 or 410
     - The task instance stopped running, or was cleared, while it registered the wait.

The API server answers 422 without a reason, with a message that names what it refuses, when ``events``,
``match`` or ``timeout`` breaks the rules above, such as a timeout of more than 365 days. The task does
not send some mistakes at all: a ``timeout`` that is not a ``timedelta`` raises ``TypeError``, and an
``events`` that is a string, or a ``match`` that is not a mapping, raises ``ValueError``.

The task registers its wait once, without the retries that the Task SDK makes for other calls to the API
server, because a repeated registration would end the wait that the first one registered. A network error
or a 5xx while it registers therefore fails the try as well.

An API server older than Airflow 3.5 has no route to register waits. A task fails with
``WebhookWaitUnsupported`` on such a server instead of waiting, so upgrade the API servers before the
workers. ``WaitForWebhookOperator`` and ``@task.webhook_wait`` have no other way to wait; an operator that
can also poll can catch the exception and poll instead, as in `Writing an operator that waits`_.

While a task waits
------------------

A waiting task is parked in the ``awaiting_input`` state, as a
:doc:`Human-in-the-loop </tutorial/hitl>` task waiting for a response is. It holds no worker slot, no pool
slot and no trigger, so no triggerer has to run. Its Dag run keeps running, so it counts towards the Dag's
``max_active_runs``, and the task counts towards its own ``max_active_tis_per_dag`` and
``max_active_tis_per_dagrun``, but not towards the Dag's ``max_active_tasks``.

A delivery that matches the wait while the task still runs, before it parks, marks the wait ``received``,
as a wait that takes an early delivery when it is registered is, and the task then resumes as soon as it
parks.

The wait can be followed in several places:

* The **Details** tab of the task instance shows the try's newest wait: its webhook, linked to the
  webhook's deliveries; its status, with what it means; the events and match it accepts; its deadline;
  and, once a delivery matched it, a link to that delivery. The REST API lists the same, as described in
  :ref:`inbound-webhooks-task-waits`.
* Under **Browse → Webhook Deliveries**, the details of a delivery list the waits it went to, each linked
  to the task instance that registered it. That page, and the delivery link of the **Details** tab, show
  a delivery only to users who may read the webhook's connection, as described in `Matching a delivery`_.
* Each delivery that goes to a wait writes an audit log entry with the event ``webhook_delivery``, the
  owner ``webhook:<name>`` and the task instance, which the users who may read the Dag's audit log see.
* The scheduler emits the ``webhooks.open_waits`` gauge, the open waits of each webhook whose task is
  running or parked (see :ref:`inbound-webhooks-metrics`).

While the webhook is not served, as when the webhooks process is stopped, a waiting task keeps waiting
until its timeout. ``airflow db clean`` removes waits once their deadline has passed. A wait that received
a delivery whose record ``airflow db clean`` removed before the task parked resumes the task at its
deadline, with the timeout event rather than the delivery.

Writing an operator that waits
------------------------------

``BaseOperator.wait_for_webhook`` registers a wait and parks the task, the way ``defer`` defers it:

.. code-block:: python

    def wait_for_webhook(
        self,
        *,
        webhook: str,
        events: Sequence[str | int] | None = None,
        match: Mapping[str, str | int | Sequence[str | int]] | None = None,
        method_name: str,
        timeout: timedelta | None = None,
    ) -> NoReturn: ...

It never returns. When a delivery matches, or the timeout passes, the task runs again on a worker, in a new
instance of the operator, and calls ``method_name`` with the context and ``event``, the resume event or the
timeout event. Nothing else is kept from before the park, so derive what that method needs from the event
or the context. This operator starts a job in a service that reports each finished job to the ``jobs``
webhook, and waits for that report:

.. code-block:: python

    from datetime import timedelta

    from airflow.sdk import BaseOperator
    from airflow.sdk.exceptions import WebhookWaitUnsupported


    class RunJobOperator(BaseOperator):
        def execute(self, context):
            job_id = self.start_job()
            try:
                self.wait_for_webhook(
                    webhook="jobs",
                    events=["job.finished"],
                    match={"job_id": job_id},
                    method_name="execute_complete",
                    timeout=timedelta(hours=6),
                )
            except WebhookWaitUnsupported:
                return self.poll_until_finished(job_id)

        def execute_complete(self, context, event):
            if event.get("error_type") == "timeout":
                raise TimeoutError(event["error"])
            return event["attributes"]["job_id"]

        def start_job(self) -> str:
            """Start the job through the service's API, and return its id."""

        def poll_until_finished(self, job_id: str) -> str:
            """Poll the service until the job ends, and return its id."""

The ``jobs`` webhook's connection maps the ``job_id`` attribute with ``attribute_paths``, and sets
``correlation_key_field`` to the same field, so that the report of a job that finishes before the task
registers its wait is kept for the wait.

A try that has an open webhook wait parks on that wait each time it parks, before any Human-in-the-loop
request it has, so an operator that also waits for a human must resume from its webhook wait before it
parks for the human. ``wait_for_webhook`` is not available in a ``@task`` function: use
``@task.webhook_wait`` there.

Testing a Dag that waits
------------------------

``airflow dags test`` (and ``dag.test()``) registers a wait through an Execution API that it runs in its own
process, unless it runs tasks through an executor, so that process needs the webhook listed in its
``[webhooks] names``, with access to the webhook's connection and the provider of its source type. The task
then stays parked, and the test run waits, until a delivery that matches the wait reaches an API server
started with ``--apps webhooks`` that shares the metadata database. The test run does not time waits out:
only a scheduler running on the same metadata database does that.

``airflow webhooks send-test``, described in :ref:`inbound-webhooks-cli`, can send that delivery. It has to
carry what the source type reads the event and the attributes from, or the wait never matches it. To
resume the merge gate above, start the test run:

.. code-block:: bash

    airflow dags test merge_gate --conf '{"pr_number": 4242}'

and, while it waits, send a closed pull request to the webhooks process, here on port 8081:

.. code-block:: bash

    airflow webhooks send-test github \
        --url http://localhost:8081/webhooks/github \
        --signature-header X-Hub-Signature-256 \
        --signature-prefix sha256= \
        --header 'X-GitHub-Event: pull_request' \
        --data '{"action": "closed", "number": 4242, "repository": {"full_name": "my-org/configs"},
                 "pull_request": {"number": 4242, "merged": true, "merge_commit_sha": "6dcb09b5b5"}}'

The GitHub source type reads the event from the ``X-GitHub-Event`` header, refusing a delivery without
it, and the ``action``, ``number`` and ``repository`` attributes from the body, while ``apply`` reads
``merged`` and ``merge_commit_sha`` from the payload. The source type always checks the
``X-Hub-Signature-256`` signature, so its connection sets no ``signature_header``, and ``send-test`` is
given that header and its prefix.
