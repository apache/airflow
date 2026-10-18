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

.. _webhook-topics:

Scheduling on Inbound Webhooks
==============================

|experimental|

.. versionadded:: 3.5.0

.. warning::

    Inbound webhooks are experimental. ``WebhookTopic``, the topic names and the extra of topic events may
    change in a minor release, so check the release notes when you upgrade.

A Dag can run when a service sends Airflow a webhook, such as when a label is added to a pull request on
GitHub. The Deployment Manager sets up the webhook, as described in
:doc:`/administration-and-deployment/webhooks`: it has a name, such as ``github``, and a source type that
verifies each delivery and describes it. For each delivery, Airflow builds a list of *topic names*, and
records an asset event on each of them that a Dag schedules on. A Dag subscribes to deliveries by
scheduling on a topic, a :class:`~airflow.sdk.WebhookTopic`, and each delivery then starts a run of it,
with the delivery in ``triggering_asset_events``.

Topic names
-----------

A topic name has the form ``<webhook>/<path>/<event>/<action>/<detail>``:

* ``webhook`` is the webhook's name.
* ``path`` is where the delivery happened, ``/``-separated from broad to narrow, such as the GitHub
  repository ``my-org/web``. A delivery without a path has names without one, such as
  ``demo/deploy_finished``.
* ``event``, ``action`` and ``detail`` say what happened, from broad to specific, such as
  ``pull_request``, ``labeled`` and ``deploy-preview``.

A delivery offers every prefix of what happened, each with its path and with ``*`` in place of each
trailing part of its path, so ``*`` stands for anything below that point. A label ``deploy-preview`` added
to a pull request in ``my-org/web`` offers nine names, from broad to specific:

.. code-block:: text

   github/my-org/web/pull_request                           any pull request activity in my-org/web
   github/my-org/*/pull_request                             ... in any repository of my-org
   github/*/pull_request                                    ... in any repository
   github/my-org/web/pull_request/labeled                   any label added
   github/my-org/*/pull_request/labeled
   github/*/pull_request/labeled
   github/my-org/web/pull_request/labeled/deploy-preview    the deploy-preview label added
   github/my-org/*/pull_request/labeled/deploy-preview
   github/*/pull_request/labeled/deploy-preview

``*`` is a character of the name, not a pattern: a Dag subscribes to ``github/my-org/*/pull_request`` by
using that name. An empty part of the path is left out, while an empty part of what happened ends the name
there, dropping the parts after it. A value with a character outside ASCII is never part of a name, and
is not folded into one: a label ``Café`` ends the name before it, so the delivery offers names up to
``.../pull_request/labeled``, and a path part such as ``wéb`` is offered only as ``*``, as in
``github/my-org/*/...``. Folding would give two values the same name, such as the branches ``maín`` and
``main``. A ``/`` inside a sender's value adds a level: a push to the branch ``feature/x`` offers
``.../push/feature/x``, and no name for ``feature`` alone. Names are case-sensitive.

Each source type documents which fields of a delivery fill each part; see for example the Inbound Webhooks
page of the GitHub provider's documentation. The webhook's deliveries page in the UI lists the names that
each delivery offered.

Subscribing to a topic
----------------------

:class:`~airflow.sdk.WebhookTopic` builds a topic name from its parts, with the same rules that the API
server uses for deliveries:

.. code-block:: python

    from airflow.sdk import WebhookTopic

    preview_requested = WebhookTopic(
        webhook="github",
        path="my-org/web",
        event="pull_request",
        action="labeled",
        detail="deploy-preview",
    )
    # preview_requested.name == "github/my-org/web/pull_request/labeled/deploy-preview"

``webhook`` and ``event`` are required, and ``detail`` requires ``action``. The last part of ``path`` may
be ``*``: ``path="my-org/*"`` matches any repository of ``my-org`` and ``path="*"`` any delivery that has a
path, while ``path=None``, the default, matches only deliveries without a path. Some topics that no
delivery could offer raise ``ValueError``, such as one whose webhook name has uppercase letters or whose
path has ``*`` anywhere but in its last part or a part with a character outside ASCII. Others do not,
such as one for a webhook name that is not listed in ``[webhooks] names``.

``WebhookTopic(name="github/my-org/web/pull_request/labeled/deploy-preview")`` takes the whole name
instead, without checking it. A plain ``Asset("github/my-org/web/pull_request/labeled/deploy-preview")``
receives the same events, so Dags written for the ``airflow-provider-inbound-webhooks`` package keep
working, but it has none of the checks below.

A topic receives events only from its webhook, so:

* it cannot have ``extra``, ``watchers``, a ``group`` other than ``webhook``, which is its group, or a
  ``uri`` other than its name;
* a task that declares it in its inlets or outlets makes the Dag fail to import, and a ``@task`` that
  returns one, or that receives one as an argument, fails when it runs;
* a ``PartitionedAssetTimetable`` cannot schedule on it, because topic events have no partition key, or
  the Dag fails to import.

It accepts ``access_control`` like any asset.

When a delivery records events
------------------------------

A delivery records an event on one of its names only if a Dag that is not paused, draining or stale
schedules on that name, directly or through ``Asset.ref``. Otherwise it records nothing, and its outcome is
``dag_paused`` if only paused or draining Dags schedule on its names, or ``no_subscribers`` if none do.
Unpausing a Dag does not replay the deliveries received while it was paused. If the sender delivers one of
them again, the copy is processed again when the delivery recorded nothing, with outcome ``dag_paused``,
but is answered as a duplicate when the delivery started another Dag's run, so the unpaused Dag never
receives it.

* ``Asset.ref(name=...)`` and ``Asset.ref(uri=...)`` subscribe only to a topic that some Dag declares,
  because deliveries never create assets and events are recorded only on active assets.
* A Dag that schedules on a topic name with a partitioned timetable, which is possible only with a plain
  ``Asset``, never runs for a delivery.
* A Dag that schedules on an ``AssetAlias`` never receives topic events.
* An asset whose URI is not its name, such as ``Asset(name="github/my-org/web/issues", uri="s3://...")``,
  never receives topic events, even if its name is one of a delivery's names.
* Webhooks have no team in Airflow 3.5, so with multi-team on, their topic events queue only Dags that
  have no team.

Each name is a separate asset, so a delivery records one event on each name that a Dag schedules on.

Reading the delivery
--------------------

Each topic event's ``extra`` describes its delivery:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Key
     - Value
   * - ``from_webhook``
     - ``true``. Anyone allowed to create asset events can set it too, so it tells where an event says
       it comes from, not where it comes from.
   * - ``webhook``
     - The webhook's name.
   * - ``source_type``
     - The webhook's source type, such as ``github``.
   * - ``delivery_id``
     - The sender's id for the delivery, or null.
   * - ``inbound_delivery_id``
     - The id of Airflow's record of the delivery, as listed on the webhook's deliveries page.
   * - ``correlation_key``
     - What the delivery is about, if its source type says, or null.
   * - ``payload``
     - The delivery's body as parsed from JSON, or what the webhook's ``store_payload`` keeps of it. It is
       null when ``store_payload`` keeps nothing, or when it is larger than
       :ref:`[webhooks] max_event_payload_bytes <config:webhooks>` (64 KiB by default) as compact JSON.
   * - ``payload_truncated``
     - ``true``, present only when the payload was left out for its size.

Read it from ``triggering_asset_events``, and check for a missing payload:

.. code-block:: python

    @task
    def deploy(*, triggering_asset_events):
        for event in triggering_asset_events[preview_requested]:
            payload = event.extra["payload"]
            if payload is None:
                raise ValueError(f"No payload kept for delivery {event.extra['inbound_delivery_id']}")
            print("Pull request", payload["pull_request"]["number"])

Every user allowed to read asset events can read this extra, payload included.

Users allowed to create asset events can also record an event on a topic, with an ``extra`` of their own,
``from_webhook`` and ``payload`` included, and so start the Dags scheduled on it, whether or not they may
trigger those Dags. Airflow marks the events that do not come from a webhook in ways their creator cannot
remove: the REST API adds ``from_rest_api``, an asset watcher's trigger adds ``from_trigger``, and an
event that a task records has a ``source_dag_id``. A Dag that acts on the payload, such as by deploying
the pull request it names, should check them, as the examples on this page do not for brevity:

.. code-block:: python

    def is_webhook_event(event) -> bool:
        return (
            event.source_dag_id is None
            and event.extra.get("from_webhook") is True
            and "from_rest_api" not in event.extra
            and "from_trigger" not in event.extra
        )

Runs
----

* Each event starts its own run, the default since Airflow 3.4. A deployment that sets
  :ref:`config:scheduler__batch_asset_events` consumes queued events together instead, unless the Dag
  pins one run per event with ``AssetTriggeredTimetable(assets=topic, batch_asset_events=False)``.
* Schedule each Dag on one topic. Each name a delivery matches is a separate event, so a Dag scheduled
  on ``a | b``, where ``b`` is a broader name than ``a``, runs twice for a delivery that offers both. Pick
  the topic that selects the deliveries you want, and filter the payload in the task. ``schedule=[a, b]``
  and ``a & b`` need batching, and wait for an event on each topic.
* Keep ``catchup=False``, the default unless :ref:`config:scheduler__catchup_by_default` is set. With
  ``catchup=True``, a run also consumes every event on the topic that the Dag has not consumed, including
  events recorded before the Dag subscribed, for other Dags on the same topic. Events that reach the Dag
  only through ``Asset.ref`` are not swept in that way.
* ``dag_run.conf`` is ``{}``: the delivery is in ``triggering_asset_events``, not in the configuration.
  A run triggered by hand has no triggering asset events.
* Runs are created in the order the deliveries were recorded, and may finish in any order. A burst of
  deliveries queues behind ``max_active_runs``; the webhook's rate limit is the only bound on how many
  can queue.

Example: a service that signs its deliveries
--------------------------------------------

The ``demo`` webhook of :ref:`inbound-webhooks` uses the ``hmac_json`` source type of the standard
provider, with ``event_field`` set to ``event`` and ``path_field`` to ``service``. A delivery of
``{"event": "deploy_finished", "service": "billing", "version": "1.4.2"}`` offers the names
``demo/billing/deploy_finished`` and ``demo/*/deploy_finished``. This Dag runs once for each deployment
of the ``billing`` service:

.. code-block:: python

    from airflow.sdk import AssetTriggeredTimetable, WebhookTopic, dag, task

    billing_deployed = WebhookTopic(webhook="demo", path="billing", event="deploy_finished")


    @dag(
        schedule=AssetTriggeredTimetable(assets=billing_deployed, batch_asset_events=False),
        catchup=False,
    )
    def smoke_test_billing():
        @task
        def smoke_test(*, triggering_asset_events):
            for event in triggering_asset_events[billing_deployed]:
                payload = event.extra["payload"]
                if payload is None:
                    raise ValueError(f"No payload kept for delivery {event.extra['inbound_delivery_id']}")
                print("Testing billing", payload["version"])

        smoke_test()


    smoke_test_billing()

To run it without the service, send a delivery with ``airflow webhooks send-test``:

.. code-block:: bash

    airflow webhooks send-test demo \
        --url https://airflow.example.com/webhooks/demo \
        --delivery-id deploy-42 \
        --data '{"event": "deploy_finished", "service": "billing", "version": "1.4.2"}'

It prints ``{"status": 202, "body": {"delivery_id": "deploy-42", "outcome": "accepted"}}`` while the Dag
is unpaused. Sending it again prints status 200 and the outcome ``duplicate``, and starts no second run,
because it repeats the delivery id and the body.

Example: a label on a GitHub pull request
-----------------------------------------

With the ``github`` webhook set up as described in the Inbound Webhooks page of the GitHub provider's
documentation, this Dag deploys a preview each time the label ``deploy-preview`` is added to a pull
request in ``my-org/web``:

.. code-block:: python

    from airflow.sdk import WebhookTopic, dag, task

    preview_requested = WebhookTopic(
        webhook="github",
        path="my-org/web",
        event="pull_request",
        action="labeled",
        detail="deploy-preview",
    )


    @dag(schedule=preview_requested, catchup=False)
    def deploy_preview():
        @task
        def deploy(*, triggering_asset_events):
            for event in triggering_asset_events[preview_requested]:
                if (payload := event.extra["payload"]) is None:
                    raise ValueError(f"No payload kept for delivery {event.extra['inbound_delivery_id']}")
                print("Deploying a preview of pull request", payload["pull_request"]["number"])

        deploy()


    deploy_preview()

A push has no action, so its branch takes the action's place: pushes to ``main`` are
``WebhookTopic(webhook="github", path="my-org/web", event="push", action="main")``. GitHub redelivers a
delivery only when someone asks it to, from the webhook's **Recent Deliveries** page; Airflow answers such
a redelivery of a delivery that started a run as a duplicate.
