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

.. _inbound-webhooks:

Inbound Webhooks
================

|experimental|

.. versionadded:: 3.5.0

.. warning::

    Inbound webhooks are experimental. Their configuration, routes, responses, metrics, CLI commands and
    the webhook source interface may change in a minor release, so check the release notes when you
    upgrade. Request handling under ``/webhooks`` is still covered by the security process, as described
    in :ref:`inbound-webhooks-security`.

An inbound webhook lets a service such as GitHub start Dags by sending Airflow an HTTP request when
something happens, such as a label being added to a pull request. The service, the *sender*, delivers
each event to ``POST /webhooks/<name>`` on an API server started with ``--apps webhooks``. It does not
hold Airflow credentials. Instead, it signs each delivery with a secret it shares with Airflow, or sends
the secret with it, and the webhook's *source type* checks that before it describes the delivery.
Airflow records every delivery that passes in the metadata database and, in the same transaction,
records an asset event on each of the delivery's *topic names* that a Dag schedules on, so the scheduler
starts a run of each such Dag. An *asset route* in the webhook's connection can also have a delivery
record an event on a data asset, such as a table that the sender has loaded (see `Asset routes`_). A task
can also wait in its Dag run for a delivery that matches it, such as one saying that a pull request it
opened was merged: the delivery then resumes the task, through the task's *webhook wait*. And a Dag or a
task can declare, with ``cancel_on``, which deliveries cancel its runs, such as the runs that deploy a
preview of a pull request once the pull request is closed: such a delivery records a *cancel request*,
which the scheduler applies to the runs about the same pull request.

This page is for Deployment Managers. Dag authors subscribe to deliveries as described in
:doc:`/authoring-and-scheduling/webhook-topics`, wait for them as described in
:doc:`/authoring-and-scheduling/webhook-waits`, and have them cancel runs as described in
:doc:`/authoring-and-scheduling/webhook-cancel`. Provider authors write source types as described in the
Webhook Sources page of the providers documentation.

Setting up a webhook
--------------------

These steps set up a webhook named ``demo`` for a service that signs its JSON deliveries with
HMAC-SHA256, using the ``hmac_json`` source type of the standard provider.

1. Install the provider that ships the source type. ``hmac_json`` and ``token_header`` ship in the
   standard provider, whose Inbound Webhook Connection page describes their options, and ``github`` in
   the GitHub provider, whose Inbound Webhooks page describes it. ``airflow providers webhook-sources``
   lists the source types that the installed providers declare.

2. List the webhook in :ref:`[webhooks] names <config:webhooks>`:

   .. code-block:: ini

       [webhooks]
       names = demo

3. Create the webhook's connection, ``webhook_demo``, of the ``inbound_webhook`` type, with the shared
   secret as its password and the source type's options in its extra:

   .. code-block:: bash

       SECRET="$(openssl rand -hex 32)" && echo "$SECRET"
       airflow connections add webhook_demo \
           --conn-type inbound_webhook \
           --conn-password "$SECRET" \
           --conn-extra '{
               "source_type": "hmac_json",
               "signature_header": "X-Signature",
               "event_field": "event",
               "path_field": "service",
               "delivery_id_header": "X-Delivery-Id"
           }'

4. Check that the webhook can be served:

   .. code-block:: bash

       airflow webhooks check demo

5. Start the webhooks process, and have a reverse proxy forward ``/webhooks`` to it (see below):

   .. code-block:: bash

       airflow api-server --apps webhooks --port 8081 --proxy-headers

6. Send a test delivery, signed as the connection says:

   .. code-block:: bash

       airflow webhooks send-test demo \
           --url http://localhost:8081/webhooks/demo \
           --delivery-id deploy-42 \
           --data '{"event": "deploy_finished", "service": "billing", "version": "1.4.2"}'

   Until a Dag schedules on one of the delivery's topic names, such as ``demo/billing/deploy_finished``,
   it prints ``{"status": 202, "body": {"delivery_id": "deploy-42", "outcome": "no_subscribers"}}``.

7. Point the sender at ``https://<your-proxy>/webhooks/demo``, with the same secret. The deliveries it
   sends are listed under **Browse → Webhook Deliveries** in the Airflow UI, and at
   ``/webhook_deliveries/demo`` for this webhook alone (see `Viewing deliveries`_).

Running the webhooks process
----------------------------

Only an API server started with ``webhooks`` in ``--apps`` serves inbound webhooks. They are opt-in, and
``all``, the default, stays ``core,execution``: the routes that receive webhooks have to be reachable by
the senders, usually from the internet, while the UI and the REST API should not be. A Core API Server
answers ``/webhooks`` and every path under it with 404 and
``{"error": "Inbound webhooks are served only by airflow api-server --apps webhooks"}``, so that a sender
sent to the wrong process fails instead of receiving the UI.

An API server started with ``--apps webhooks``:

* serves ``POST /webhooks/<name>`` for each name that :ref:`[webhooks] names <config:webhooks>` lists,
  also with a trailing slash, because senders such as GitHub do not follow redirects;
* serves ``GET /webhooks/_health``, which answers ``{"status": "healthy"}`` without reading the database,
  for load balancer and Kubernetes probes; requests to it are not access-logged;
* serves nothing else: no UI, no REST API, and no API documentation at its root;
* does not start if ``[webhooks]`` is invalid or ``names`` lists no webhooks;
* logs an error at startup for each source type that it cannot serve because of a conflict between
  providers (see `Source types`_).

It reads ``[webhooks]`` once, at startup, so restart it after changing ``names``. Several processes can
serve the same webhooks: recognizing repeated deliveries and recording them rely on the database alone,
and only the connection cache and the rate limit are kept per process.

Run the process apart from the Core API Server, and put a reverse proxy in front of it that terminates
TLS, forwards only ``/webhooks`` to it, bounds request sizes, buffers each request body and times out
slow ones, and limits requests per sender address: the process reads a body for as long as the client
takes to send it, and ``[webhooks] rate_limit_per_minute`` counts verified deliveries per webhook, not
requests per address. In nginx, for example, ``client_max_body_size``, ``proxy_request_buffering on``,
``client_body_timeout`` and ``limit_req`` do this.
Start the process with ``--proxy-headers``, and set ``FORWARDED_ALLOW_IPS`` to the proxy's addresses, so
that the access log records each sender's address rather than the proxy's. Leave the variable unset only
with the ``uvicorn`` :ref:`server type <config:api__server_type>` and a proxy on the same host: with
``gunicorn``, an unset variable trusts every client, so a sender that reaches the process directly can
choose the address that is logged. :doc:`web-stack` describes these settings, ``base_url`` paths, and
combining ``webhooks`` with other apps in development.

Configuration
-------------

The :ref:`[webhooks] <config:webhooks>` section of the configuration reference describes each option.

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Default
     - Meaning
   * - ``names``
     - empty
     - The webhooks to serve, comma-separated. Read at startup.
   * - ``source_type_owners``
     - empty
     - ``source_type=package`` pairs that pick the package serving a source type (see `Source types`_).
   * - ``connection_cache_ttl``
     - ``60``
     - Seconds a webhook's connection, or the failure to find or use it, is reused in each process.
   * - ``max_body_bytes``
     - ``1048576``
     - Largest body a webhook accepts, unless its connection sets its own ``max_body_bytes``.
   * - ``delivery_retention``
     - ``7``
     - Days a delivery's record, and the cancel requests it recorded, are kept before ``airflow db clean``
       may remove them, at most 365.
   * - ``rate_limit_per_minute``
     - ``0``
     - Verified deliveries per minute that each process takes for each webhook; ``0`` is no limit.
   * - ``store_payload``
     - ``full``
     - What is kept of a delivery's payload: ``full``, ``summary`` or ``none``.
   * - ``max_event_payload_bytes``
     - ``65536``
     - Largest payload copied into a topic event's extra; ``0`` copies every payload.
   * - ``early_delivery_ttl``
     - ``600``
     - Seconds after its receipt that a delivery kept for webhook waits, ``early`` or ``cancel_requested``,
       can still go to a wait registered later.
   * - ``early_delivery_max_per_webhook``
     - ``1000``
     - Most deliveries kept for webhook waits, ``early`` or ``cancel_requested``, within
       ``early_delivery_ttl`` that a webhook keeps; older ones expire.
   * - ``default_wait_timeout``
     - empty
     - Seconds a webhook wait registered without a timeout lasts, at most 31536000 (365 days). When it is
       empty, such a wait is refused.
   * - ``cancel_requests_per_loop``
     - ``20``
     - Most webhook cancel requests the scheduler applies each time it looks for them, about once a second.
       Each request is applied and committed on its own, and the rest wait for the next time. At least 1.

The scheduler reads ``cancel_requests_per_loop``, at startup, and does not start if it is below 1; the
webhooks process does not read it.

The API servers that serve the Execution API read ``[webhooks]`` too, when a task registers a webhook
wait: they refuse a wait on a webhook that their ``names`` does not list, or whose connection they cannot
read or use, and take ``default_wait_timeout`` and ``early_delivery_ttl`` from their own options. Give them
the same ``[webhooks]`` options as the webhooks process, the same access to the ``webhook_<name>``
connections, and the providers that ship the source types, or tasks cannot wait on the webhooks (see
:doc:`/authoring-and-scheduling/webhook-waits`).

Webhooks and their connections
------------------------------

Each name that ``[webhooks] names`` lists is a webhook, served at ``/webhooks/<name>``. A name has 1 to 64
lowercase letters, digits, ``-`` and ``_``, and starts with a letter or a digit, so no name can collide
with ``_health``. ``other`` is reserved, because the webhook metrics use it for names that are not listed.
The list is what exposes a route: creating a webhook connection exposes nothing until its name is listed,
and a request to a name that is not listed is answered 404 without looking up a connection.

Listing a name also gives its sender the asset names that start with ``<name>/``: its deliveries record
events on any such asset whose URI is its name, as that of a ``WebhookTopic`` or an ``Asset`` given only
a name, and start the Dags scheduled on it. Do not list a name that Dag authors use as the start of other
asset names.

The connection ``webhook_<name>`` configures the webhook. It must have the type ``inbound_webhook``,
which the standard provider registers, and it can come from any secrets backend, an environment variable
or the metadata database:

* Its password is the shared secret, and must not be empty.
* ``source_type`` in its extra names the source type. It defaults to the webhook's name, so
  ``webhook_github`` uses the ``github`` source type.
* ``max_body_bytes`` in its extra replaces ``[webhooks] max_body_bytes`` for this webhook, higher or
  lower. Anyone who may edit the connection can therefore raise the limit.
* ``store_payload`` in its extra sets what is kept of its payloads (see `Stored payloads`_).
* ``asset_routes`` in its extra has its deliveries record events on data assets (see `Asset routes`_).
* The rest of its extra holds the options that the source type documents, such as ``signature_header``
  for ``hmac_json``.

Whoever may edit ``webhook_<name>`` administers the webhook: they can change its secret and its source
type, and so decide which deliveries Airflow accepts as coming from that sender, and its asset routes, and
so decide which assets its deliveries record events on.

Each process caches a webhook's connection for ``[webhooks] connection_cache_ttl`` seconds, including the
failure to find or use it, so a changed connection can take that long to apply. A changed secret applies
sooner: a delivery that fails verification has the connection read again, at most once per
``connection_cache_ttl`` for each webhook, and is verified against what that finds. To rotate a secret,
change it in the connection and then in the sender, within ``connection_cache_ttl`` of each other. The
first delivery signed with the new secret then usually passes, and deliveries signed with the old one pass
until the cached connection is read again or expires. A failure to read a secrets backend is never cached.

Webhooks have no team in this version of Airflow. ``[webhooks] names`` refuses a name qualified with a team,
such as ``team:name``, the connection is looked up without a team, so a connection that belongs to a team is
not found, and when :ref:`config:core__multi_team` is on, topic events queue only Dags that have no team, an
asset route records nothing for an asset that a Dag of a team schedules on, a task whose Dag has a team cannot
register a webhook wait, and no delivery records a cancel request for a Dag that has a team (see
:doc:`/authoring-and-scheduling/webhook-cancel`).

Source types
------------

A source type verifies and describes the deliveries of one kind of sender. Providers declare their source
types under ``webhook-sources`` in their ``provider.yaml``, and the Webhook Sources page of the providers
documentation lists those of the community providers. ``airflow providers webhook-sources`` lists every
source type that the installed providers declare, with the package and class that implement it, and
whether it is served.

A source type that more than one installed package declares is served by none of them, so a package
cannot take over another provider's source type by declaring it too. Its webhooks are answered
503, ``airflow providers webhook-sources`` shows ``conflict: served by none``, ``airflow webhooks check``
reports the conflict, and the webhooks process logs an error for it at startup. To serve it, set
:ref:`[webhooks] source_type_owners <config:webhooks>` to the package that should serve it:

.. code-block:: ini

    [webhooks]
    source_type_owners = github=apache-airflow-providers-github

A source type listed there is served only by the package named, even if no other package declares it.
If the package named does not declare the source type, the source type is not served. The webhooks
process then logs an error for it at startup if another package declares it, and
``airflow webhooks check`` reports it for each webhook that uses it.

Airflow never reads a class path from a connection's extra: a source type is always a class that an
installed provider declares.

How a delivery is processed
---------------------------

Each request to ``POST /webhooks/<name>`` passes these steps in order, and stops at the first that
refuses it:

1. The name must be listed in ``[webhooks] names``.
2. The webhook's connection is read, from the per-process cache.
3. The body must fit within the webhook's ``max_body_bytes``, checked on ``Content-Length`` first and then
   while it is read.
4. The source type verifies the request with the shared secret. A source type that declares its
   handshakes unsigned has handshakes checked before this step instead.
5. A handshake, a request with which some senders check a webhook's URL, is answered with the body the
   source type returns. Nothing is recorded, and it does not count against the rate limit.
6. The delivery must be within ``[webhooks] rate_limit_per_minute``.
7. The source type parses the delivery.
8. One transaction records the delivery, its topic events, the events of its asset routes and, if it
   has a correlation key, a cancel request for each Dag whose ``cancel_on`` it matches (see
   :doc:`/authoring-and-scheduling/webhook-cancel`), with an audit log entry if it recorded any event or
   cancel request, and hands the delivery to the
   :doc:`webhook waits </authoring-and-scheduling/webhook-waits>` that it matches, with an audit log entry
   for each (see `Outcomes and repeated deliveries`_).
9. The response is sent once that transaction has committed.

The status codes and response bodies are:

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Status
     - When
     - Body
   * - 202
     - The delivery was recorded.
     - ``{"delivery_id": <the sender's id or null>, "outcome": <outcome>}``
   * - 200
     - The delivery repeats one that already acted.
     - The same, with ``"outcome": "duplicate"``.
   * - 200
     - A handshake.
     - The body the source type returns.
   * - 400
     - The source type finds the delivery or handshake malformed, the delivery id is longer than 255
       characters or not printable ASCII, or the client disconnects before sending the whole body.
     - ``{"detail": <what is wrong with the request>}``
   * - 401
     - Verification failed.
     - ``{"detail": "Invalid webhook credentials"}``
   * - 404
     - The name is not listed, or there is no connection ``webhook_<name>``.
     - ``{"detail": "Unknown webhook"}``
   * - 404
     - Any other path under ``/webhooks``, such as ``/webhooks/<name>/events``.
     - ``{"detail": "Not Found"}``
   * - 405
     - A method other than ``POST`` to ``/webhooks/<name>``.
     - ``{"detail": "Method Not Allowed"}``
   * - 413
     - The body is larger than the webhook's ``max_body_bytes``.
     - ``{"detail": "Body exceeds <max_body_bytes> bytes"}``
   * - 429
     - A verified delivery over the rate limit. ``Retry-After`` says when to send it again.
     - ``{"detail": "Too many deliveries"}``
   * - 500
     - A bug in the source type or in Airflow.
     - ``{"detail": "Internal Server Error"}``
   * - 503
     - The connection is misconfigured, its source type is declared by more than one package, a secrets
       backend fails, or the metadata database is unavailable, locked, out of pooled connections or not
       yet migrated. ``Retry-After`` is ``30``.
     - ``{"detail": "Temporarily unavailable"}``

No response carries a traceback, and a 503 does not say which of its causes applies. The logs do: Airflow
logs the refusals of a listed webhook, such as a failed verification, a body over its limit, the rate
limit, a malformed delivery, a missing connection or an unavailable database, and the errors answered
500, at most once a minute for each webhook and reason, and a misconfigured connection once until it
changes, with what is wrong with it. Beyond its access log line, it does not log requests to names that
are not listed, requests to a path or with a method that it does not serve, or a client that disconnects
before sending the whole body. Every request under ``/webhooks`` other than ``/webhooks/_health`` gets
an access log line, refused or not, so the access log grows with every request that reaches the process.

The delivery contract follows from this order. A 202 means that the delivery's record, whatever topic
events, asset route events, cancel requests and audit log entries it recorded, and what it did for webhook
waits, committed before the answer was sent; a 200 ``duplicate`` means that an earlier copy's did; a 200
handshake records nothing. Asset listeners and metrics run after the commit, and the scheduler applies the
cancel requests after the answer, within a second or two while it runs. A 5xx records nothing, so a
sender that retries can deliver again once the cause is fixed. The one exception is a delivery kept for
webhook waits, ``early`` or ``cancel_requested``, which looks for waits once more after its record has
committed: if that second look fails, the delivery is answered 5xx but its record and its cancel requests
are kept, and a retry is processed again in full, as a repeat of such a delivery is (see
`Outcomes and repeated deliveries`_). Some senders, such as Jira Cloud, retry
on their own, but GitHub does not, so a GitHub delivery refused during an outage has to be sent again from
the webhook's **Recent Deliveries** page in GitHub. Delivery is at least once and unordered.

.. _inbound-webhooks-outcomes:

Outcomes and repeated deliveries
--------------------------------

A recorded delivery has the first of these outcomes that applies to it:

* ``resumed_wait``: it went to at least one :doc:`webhook wait </authoring-and-scheduling/webhook-waits>`,
  a task waiting for a delivery that matches it. A wait whose task is parked resumes the task at once, and
  one whose task is still running resumes it once it parks. Its record lists the waits, and the topic
  events, asset route events and cancel requests that it recorded as well, if any.
* ``accepted``: it recorded topic events or asset route events, for at least one Dag that is not paused,
  and went to no wait. Its record lists the cancel requests that it recorded as well, if any. It is not
  kept for a wait, even if it has a correlation key, so a wait registered after it never gets it.
* ``cancel_requested``: it has a correlation key, recorded no event and went to no wait, and recorded a
  cancel request for at least one Dag whose ``cancel_on`` it matches, which the scheduler applies to the
  runs about its correlation key (see :doc:`/authoring-and-scheduling/webhook-cancel`). It is kept for a
  wait as an ``early`` delivery is, and counts toward ``early_delivery_max_per_webhook`` as one does: a
  wait that gets it makes it ``resumed_wait``, and its cancel requests stay.
* ``early``: it has a correlation key, recorded no event, requested no cancel and went to no wait. It is
  kept, with its payload, for a wait that matches it and is registered within
  :ref:`[webhooks] early_delivery_ttl <config:webhooks>` seconds of its receipt. Such a wait gets it as
  soon as it is registered, and resumes its task once the task parks; the delivery's outcome then becomes
  ``resumed_wait``. Once that time has passed, it keeps its outcome and its payload until its record is
  removed (see `Retention`_), but no wait gets it. Unpausing a Dag does not replay it, but a repeat of it
  is processed again in full.
* ``expired``: it was kept for waits, ``early`` or ``cancel_requested``, until its webhook kept
  :ref:`[webhooks] early_delivery_max_per_webhook <config:webhooks>` later such deliveries received
  within ``early_delivery_ttl``. Its payload is dropped, and no wait registered after then gets it unless
  the delivery is repeated. The cancel requests it recorded, if any, stay.
* ``dag_paused``: it has no correlation key, and only Dags that are paused or draining schedule on its
  names or on its routes' assets, so it recorded nothing. Unpausing a Dag does not replay it.
* ``no_subscribers``: it has no correlation key, no Dag that an event could start schedules on its names,
  and none of its asset routes could record an event, so it recorded nothing.

The record of a delivery says why each of its asset routes recorded nothing, whatever its outcome. A
delivery requests a cancel whatever else it does, so the cancel requests of a ``resumed_wait`` or
``accepted`` delivery are listed in its record too.

``duplicate`` is only ever answered, never recorded. A delivery repeats a recorded one when it is sent to
the same webhook with the same delivery id and the same body; the body counts, because senders send the
delivery id in a header that their signature does not cover. A repeat of an ``accepted`` or
``resumed_wait`` delivery is answered 200 ``duplicate`` and records nothing. A repeat of a delivery that
recorded no event and went to no wait, with outcome ``early``, ``cancel_requested``, ``expired``,
``dag_paused`` or ``no_subscribers``, is processed again in full, as a first delivery is, without a second
record: a Dag that subscribed or was unpaused since then receives it, a webhook wait registered since then
gets it, a Dag that declared a matching ``cancel_on`` since then gets a cancel request, and the record then
says what the repeat did. The delivery recorded no event before, so no event is recorded twice, and it
never records a second cancel request for a Dag: a repeat of a ``cancel_requested`` delivery keeps the
requests of the first copy, and is answered 202 ``cancel_requested`` again unless it now records an event
or goes to a wait. A request that a repeat records
is dated from when the delivery was first received, as its record is. A repeat that is ``early`` again is
early for ``early_delivery_ttl`` from when the delivery was first received. If two copies of a delivery
arrive at once, at most one of them acts. A delivery without a delivery id, or with an empty one, cannot be
recognized as a repeat, so each copy of it acts.

The generic sources read the delivery id from the header or field that the connection's
``delivery_id_header`` or ``delivery_id_field`` names, and other source types from where their sender puts
it, such as ``X-GitHub-Delivery``. The ``hmac_json`` signature covers the body only, so whoever captures a
delivery can send it again under a new id in ``delivery_id_header``, and it acts again. If the sender puts
its delivery id in the signed body, use ``delivery_id_field`` instead: a copy sent again then always
repeats the delivery, as long as its record is kept.

.. _inbound-webhooks-asset-routes:

Asset routes
------------

A delivery is sometimes about a data asset rather than a topic, as when an ETL service reports that it has
loaded a table. An *asset route* has the webhook's deliveries record an event on such an asset, so that
the Dags scheduled on it run. Each route, in ``asset_routes`` in the extra of the webhook's connection,
names one of the webhook's topic names and an asset: a delivery that offers the topic name also records
an event on the asset, in the same transaction as its record and its topic events. For example, with this
extra on ``webhook_etl``:

.. code-block:: json

    {
        "source_type": "hmac_json",
        "signature_header": "X-Signature",
        "event_field": "event",
        "path_field": "table",
        "asset_routes": [
            {
                "topic": "etl/orders/load.completed",
                "asset": "s3://warehouse/orders",
                "partition_key_field": "data.date"
            }
        ]
    }

a delivery of ``{"event": "load.completed", "table": "orders", "data": {"date": "2026-10-19"}}`` records
an event on the asset whose URI is ``s3://warehouse/orders``, with the partition key ``2026-10-19``.

``asset_routes`` is a list of objects with these keys, or that list as JSON text, as in a connection given
as a URI:

* ``topic`` (required): one of the names that the webhook's deliveries offer, as described in
  :doc:`/authoring-and-scheduling/webhook-topics`, such as ``etl/orders/load.completed`` or
  ``etl/*/load.completed``. A route whose topic a delivery does not offer does nothing for it.
* ``asset`` (required): the asset's URI, or else its name: a route looks for an active asset with this
  URI first, and then for one with this name, comparing each exactly with what Airflow stores. Airflow,
  and the provider that handles the URI's scheme, normalize a URI when a Dag declares the asset, so the
  stored URI can differ from the one in the Dag's code: with the Postgres provider,
  ``Asset(name="orders", uri="postgres://db/analytics/public/orders")`` is stored with the URI
  ``postgres://db:5432/analytics/public/orders``, which a route must then use, or else the name
  ``orders``. The asset's page in the UI shows both. On MySQL, where asset names and URIs are ASCII, a
  route whose ``asset`` is not finds no asset.
* ``partition_key_field``: a dotted path into the delivery's payload, such as ``data.date``, to the
  event's partition key. Without it, the event has no partition key. The partition key is kept whatever
  ``store_payload`` says (see `Stored payloads`_), so do not point it at personal data.

Any other shape, or any other key, leaves the connection misconfigured: its deliveries are answered 503,
and ``airflow webhooks check`` says what is wrong.

As a topic does, a route records an event only for a Dag that can run:

* The asset must be active, that is, declared by a Dag. Deliveries never create assets.
* The asset must not be a webhook topic, that is, an asset in the ``webhook`` group, such as a
  ``WebhookTopic``: a topic receives events only from its own webhook. A plain ``Asset`` named like a
  topic has no such guard.
* A Dag that is not paused, draining or stale must schedule on the asset, directly or through
  ``Asset.ref``, and be one that the event can start. A keyed event, one with a partition key, queues only
  partition-aware Dags: with ``partition_key_field``, only a Dag with a partitioned timetable runs for the
  event, and a Dag scheduled on the asset without one does not. Without ``partition_key_field``, only the
  Dags without a partitioned timetable can.
* The partition key is the value at ``partition_key_field``, read with no template engine and no access
  to Airflow's context, connections or variables. It must be a non-blank string of at most 250
  characters, without U+0000, an unpaired surrogate, or a character outside the Basic Multilingual Plane,
  such as an emoji, which some databases cannot store; any other value, or no value, skips the route.
* Webhooks have no team in Airflow 3.5, so when :ref:`config:core__multi_team` is on, a route records
  nothing for an asset that any Dag of a team schedules on, paused or not, even if Dags without a team
  schedule on it too.

A route that a delivery offers the topic of but that records nothing is listed in the delivery's record,
with why (see `Why a Dag did not run`_). Each route that matches records its own event, so two routes to
one asset that a delivery both matches record two events on it. The events carry the same extra as topic
events, described in :doc:`/authoring-and-scheduling/webhook-topics`, so Dags read the delivery from them
in the same way, and ``store_payload`` and ``[webhooks] max_event_payload_bytes`` apply to their payloads
as they do to those of topic events. A delivery that acts through a route alone is ``accepted``, writes an
audit log entry, and is recognized as a repeat like any other (see `Outcomes and repeated deliveries`_).

A route needs only edit rights on ``webhook_<name>``, while ``POST /api/v2/assets/events`` requires
permission to create events on the asset. So whoever may edit the connection can have its deliveries record
events on any active asset other than a webhook topic, and start the Dags scheduled on it. Give edit rights
on webhook connections only to users who may also record events on the assets that a route could name.

Stored payloads
---------------

What is kept of a delivery's payload is set by ``store_payload`` in its connection's extra, or else by the
source type's default if it has one, or else by :ref:`[webhooks] store_payload <config:webhooks>`:

* ``full`` keeps the payload as parsed from the body;
* ``summary`` keeps what the source type's summary keeps, or the whole payload if the source type defines
  no summary. The summary of the ``github`` source type drops each email address with the name beside it,
  and reduces the objects that GitHub repeats to their name or login, but keeps account logins and free
  text such as commit messages and pull request bodies;
* ``none`` keeps nothing.

A delivery that acts keeps a payload: an ``accepted``, ``resumed_wait``, ``cancel_requested`` or ``early``
delivery keeps one in its record, in the extra of each of its topic events and asset route events, and in the
event that a webhook wait resumes its task with. Deliveries that act on nothing, ``no_subscribers`` and
``dag_paused``, keep none, so a sender that sends every event does not fill the database, and an ``expired``
delivery drops its payload. An ``early`` or ``cancel_requested`` delivery keeps its payload until its record
is removed, also once ``early_delivery_ttl`` has passed, so every delivery with a correlation key that no Dag
subscribes to, such as each ``github`` delivery about a pull request or an issue, keeps its payload for
``delivery_retention`` days. Each event carries its own copy, and the copy is left out of an event, with
``payload`` null and ``payload_truncated`` true, when it is larger than
:ref:`[webhooks] max_event_payload_bytes <config:webhooks>` as compact JSON; the delivery's record, and the
event that a wait resumes its task with, keep it all the same.

Every user allowed to read asset events can read their extra, and every Dag scheduled on a topic, or on an
asset that an asset route names, receives it. When the payloads must not keep personal or otherwise sensitive
data, set ``store_payload`` to ``none``, or to ``summary`` once you have checked what the source type's
summary keeps. ``store_payload`` does not cover the partition key that an asset route reads from the
payload: it is kept in the asset event, in the delivery's record, in the Dag run it starts and in the
records that queue that run, so point ``partition_key_field`` only at data that may be kept.

Retention
---------

Each delivery's record has a ``retain_until`` time: the time it was received plus
:ref:`[webhooks] delivery_retention <config:webhooks>` days. ``airflow db clean`` removes records by that
column rather than by the time they were received, so a ``--clean-before-timestamp`` in the past never
removes a record inside its retention, while one in the future does. A changed ``delivery_retention``
applies to deliveries received after the change. Nothing else removes records, so run ``airflow db clean``
regularly, as for the other tables it cleans; ``--yes`` skips its confirmation prompt, as a scheduled run
needs. Unless you pass ``--skip-archive``, it keeps the records it removes, payloads included, in an archive
table. Clean the cancel requests that deliveries record, described below, in the same run:

.. code-block:: bash

    airflow db clean --tables inbound_delivery,webhook_cancel_request --yes \
        --clean-before-timestamp "$(date -u '+%Y-%m-%dT%H:%M:%S+00:00')"

Keep ``delivery_retention`` longer than the longest time over which any sender you configure retries a
delivery: a retry that arrives after its delivery's record was removed is not recognized as a repeat, and
acts again. Topic events and audit log entries are removed with their own tables.

``airflow db clean`` removes the webhook waits of tasks, in the ``inbound_wait`` table, by their
``deadline`` column, so a wait is kept until its deadline however long ago it was registered. A wait that
received a delivery whose record was removed before its task parked resumes the task at its deadline, with
the timeout event rather than the delivery.

The cancel requests that deliveries record, in the ``webhook_cancel_request`` table, have the
``retain_until`` of their delivery's record, and ``airflow db clean`` removes them by that column, whether
or not the scheduler has applied them. A request is kept once it is applied, because the scheduler also
applies it to a run created later from an earlier delivery, as described in
:doc:`/authoring-and-scheduling/webhook-cancel`. Deleting a Dag removes its requests. The conditions that
Dags declare with ``cancel_on``, in the ``dag_cancel_condition`` table, belong to their Dags: the Dag
processor keeps them in step with each Dag's code, deleting a Dag removes them, and
``airflow db clean`` does not clean them.

.. _inbound-webhooks-deliveries:

Viewing deliveries
------------------

The record of each delivery can be read in the UI and through the REST API: when the delivery was
received, its webhook, source type, delivery id, correlation key and attributes, its outcome, the topic
names it offered, the topic events and asset route events it recorded, the asset routes it skipped, the
webhook waits it went to, and the cancel requests it recorded, with what the scheduler did with each.
Neither shows its payload.

In the UI
~~~~~~~~~

**Browse → Webhook Deliveries** has two tabs:

* **Deliveries**, at ``/webhook_deliveries``, lists the deliveries of every webhook that you may read,
  newest first: when each was received, its webhook, outcome, delivery id, correlation key and source
  type. Its filters select one or more webhooks, an outcome, a delivery id, a correlation key, a source
  type and a range of times of receipt. They are kept in the page's address, so a filtered list can be
  shared as a link.
* **Webhooks**, at ``/webhook_overview``, lists the webhooks that you may read, with whether each can be
  served and what it last received (see `The webhooks overview`_).

A webhook's name, in either tab, links to ``/webhook_deliveries/<name>``, which lists only that webhook's
deliveries. The **Connections** page links there too, from each ``inbound_webhook`` connection stored in
the metadata database.

The **Show Details** button of a delivery opens a panel with its details: its id, webhook, delivery
id, time of receipt, source type and correlation key; its outcome, with a sentence on what it means; its
attributes; the topic names it offered, each with a button that copies it; the topic events it
recorded, each linked to its asset's page; the events its asset routes recorded, each linked to its
asset's page with the route's topic and the partition key; the asset routes it skipped, each with
why; the webhook waits it went to, each linked to the page of the task instance that registered it,
with its Dag id, run id and the wait's id; and the cancel requests it recorded, each with its Dag, its
scope, the tasks it ends, its status, with what it means, and when the scheduler applied it or failed to,
and the runs and task instances it cancelled, each linked to its page, or the runs it left alone, with why.
``?id=<id>`` in the address opens the panel of the delivery
with that id, as in ``/webhook_deliveries/demo?id=<id>``. Wherever the UI lists asset events, the source
of an event that a delivery recorded reads **Webhook** and links there.

Who can read deliveries
~~~~~~~~~~~~~~~~~~~~~~~

Reading a webhook's deliveries, or its row in the overview, requires permission to read its connection,
``webhook_<name>``, which the **op** and **admin** roles of the Simple auth manager, and the ``Op`` and
``Admin`` roles of the FAB auth manager, have. The lists across webhooks, in both tabs and through the REST
API, leave out every webhook whose connection the user may not read, so a user who may read none sees
empty lists rather than an error. They take the webhooks from the Core API Server's ``[webhooks] names``
and from the records, so a webhook removed from ``names`` keeps its deliveries readable.

Which users see **Webhook Deliveries** in the **Browse** menu depends on the auth manager:

* The Simple auth manager shows it to every user.
* The FAB auth manager shows it to users with menu access to Connections, ``Connections.menu_access``,
  which the ``Op`` and ``Admin`` roles have. This needs a release of ``apache-airflow-providers-fab`` that
  maps the entry to Connections. With an older release, the entry is hidden for every role, ``Admin``
  included, but the pages can still be opened at their addresses.
* With the Keycloak auth manager, run ``airflow keycloak-auth-manager create-resources`` again when you
  upgrade. Until Keycloak has a resource for the entry, it refuses the UI's menu authorization request, and
  the menus fail to load for every user. Webhooks have no team in Airflow 3.5, so the roles that
  ``--teams`` creates for a team do not get the entry.

Through the REST API
~~~~~~~~~~~~~~~~~~~~

``GET /api/v2/webhooks/{webhook_name}/deliveries`` lists the deliveries of one webhook, and
``GET /api/v2/webhooks/~/deliveries`` those of every webhook that the caller may read, as the
**Deliveries** tab does; ``~`` is never the name of a webhook. Both list newest first, and take:

* ``limit`` and ``offset``;
* ``order_by``: ``received_at``, ``id``, ``webhook``, ``outcome`` or ``delivery_id``, descending with a
  leading ``-``, and ``-received_at`` by default;
* ``webhook_names``, once for each webhook, as in ``?webhook_names=github&webhook_names=demo``, to list
  only the deliveries of those webhooks;
* ``outcome``, ``delivery_id``, ``id``, ``correlation_key`` and ``source_type``, each matched exactly;
* ``received_at_gte``, ``received_at_gt``, ``received_at_lte`` and ``received_at_lt``.

A ``{webhook_name}`` whose connection the caller may not read is answered 403, and one that neither the
Core API Server's ``[webhooks] names`` lists nor any record has is answered 404. Each delivery has these
fields:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Value
   * - ``id``
     - Airflow's id for the record, which its topic events carry as ``inbound_delivery_id``.
   * - ``webhook``
     - The webhook's name.
   * - ``delivery_id``
     - The sender's id for the delivery, or null.
   * - ``received_at``
     - When Airflow first received the delivery. A repeat that is processed again (see
       `Outcomes and repeated deliveries`_) can change ``outcome``, ``candidates`` and ``actions``, but not
       this.
   * - ``source_type``
     - The source type that verified and parsed the delivery.
   * - ``attributes``
     - What the source type parsed from the delivery, such as ``{"event": "deploy_finished"}``.
   * - ``correlation_key``
     - What the delivery is about, if its source type says, or null.
   * - ``outcome``
     - ``resumed_wait``, ``accepted``, ``cancel_requested``, ``early``, ``expired``, ``dag_paused`` or
       ``no_subscribers`` (see `Outcomes and repeated deliveries`_).
   * - ``candidates``
     - The topic names that the delivery offered.
   * - ``actions``
     - What the delivery did, or null if it recorded nothing, skipped no asset route, went to no webhook
       wait and recorded no cancel request. Each of these keys is present only when its list is not empty:

       * ``asset_events``: the topic events that the delivery recorded, sorted by asset name, each with the
         asset's name, the asset's id and the asset event's id, as in
         ``{"asset": "demo/billing/deploy_finished", "asset_id": 12, "event_id": 345}``;
       * ``asset_routes``: the events that its asset routes recorded, in the order of the routes, each with
         the route's ``topic`` and ``asset``, the asset's id, the asset event's id and the partition key, or
         null, as in ``{"topic": "etl/orders/load.completed", "asset": "s3://warehouse/orders",
         "asset_id": 7, "event_id": 346, "partition_key": "2026-10-19"}``;
       * ``skipped_asset_routes``: the asset routes whose topic it offered that recorded nothing, in the
         order of the routes, each with the route's ``topic``, ``asset`` and ``partition_key_field``, or
         null, and the ``reason``, one of those listed in `Why a Dag did not run`_;
       * ``waits``: the webhook waits that the delivery went to, each with the wait's id, and the id, Dag
         id, run id, task id and map index of the task instance that registered it, as in
         ``{"wait_id": "0199...", "ti_id": "0199...", "dag_id": "deploy", "run_id": "manual__2026-10-20",
         "task_id": "wait_for_merge", "map_index": -1}``, plus its ``region_id`` and ``region_index`` if
         the task instance is in a dynamic region, such as a mapped task's expansion or a loop;
       * ``cancel_requests``: the cancel requests that the delivery recorded, each with the request's id,
         the Dag id, and the scope, ``run`` if a condition of the Dag itself matched or else ``task``, as in
         ``{"request_id": "0199...", "dag_id": "deploy_preview", "scope": "run"}``. It summarizes the
         ``cancel_requests`` field, which is authoritative.
   * - ``cancel_requests``
     - The cancel requests that the delivery recorded, by Dag id, each with what the scheduler did with it
       (see :doc:`/authoring-and-scheduling/webhook-cancel`). These records are authoritative: ``actions``
       summarizes them, can miss one that a concurrent repeat of the delivery recorded, and still names a
       request whose record was removed. Each has:

       * ``id`` and ``dag_id``;
       * ``targets``: what the request cancels in each run about the delivery's correlation key, as in
         ``{"run": {"correlation_key_confs": ["pr"]}, "tasks": [{"task_id": "run_e2e_tests",
         "cancel_state": "skipped", "correlation_key_confs": []}]}``. ``run`` is null unless a condition of
         the Dag itself matched, and fails the whole run; ``tasks`` lists, by task id, each task whose
         condition matched, with the state it ends the task in. ``correlation_key_confs`` lists the conf
         keys under which a run triggered with a conf holds the correlation key, and is empty when only
         the runs that a delivery started are about it;
       * ``status``: ``pending`` until the scheduler applies the request, which it does within a second
         or two while it runs; then ``applied``, or ``failed`` if applying it raised an error, which the
         scheduler logged;
       * ``applied_at``: when the scheduler applied the request or failed to, or null;
       * ``result``: null until the scheduler acts on the request, then what it did:
         ``{"dag_runs": [...], "task_instances": [...], "skipped_runs": [...], "error": null}``.
         ``dag_runs`` lists the ``run_id`` and ``state`` of each run that the request cancelled, ``failed``,
         or ``running`` or ``queued`` while a teardown still has to run; ``task_instances`` the
         ``ti_id``, ``run_id``, ``task_id``, ``map_index`` and new ``state`` of each task instance that it
         ended, plus ``region_id`` and ``region_index`` if the task instance is in a dynamic region;
         ``skipped_runs`` the ``run_id`` of each run about the key that it left alone, with the
         ``reason``, ``dag_not_found`` if the run's Dag version could not be loaded or ``dag_has_team``
         (see :doc:`/authoring-and-scheduling/webhook-cancel`); and ``error`` the class name of the error
         that failed it. A run created later from an earlier delivery is added when the scheduler cancels
         it as it creates it, which can happen while the request is ``pending``.
   * - ``team_name``
     - Null: webhooks have no team in Airflow 3.5.

The webhooks overview
~~~~~~~~~~~~~~~~~~~~~

``GET /api/v2/webhooks``, which the **Webhooks** tab shows, lists by name the webhooks whose connection the
caller may read, with ``limit`` and ``offset``. It says for each whether it can be served, as
``airflow webhooks check`` does, but without saying why not:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Value
   * - ``name``
     - The webhook's name.
   * - ``listed``
     - Whether ``[webhooks] names`` lists the webhook, which it must to be served. A webhook that it does
       not list is shown because it has records.
   * - ``connection_status``
     - ``ok`` if the connection ``webhook_<name>`` configures a webhook that can be served; ``not_found``
       if no secrets backend has it; ``wrong_type`` if its type is not ``inbound_webhook``;
       ``misconfigured`` if it cannot be used as it is, such as one without a password, with an invalid
       option, belonging to a team, or naming a source type that no installed package serves;
       ``unavailable`` if a secrets backend failed.
   * - ``source_type``
     - The source type that the connection names, or null if the connection was not read that far.
   * - ``source_type_status``
     - ``ok`` if an installed package serves the source type; ``conflict`` if more than one package
       declares it and ``[webhooks] source_type_owners`` does not name a package for it; ``unknown`` if no
       package serves it for another reason, such as none declaring it, or ``source_type_owners`` naming a
       package that does not declare it (see `Source types`_). Null when ``source_type`` is.
   * - ``last_received_at`` and ``last_outcome``
     - When the webhook's last recorded delivery was received, and its outcome, or null if it has none.

The Core API Server works out these statuses from its own ``[webhooks]`` options, installed providers and
secrets backends, not from those of the webhooks process, so give both processes the same for the overview
to show what the webhooks process serves. It reads the connection of each webhook on the page from the
secrets backends at every call, without a cache. It never returns what a connection holds or why a secrets
backend failed, and answers 500 if its ``[webhooks]`` options are invalid: ``airflow webhooks check`` says
why.

.. _inbound-webhooks-task-waits:

A task's webhook waits
~~~~~~~~~~~~~~~~~~~~~~

:doc:`/authoring-and-scheduling/webhook-waits` describes how tasks wait for deliveries.

The **Details** tab of a task instance shows the newest webhook wait of the selected try, which is the
try's open wait while it has one: its webhook, linked to the webhook's deliveries; its status, with a
sentence on what it means; the events and the match that it accepts; its deadline; when it was registered
and, once it closed, when it ended; and, once a delivery matched it, a link that opens that delivery's
details. A try has at most one open wait, since registering a wait ends the try's earlier waits that no
delivery has resumed. If the try is no longer running or parked while its wait is still ``registered`` or
``received``, the panel says that no delivery will resume it.

``GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/webhookWaits`` lists the waits
of the current try of a task instance, newest first, or those of the try that ``try_number`` names. It
also takes ``map_index``, ``region_id`` and ``region_index``, which address the task instance as the task
instance routes do, and ``limit`` and ``offset``. It requires permission to read the task instance, not
the webhook's connection, and never returns the payload of a delivery. Each wait has these fields:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Value
   * - ``id``
     - The wait's id, which the ``waits`` of a delivery's ``actions`` name as ``wait_id``.
   * - ``webhook``
     - The webhook's name.
   * - ``events`` and ``match``
     - The delivery events and attributes that the wait accepts, as the task gave them; ``events`` is null
       for any event.
   * - ``status``
     - ``registered`` until a matching delivery arrives; ``received`` if one arrived before the task
       parked: while the task was running, or before the wait was registered, when Airflow kept the
       delivery for waits (``early``, or ``cancel_requested`` if it requested a cancel); the task resumes
       with it once it parks; ``resumed`` once a delivery resumed the task; ``timed_out`` if none arrived
       before the deadline; ``ended`` if the task registered another wait, if a webhook cancel request
       ended its task (see :doc:`/authoring-and-scheduling/webhook-cancel`), or if a wait registered on the
       same events and match found the wait's try no longer running or parked, or, for a ``registered``
       wait, a matching delivery did. Nothing else ends a wait whose try stopped: it keeps ``registered``
       until a matching delivery or such a registration ends it, and ``received``, since no other delivery
       is offered to it, until such a registration does.
   * - ``deadline``
     - When the wait times out if no matching delivery has arrived.
   * - ``created_at`` and ``ended_at``
     - When the wait was registered, and when it closed, or null while it is open.
   * - ``inbound_delivery_id``
     - The ``id`` of the delivery record that the wait received, or null. The delivery's details open at
       ``/webhook_deliveries/<webhook>?id=<inbound_delivery_id>`` for users who may read the webhook's
       deliveries.

Why a Dag did not run
~~~~~~~~~~~~~~~~~~~~~

A webhook's deliveries say why a delivery started no run:

* The delivery is not listed: Airflow refused it, or it never reached Airflow. The sender's own record of
  its deliveries, such as the **Recent Deliveries** page of a GitHub webhook, shows what Airflow answered,
  and the webhooks process logs why it refused a delivery (see `How a delivery is processed`_). The
  **Webhooks** tab shows whether the webhook is listed, and whether its connection and source type can be
  served.
* ``no_subscribers``: no Dag schedules on any of the topic names that the delivery offered, in a way that
  receives topic events (see :doc:`/authoring-and-scheduling/webhook-topics`), and no asset route recorded
  an event (see below). Compare those names with the name of the Dag's topic.
* ``dag_paused``: only paused or draining Dags schedule on its names. Unpausing a Dag does not replay the
  delivery, but the sender can deliver it again.
* ``early``, ``cancel_requested`` or ``expired``: the delivery has a correlation key and recorded no event: no
  Dag that an event could start schedules on its names, or only paused or draining ones do, and no asset route
  recorded an event. Unpausing a Dag does not replay the delivery, but the sender can deliver it again, and
  the repeat is processed again in full, unless a webhook wait has taken the delivery since.
* ``accepted``, or ``resumed_wait`` with events: it recorded the topic events and asset route events that
  its details link to, which queue the Dags scheduled on their assets. A Dag that has no run yet may be
  waiting for an event on another asset, as with ``a & b``, or, with a partitioned timetable, for the same
  partition of its other assets, or for one of its ``max_active_runs`` to finish.

When a Dag scheduled on the asset of an asset route did not run, the delivery's ``skipped_asset_routes``,
which the details panel lists as **Skipped Asset Routes**, says why, unless the delivery did not offer the
route's topic, which its topic names show:

* ``asset_not_active``: no Dag declares an asset with the route's ``asset`` as its name, or as its URI
  as Airflow stores it, which can differ from the URI in the Dag's code (see `Asset routes`_).
* ``asset_is_topic``: the asset is a webhook topic, which receives events only from its own webhook.
* ``dag_has_team``: :ref:`config:core__multi_team` is on, and a Dag of a team schedules on the asset.
* ``partition_key_missing``: the payload has no value, or null, at the route's ``partition_key_field``.
* ``partition_key_invalid``: the value there is not a non-blank string of at most 250 characters, or has
  U+0000, an unpaired surrogate, or a character outside the Basic Multilingual Plane.
* ``dag_paused``: only paused or draining Dags that the event could start schedule on the asset.
* ``no_subscribers``: no Dag that the event could start schedules on the asset, as when a route with a
  ``partition_key_field`` names an asset that no Dag with a partitioned timetable schedules on, or one
  without names an asset that only such Dags schedule on.

.. _inbound-webhooks-not-cancelled:

Why a run was or was not cancelled
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:doc:`/authoring-and-scheduling/webhook-cancel` describes which runs a delivery cancels. When a run that
should have been cancelled was not, the deliveries say why:

* The delivery has no correlation key, so it requested no cancel: its record shows none. A GitHub ``push``
  or ``workflow_run`` has none, and neither has a delivery to a generic source whose connection sets no
  ``correlation_key_field``, or whose field the delivery does not fill. ``airflow webhooks check`` notes a
  webhook with ``cancel_on`` conditions whose latest deliveries have no key.
* The delivery lists no cancel request for the Dag: none of the Dag's conditions, or its tasks', matched
  its event and attributes, which its record shows, or the Dag declared them only after the delivery
  arrived. ``airflow webhooks check`` lists the conditions on each webhook.
* :ref:`config:core__multi_team` is on, and the Dag has a team: no request is recorded for it, and
  ``airflow webhooks check`` reports it.
* The request is ``pending``: no scheduler has applied it yet, because none is running, or because many
  requests wait before it.
* The request is ``failed``: applying it raised an error, which its ``error`` names and the scheduler
  logged.
* The request is ``applied`` but lists no run, or not this one: the run is not about the delivery's
  correlation key. It was started by an event that a delivery to another webhook, or with another key,
  recorded, or by an event that the REST API, a trigger or a task created; it was triggered without the key in
  its ``conf``, under another conf key than the condition's ``correlation_key_conf``, or with a condition that
  names none; it was triggered after the delivery was received, or, if only its ``conf`` makes it about the
  key, cleared after it, or started from an event newer than the delivery; or it had already finished.
* The request is ``applied`` but lists no run, and the run's only unfinished tasks were teardowns when the
  scheduler applied it: a cancel leaves teardowns to run, so it changed nothing, and the run ends as its tasks
  make it end.
* The request lists the run under the runs it left alone, with the reason ``dag_not_found``, when the
  scheduler could not load the run's Dag version, or ``dag_has_team``.

A run that was cancelled by mistake can be cleared, which queues it again, as for any failed run. Clearing it
does not cancel it again once the request that cancelled it is ``applied``. A run that the scheduler cancelled
as it created it, while the request was still ``pending``, is cancelled again if it is cleared before the
scheduler applies that request, which takes longer when many requests wait.

.. _inbound-webhooks-audit-log:

Audit log
~~~~~~~~~

A delivery that records topic events, asset route events or cancel requests also writes an audit log
entry, in the same transaction, with the event ``webhook_delivery``, the owner ``webhook:<name>``, and in
its extra the delivery id, the topic names it recorded events on as ``names``, its outcome, ``accepted``,
``resumed_wait`` or ``cancel_requested``, if its asset routes recorded events, the ``topic`` and
``asset`` of each such route as ``asset_routes``, and, if it recorded cancel requests, the
``request_id``, ``dag_id`` and ``scope`` of each as ``cancel_requests``. A repeat of a delivery writes an
entry only if it records something new, and lists only the cancel requests that it recorded itself. The
entry has no Dag, so only users allowed to read audit log entries that belong to no Dag see it, such as
the **admin** role of the Simple auth manager.

A delivery that goes to webhook waits writes one more entry for each wait, with the same event and owner,
the task instance that registered the wait, and in its extra the delivery id, the outcome
``resumed_wait`` and the wait's id as ``wait_id``. A wait that gets an ``early`` or ``cancel_requested``
delivery when it is registered writes the same entry. Users allowed to read the audit log of the task's
Dag see it.

The scheduler writes an entry with the event ``webhook_cancel`` and the owner ``webhook:<name>`` for each
run that a cancel request cancels, with the run's Dag id and run id, and for each task instance that a
task's ``cancel_on`` ends, with the task instance. Its extra has the delivery id, the sender's or else the
id of Airflow's record of the delivery, the request's id as ``request_id``, the ``scope``, ``run`` or
``task``, and for a task instance the ``state`` it ended in. Users allowed to read the audit log of the
Dag see these entries. A task instance that a run's cancel ends gets no entry of its own: the run's entry
covers it.

.. _inbound-webhooks-metrics:

Metrics
-------

When :doc:`metrics <logging-monitoring/metrics>` are enabled, the webhooks process emits the first three of
these metrics, and the scheduler ``webhooks.open_waits`` and ``webhooks.cancels``:

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Name
     - Type
     - Description
   * - ``webhooks.deliveries``
     - counter
     - Verified deliveries answered 200 or 202, handshakes excluded, tagged ``webhook`` and
       ``outcome``, ``duplicate`` included.
   * - ``webhooks.rejections``
     - counter
     - Refused requests, tagged ``webhook`` and ``status``, the status code answered. ``webhook`` is
       ``other`` for a name that is not listed, so that anyone sending requests cannot add series.
   * - ``webhooks.delivery_duration``
     - timer
     - Milliseconds taken to record a verified delivery, tagged ``webhook``. Recordings that fail are not
       timed.
   * - ``webhooks.open_waits``
     - gauge
     - Open webhook waits whose task is running or parked, tagged ``webhook``, every
       :ref:`[scheduler] ti_metrics_interval <config:scheduler__ti_metrics_interval>`. A webhook left with
       none reports 0 once, and nothing more until it has one again.
   * - ``webhooks.cancels``
     - counter
     - Runs and task instances that webhook cancel requests cancelled, tagged ``webhook`` and ``scope``:
       one with the scope ``run`` for each run that a Dag's ``cancel_on`` cancelled, and one with the scope
       ``task`` for each task instance that a task's ``cancel_on`` ended. Requests that cancel nothing are
       not counted.

The tags reach OpenTelemetry, and StatsD only with a tagged format such as InfluxDB or DogStatsD; see
:doc:`logging-monitoring/metrics`. Requests under ``/webhooks`` are also counted in the API server's HTTP
request metrics. A 500 is not counted as a rejection, but is logged.

.. _inbound-webhooks-cli:

Command line
------------

``airflow webhooks check [NAME ...]``
    Checks that each webhook, by default each name that ``[webhooks] names`` lists and each webhook that a
    Dag's or a task's ``cancel_on`` names, can be served: that ``names`` lists it, and that its connection
    exists without a team, has the ``inbound_webhook`` type and a password, and configures a source type
    that an installed provider serves, with options that the source type accepts. It names the problem
    otherwise, such as a source type that two packages declare, or the standard provider release needed
    for ``hmac_json`` and ``token_header``. It prints the source type, body limit and ``store_payload`` of
    each webhook it can serve.

    It then lists which Dags and tasks each webhook cancels, one line for each condition of their
    ``cancel_on``, as in ``Webhook github cancels the runs of Dag deploy_preview on pull_request deliveries
    with action closed, repository my-org/web, and manual runs by conf key pr`` and ``Webhook github sets task
    run_e2e_tests of Dag deploy_preview to skipped on pull_request deliveries with action synchronize,
    repository my-org/web``. With :ref:`config:core__multi_team` on, a Dag with a team whose ``cancel_on``
    names the webhook is a problem, since no cancel request is recorded for it. For a webhook with conditions,
    it notes when none of its latest 100 recorded deliveries has a correlation key, since ``cancel_on`` acts
    only on deliveries that have one; the note does not change the exit code. It exits with 1 if any webhook
    has a problem.

``airflow webhooks send-test NAME --url URL --data DATA``
    Sends one delivery as a sender would, and prints the answer as
    ``{"status": <status code>, "body": <answer>}``. It exits with 1 unless the answer is a 2xx, and does not
    follow redirects. It sends the body as ``application/json``, signed with HMAC-SHA256, or, if the
    connection sets ``token_header``, with the secret in that header. ``--data`` is the body, or ``@FILE``
    for the content of a file; ``--secret``, ``--signature-header`` and ``--signature-prefix`` replace what
    the connection ``webhook_<name>`` says; ``--delivery-id`` sends an id in the connection's
    ``delivery_id_header``; and ``--header 'NAME: VALUE'``, which may be repeated, adds a header. A test
    delivery is processed like any other, so it starts the Dags that schedule on its names, and records
    cancel requests that cancel the runs about its correlation key.

``airflow providers webhook-sources``
    Lists the source types that the installed providers declare, and whether each is served.

Least privilege
---------------

The webhooks process answers anyone who can reach it, so give it only what it needs:

* The metadata database connection and access to the secrets backend that holds the ``webhook_<name>``
  connections. Keep those connections in environment variables or an external secrets backend, and
  withhold the Fernet key: it is needed only to read connections stored in the metadata database, and
  with it and ``SELECT`` on ``connection`` the process could read every connection stored there. It needs
  neither the JWT signing key (``[api_auth] jwt_secret`` or ``[api_auth] jwt_private_key_path``) nor
  ``[api] secret_key``, and does not warn when they are unset.
* In the secrets backend, read access to connections whose ids start with ``webhook_``, since to serve
  deliveries the process looks up only the connection of each listed webhook, and to the configuration
  that the process reads through ``_secret`` options, such as ``[database] sql_alchemy_conn_secret``.
* In the metadata database, these privileges, which are enough for every delivery:

  * ``SELECT`` on ``inbound_delivery``, ``asset_event``, ``asset_dag_run_queue``, ``log``, ``asset``,
    ``asset_active``, ``asset_alias``, ``asset_alias_asset``, ``dag``, ``dag_schedule_asset_reference``,
    ``dag_schedule_asset_name_reference``, ``dag_schedule_asset_uri_reference`` and ``inbound_wait``, and,
    only if a ``webhook_<name>`` connection is stored in the metadata database, on ``connection``, which
    the metadata database secrets backend reads for a connection that no earlier backend in the chain has;
  * ``INSERT`` on ``inbound_delivery``, ``asset_event``, ``asset_dag_run_queue`` and ``log``, and
    ``UPDATE`` on ``inbound_delivery``;
  * on PostgreSQL, ``USAGE`` on the sequences ``asset_event_id_seq`` and ``log_id_seq``, which number new
    asset events and log entries;
  * on MySQL, ``UPDATE`` on ``asset_dag_run_queue``, which is written with
    ``INSERT ... ON DUPLICATE KEY UPDATE``;
  * for deliveries that go to webhook waits, ``SELECT`` on ``task_instance``, ``dag_run`` and
    ``dynamic_region``, and ``UPDATE`` on ``inbound_wait`` and on ``task_instance``, which a delivery locks
    and writes to resume a parked task;
  * for deliveries with a correlation key, ``SELECT`` on ``dag_cancel_condition``, which holds the
    ``cancel_on`` conditions of Dags, and ``SELECT`` and ``INSERT`` on ``webhook_cancel_request``, which
    holds the cancel requests that deliveries record and which a repeat of a delivery reads. The process
    writes no run or task instance state for a cancel: the scheduler applies the requests. A request's id
    is a UUID, so no sequence is involved;
  * when :ref:`config:core__multi_team` is on, ``SELECT`` on ``dag_bundle``, ``dag_bundle_team`` and
    ``team``, which the team filter reads, also with ``dag`` for the ``cancel_on`` conditions of Dags with a
    team;
  * for asset routes with a ``partition_key_field``, ``SELECT`` on ``serialized_dag`` and
    ``dag_version``, ``SELECT`` and ``INSERT`` on ``asset_partition_dag_run`` and
    ``partitioned_asset_key_log``, which queue the partition-aware Dags; ``UPDATE`` on ``asset``, which
    PostgreSQL and MySQL need for the row lock taken on the asset; and on PostgreSQL, ``USAGE`` on the
    sequences ``asset_partition_dag_run_id_seq`` and ``partitioned_asset_key_log_id_seq``.

  Airflow's tests pin which tables a delivery reads and writes when ``multi_team`` is off, for topic events
  and for asset routes with a partition key, but not for deliveries that go to webhook waits, and not these
  privileges; they also check that a delivery that requests a cancel reads and writes no ``dag_run``,
  ``task_instance``, ``serialized_dag`` or ``dag_version``. A delivery that needs a privilege the process
  lacks is answered 500 or 503, depending on the database, and the error is logged.
* Only the plugins it needs. The first delivery that records an event imports every plugin installed
  where the process runs, from the plugins folder and from entry points, to find listeners, and the
  ``on_asset_changed`` and ``on_asset_event_emitted`` hooks of listener plugins then run after each such
  delivery, with whatever network access and credentials they use.

:ref:`inbound-webhooks-security` lists the risks that remain.

Turning webhooks off
--------------------

* To stop receiving every webhook, stop the webhooks process or scale it to zero. Do not empty
  ``[webhooks] names`` for this: the process then fails at startup, and an orchestrator restarts it in a
  loop. Tasks that wait for a webhook keep waiting meanwhile, until their timeout.
* To stop one webhook, remove its name from ``[webhooks] names`` and restart the process; its deliveries
  are then answered 404, which some senders count as failures. Its records stay readable. Removing it
  from the ``names`` of the API servers that serve the Execution API also refuses new waits on it.
* To keep a webhook's deliveries from starting runs while still recording them, pause every Dag that
  schedules on its topics or on the assets that its asset routes name. Its deliveries are then recorded
  as ``dag_paused``, or as ``early``, or ``cancel_requested`` if they request a cancel, if they have a
  correlation key, such as each ``github`` delivery about a pull request or an issue, and a sender's
  redelivery of one after unpausing is processed again, so the unpaused Dags receive it. A delivery that a
  webhook wait took, at once or while it was kept for waits, is ``resumed_wait`` instead, and a redelivery
  of it is a duplicate (see :ref:`inbound-webhooks-outcomes`).
* Pausing a Dag does not stop the deliveries that match its ``cancel_on`` from cancelling its runs. To stop
  a webhook from cancelling runs, remove the conditions on it from the ``cancel_on`` of the Dags and tasks,
  which applies once the Dag processor has parsed them, or stop the webhook as above. The scheduler still
  applies the cancel requests that deliveries recorded before.

Migrating from airflow-provider-inbound-webhooks
------------------------------------------------

The ``airflow-provider-inbound-webhooks`` package received webhooks with a plugin before Airflow did.
Topic names are unchanged for its ``github``, ``hmac_json`` and ``token_header`` source types, so Dags that
schedule on them keep working, as long as the values in them are ASCII: the package folded other
characters to ASCII, such as ``Café`` to ``Cafe``, while Airflow offers no name for a part that has
them. Airflow 3.5 has no ``gitlab`` or ``jira`` source type, and the package serves those only through
its plugin, which Airflow skips. Until a release of the package declares them as
webhook sources, or a provider ships them, keep a deployment that receives them on an earlier Airflow
version, or move those senders to a generic source type that can verify them, and Dags to the topic names
that it builds.

To move to Airflow's inbound webhooks:

* Uninstall the package:

  * Airflow reserves ``/webhooks``. It skips every plugin FastAPI app whose ``url_prefix`` starts with
    ``/webhooks``, such as the package's or one at ``/webhooks-legacy``, and logs
    ``Plugin ... attempted to use reserved url_prefix ...`` for it.
  * The package registers the ``inbound_webhook`` connection type too. Its name sorts before the standard
    provider's, so while it is installed, its connection form is the one that the UI shows.
  * In versions of the package that add an ``airflow webhooks`` command group, every ``airflow`` command
    fails with ``CliConflictError``, since Airflow has a group of that name.

* Move the ``[inbound_webhooks]`` options to ``[webhooks]`` by hand, renaming ``webhook_names`` to
  ``names``. Airflow does not read the old section, and ``names`` does not accept ``*``.
* The package copies every payload into topic events by default, while Airflow leaves out a payload larger
  than ``[webhooks] max_event_payload_bytes``, 65536 bytes by default. Dags that read large payloads, such
  as those of GitHub ``push`` and ``pull_request`` deliveries, can then find ``payload`` null. Set
  ``max_event_payload_bytes`` to ``0`` to copy every payload, as the package does by default.
* ``[inbound_webhooks] dag_checks`` has no counterpart. Airflow always refuses a ``WebhookTopic`` that a
  task declares as an inlet or an outlet, or that a ``PartitionedAssetTimetable`` schedules on, and does
  not refuse topics combined with ``|``.
* Import ``WebhookTopic`` from ``airflow.sdk`` rather than from ``airflow_inbound_webhooks``. Give the
  branch of a push as its ``action``, as in
  ``WebhookTopic(webhook="github", path="my-org/web", event="push", action="main")``: Airflow's
  ``WebhookTopic`` raises ``ValueError`` for a ``detail`` without an ``action``, which the package's
  accepted as ``event="push", detail="main"``. Both build the same name.
* Make sure each webhook's connection has the ``inbound_webhook`` type, which is the only type served, and
  has no team.
* Run ``airflow webhooks check`` and start an API server with ``--apps webhooks``.

The package's delivery records are not copied to Airflow's, so a sender's retry of a delivery received
just before the switch is processed again. Other changes:

* The 202 body holds only ``delivery_id`` and ``outcome``; the topic names a delivery offered are in its
  record.
* The extra of topic events holds the keys described in :doc:`/authoring-and-scheduling/webhook-topics`,
  without the package's ``source`` and ``path``.
* Deliveries are listed under **Browse → Webhook Deliveries** in the UI and with
  ``GET /api/v2/webhooks/{webhook_name}/deliveries``, neither of which shows payloads, and removed with
  ``airflow db clean``. ``airflow webhooks`` has only ``check`` and ``send-test``.
* The source label of topic events in the UI reads **Webhook**, and links to the delivery's details.
