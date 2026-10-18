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
starts a run of each such Dag.

This page is for Deployment Managers. Dag authors subscribe to deliveries as described in
:doc:`/authoring-and-scheduling/webhook-topics`, and provider authors write source types as described in
the Webhook Sources page of the providers documentation.

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
   sends are listed at ``/webhook_deliveries/demo`` in the Airflow UI.

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
     - Days a delivery's record is kept before ``airflow db clean`` may remove it, at most 365.
   * - ``rate_limit_per_minute``
     - ``0``
     - Verified deliveries per minute that each process takes for each webhook; ``0`` is no limit.
   * - ``store_payload``
     - ``full``
     - What is kept of a delivery's payload: ``full``, ``summary`` or ``none``.
   * - ``max_event_payload_bytes``
     - ``65536``
     - Largest payload copied into a topic event's extra; ``0`` copies every payload.

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
* The rest of its extra holds the options that the source type documents, such as ``signature_header``
  for ``hmac_json``.

Whoever may edit ``webhook_<name>`` administers the webhook: they can change its secret and its source
type, and so decide which deliveries Airflow accepts as coming from that sender.

Each process caches a webhook's connection for ``[webhooks] connection_cache_ttl`` seconds, including the
failure to find or use it, so a changed connection can take that long to apply. A changed secret applies
sooner: a delivery that fails verification has the connection read again, at most once per
``connection_cache_ttl`` for each webhook, and is verified against what that finds. To rotate a secret,
change it in the connection and then in the sender, within ``connection_cache_ttl`` of each other. The
first delivery signed with the new secret then usually passes, and deliveries signed with the old one pass
until the cached connection is read again or expires. A failure to read a secrets backend is never cached.

Webhooks have no team in Airflow 3.5. ``[webhooks] names`` refuses a name qualified with a team, such as
``team:name``, the connection is looked up without a team, so a connection that belongs to a team is not
found, and when :ref:`config:core__multi_team` is on, topic events queue only Dags that have no team.

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
8. One transaction records the delivery, its topic events and, if it recorded any, an audit log entry.
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

The delivery contract follows from this order. A 202 means that the delivery's record, and whatever topic
events and audit log entry it recorded, committed before the answer was sent; a 200 ``duplicate`` means
that an earlier copy's did; a 200 handshake records nothing. Asset listeners and metrics run after the
commit. A 5xx records nothing, so a sender that retries can deliver again once the cause is fixed. Some
senders, such as Jira Cloud, retry on their own, but GitHub does not, so a GitHub delivery refused during
an outage has to be sent again from the webhook's **Recent Deliveries** page in GitHub. Delivery is at
least once and unordered.

Outcomes and repeated deliveries
--------------------------------

A recorded delivery has one of these outcomes:

* ``accepted``: it recorded topic events, for at least one Dag that is not paused.
* ``dag_paused``: only Dags that are paused or draining schedule on its names, so it recorded nothing.
  Unpausing a Dag does not replay it.
* ``no_subscribers``: no Dag schedules on its names, so it recorded nothing.

``duplicate`` is only ever answered, never recorded. A delivery repeats a recorded one when it is sent to
the same webhook with the same delivery id and the same body; the body counts, because senders send the
delivery id in a header that their signature does not cover. A repeat of an ``accepted`` delivery is
answered 200 ``duplicate`` and records nothing. A repeat of a delivery that recorded nothing, with outcome
``no_subscribers`` or ``dag_paused``, is processed again, without a second record, so that a Dag that
subscribed or was unpaused since then receives it. If two copies of a delivery arrive at once, at most one
of them acts. A delivery without a delivery id, or with an empty one, cannot be recognized as a repeat,
so each copy of it acts.

The generic sources read the delivery id from the header or field that the connection's
``delivery_id_header`` or ``delivery_id_field`` names, and other source types from where their sender puts
it, such as ``X-GitHub-Delivery``. The ``hmac_json`` signature covers the body only, so whoever captures a
delivery can send it again under a new id in ``delivery_id_header``, and it acts again. If the sender puts
its delivery id in the signed body, use ``delivery_id_field`` instead: a copy sent again then always
repeats the delivery, as long as its record is kept.

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

Only ``accepted`` deliveries keep a payload, in their record and in the extra of each of their topic
events, so a sender that sends every event does not fill the database. Each topic event carries its own
copy, and the copy is left out of a topic event, with ``payload`` null and ``payload_truncated`` true, when
it is larger than :ref:`[webhooks] max_event_payload_bytes <config:webhooks>` as compact JSON; the
delivery's record keeps it all the same.

Every user allowed to read asset events can read their extra, and every Dag scheduled on a topic receives
it. When the payloads must not keep personal or otherwise sensitive data, set ``store_payload`` to
``none``, or to ``summary`` once you have checked what the source type's summary keeps.

Retention
---------

Each delivery's record has a ``retain_until`` time: the time it was received plus
:ref:`[webhooks] delivery_retention <config:webhooks>` days. ``airflow db clean`` removes records by that
column rather than by the time they were received, so a ``--clean-before-timestamp`` in the past never
removes a record inside its retention, while one in the future does. A changed ``delivery_retention``
applies to deliveries received after the change. Nothing else removes records, so run ``airflow db clean``
regularly, as for the other tables it cleans; ``--yes`` skips its confirmation prompt, as a scheduled run
needs. Unless you pass ``--skip-archive``, it keeps the records it removes, payloads included, in an archive
table:

.. code-block:: bash

    airflow db clean --tables inbound_delivery --yes \
        --clean-before-timestamp "$(date -u '+%Y-%m-%dT%H:%M:%S+00:00')"

Keep ``delivery_retention`` longer than the longest time over which any sender you configure retries a
delivery: a retry that arrives after its delivery's record was removed is not recognized as a repeat, and
acts again. Topic events and audit log entries are removed with their own tables.

Viewing deliveries
------------------

Every recorded delivery can be listed, without its payload, with when it was received, its source type,
attributes, outcome, the topic names it offered, and the topic events it recorded:

* in the UI, at ``/webhook_deliveries/<name>``, which the **Connections** page links to from each
  ``inbound_webhook`` connection stored in the metadata database;
* through the REST API, at ``GET /api/v2/webhooks/{webhook_name}/deliveries``, newest first, with
  ``limit`` and ``offset``, ``order_by`` (``received_at``, ``id``, ``outcome`` or ``delivery_id``), and the
  filters ``outcome``, ``delivery_id``, ``id``, ``correlation_key`` and ``received_at_gte``,
  ``received_at_gt``, ``received_at_lte`` and ``received_at_lt``.

Both require permission to read the connection ``webhook_<name>``, which the **op** and **admin** roles
of the Simple auth manager, and the ``Op`` and ``Admin`` roles of the FAB auth manager, have. A name that
neither the Core API Server's ``[webhooks] names`` lists nor any record has is answered 404.

A delivery that records topic events also writes an audit log entry, in the same transaction, with the
event ``webhook_delivery``, the owner ``webhook:<name>``, and the delivery id, the topic names it recorded
events on and its outcome in its extra. The entry has no Dag, so only users allowed to read audit log
entries that belong to no Dag see it, such as the **admin** role of the Simple auth manager.

Metrics
-------

The webhooks process emits these metrics when :doc:`metrics <logging-monitoring/metrics>` are enabled:

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

The tags reach OpenTelemetry, and StatsD only with a tagged format such as InfluxDB or DogStatsD; see
:doc:`logging-monitoring/metrics`. Requests under ``/webhooks`` are also counted in the API server's HTTP
request metrics. A 500 is not counted as a rejection, but is logged.

Command line
------------

``airflow webhooks check [NAME ...]``
    Checks that each webhook, by default each name that ``[webhooks] names`` lists, can be served: that
    ``names`` lists it, and that its connection exists without a team, has the ``inbound_webhook`` type and
    a password, and configures a source type that an installed provider serves, with options that the
    source type accepts. It names the problem otherwise, such as a source type that two packages declare,
    or the standard provider release needed for ``hmac_json`` and ``token_header``. It prints the source
    type, body limit and ``store_payload`` of each webhook it can serve, and exits with 1 if any webhook has
    a problem.

``airflow webhooks send-test NAME --url URL --data DATA``
    Sends one delivery as a sender would, and prints the answer as
    ``{"status": <status code>, "body": <answer>}``. It exits with 1 unless the answer is a 2xx, and does not
    follow redirects. It sends the body as ``application/json``, signed with HMAC-SHA256, or, if the
    connection sets ``token_header``, with the secret in that header. ``--data`` is the body, or ``@FILE``
    for the content of a file; ``--secret``, ``--signature-header`` and ``--signature-prefix`` replace what
    the connection ``webhook_<name>`` says; ``--delivery-id`` sends an id in the connection's
    ``delivery_id_header``; and ``--header 'NAME: VALUE'``, which may be repeated, adds a header. A test
    delivery is processed like any other, so it starts the Dags that schedule on its names.

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
    ``dag_schedule_asset_name_reference`` and ``dag_schedule_asset_uri_reference``, and, only if a
    ``webhook_<name>`` connection is stored in the metadata database, on ``connection``, which the
    metadata database secrets backend reads for a connection that no earlier backend in the chain has;
  * ``INSERT`` on ``inbound_delivery``, ``asset_event``, ``asset_dag_run_queue`` and ``log``, and
    ``UPDATE`` on ``inbound_delivery``;
  * on PostgreSQL, ``USAGE`` on the sequences ``asset_event_id_seq`` and ``log_id_seq``, which number new
    asset events and log entries;
  * on MySQL, ``UPDATE`` on ``asset_dag_run_queue``, which is written with
    ``INSERT ... ON DUPLICATE KEY UPDATE``;
  * when :ref:`config:core__multi_team` is on, ``SELECT`` on ``dag_bundle``, ``dag_bundle_team`` and
    ``team``, which the team filter reads.

  Airflow's tests pin which tables a delivery reads and writes when ``multi_team`` is off, but not these
  privileges. A delivery that needs a privilege the process lacks is answered 500 or 503, depending on the
  database, and the error is logged.
* Only the plugins it needs. The first delivery that records topic events imports every plugin installed
  where the process runs, from the plugins folder and from entry points, to find listeners, and the
  ``on_asset_changed`` and ``on_asset_event_emitted`` hooks of listener plugins then run after each such
  delivery, with whatever network access and credentials they use.

:ref:`inbound-webhooks-security` lists the risks that remain.

Turning webhooks off
--------------------

* To stop receiving every webhook, stop the webhooks process or scale it to zero. Do not empty
  ``[webhooks] names`` for this: the process then fails at startup, and an orchestrator restarts it in a
  loop.
* To stop one webhook, remove its name from ``[webhooks] names`` and restart the process; its deliveries
  are then answered 404, which some senders count as failures. Its records stay readable.
* To keep a webhook's deliveries from starting runs while still recording them, pause every Dag that
  schedules on its topics. Their deliveries are recorded as ``dag_paused``, and a sender's redelivery
  after unpausing is processed again.

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
* Deliveries are listed with ``GET /api/v2/webhooks/{webhook_name}/deliveries`` and in the UI, and removed
  with ``airflow db clean``. ``airflow webhooks`` has only ``check`` and ``send-test``.
* The source label of topic events in the UI reads **Webhook**, and links to the delivery.
