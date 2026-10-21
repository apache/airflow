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



.. _howto/connection:inbound_webhook:

Inbound Webhook Connection
==========================

|experimental|

The Inbound Webhook connection type configures a webhook that Airflow receives. The connection
``webhook_<name>`` configures the webhook ``<name>``: its source type verifies each delivery with the
connection's shared secret and describes it.

Inbound webhooks need Airflow 3.5 or later. On earlier versions the connection type may still appear
in the connection form, but nothing reads such a connection.

Configuring the Connection
--------------------------

Shared secret (required)
    The secret that the sender signs its deliveries with, or sends with them as a token.

Extra (optional)
    ``source_type``: How deliveries are verified and named. Defaults to the webhook's name, so
    ``webhook_github`` uses the ``github`` source type. ``airflow providers webhook-sources`` lists the
    installed source types.

    ``store_payload``: What is kept of each delivery's payload, in the delivery record, in the extra of
    its events and in the event that a webhook wait resumes its task with, which
    :ref:`WaitForWebhookOperator <howto/operator:WaitForWebhookOperator>` returns as its XCom: ``full``,
    the payload as parsed from the body; ``summary``, the parts the source type keeps; or ``none``.
    Defaults to the source type's default, or else to the ``[webhooks] store_payload`` option, which is
    what ``hmac_json`` and ``token_header`` use. It does not cover the delivery's attributes, including
    those from ``attribute_paths``, its correlation key, or the partition keys that its asset routes read:
    the delivery record keeps them whatever it is set to, the extra of each of its events carries the
    correlation key, and each asset route event, and the Dag run it starts, carries its partition key. So
    do not map personal data into ``attribute_paths``, ``correlation_key_field`` or
    ``partition_key_field``.

    ``max_body_bytes``: Larger deliveries are refused, in bytes. It replaces the
    ``[webhooks] max_body_bytes`` option, higher or lower, so anyone who can edit the connection can
    raise the limit. The option applies when it is not set.

    ``asset_routes``: Rules that also record an event on a data asset for a delivery, read by Airflow
    rather than by the source type, as described in :ref:`apache-airflow:inbound-webhooks-asset-routes`.
    A JSON list of objects, or that list as JSON text, with these keys, each a non-empty string:

    * ``topic`` (required): one of the topic names that this webhook's deliveries offer, such as
      ``etl/orders/load.completed``. A delivery that offers this name records an event on the asset.
    * ``asset`` (required): the URI of an active asset, or else its name, exactly as Airflow stores it.
    * ``partition_key_field``: a dotted path into the body to the event's partition key. Without it, the
      event has no partition key.

    Any other key, a ``partition_key_field`` of null, or any other shape leaves the connection
    misconfigured: its deliveries are answered 503, and ``airflow webhooks check`` says what is wrong.

    A route records an event only for a Dag that can run for it: one that schedules on the asset and is
    not paused, draining or stale. An event with a partition key starts only the Dags with a partitioned
    timetable, and one without starts only the Dags without one. The partition key must be a non-blank
    string of at most 250 characters, with no U+0000 and no character outside the Basic Multilingual
    Plane, such as an emoji; if the field is missing or null the route is skipped as
    ``partition_key_missing``, and if it holds anything else, as ``partition_key_invalid``. A route that
    a delivery offers the topic of but that records nothing is listed, with the reason, in the
    ``skipped_asset_routes`` of the delivery record's actions.

    A route needs only edit rights on the connection, while recording an asset event through the REST API
    needs permission to create asset events. So anyone who can edit the connection can have its
    deliveries record events on any active asset that is not a webhook topic, and start the Dags
    scheduled on it.

    The other keys are the source type's options. This provider ships two source types, for services
    that let you configure where they put the event name and the signature or token.

    ``hmac_json``, for services that sign the raw body with HMAC-SHA256:

    * ``signature_header`` (required): the header carrying the signature.
    * ``signature_prefix``: text before the digest in the signature header, such as ``sha256=``.
    * ``signature_encoding``: ``hex`` (default) or ``base64``.

    ``token_header``, for services that can only send the shared secret in a header. A token proves who
    sent a delivery but not that its body is unmodified, so integrity depends on TLS.

    * ``token_header`` (required): the header carrying the shared secret.
    * ``token_prefix``: text before the secret in the token header, such as ``Bearer`` and a space.

    Both read the JSON body's event and path, the delivery ID the service repeats when it retries a
    delivery, and optionally more attributes and a correlation key. Fields are dotted paths into the
    body, such as ``data.type``.

    * ``event_field`` or ``event_header`` (exactly one is required): where the event name is.
    * ``path_field``: where the path is, such as a service or project name.
    * ``delivery_id_field`` or ``delivery_id_header``: where the delivery ID is.
    * ``attribute_paths``: a JSON object, or that object as JSON text, that maps attribute names to
      fields, such as ``{"job_id": "data.job.id"}``. A name has at most 64 lowercase letters, digits and
      underscores, starts with a letter, and is neither ``event`` nor ``path``. Any other shape leaves
      the connection misconfigured.
    * ``correlation_key_field``: where the correlation key is, the value that says what the delivery is
      about, such as a job run's ID, so that deliveries about the same thing can be found together.

    Their attributes are ``event``, ``path`` when the delivery has one, and each attribute in
    ``attribute_paths`` whose field holds a non-empty string or an integer in the signed 64-bit range. A
    field that is missing, or holds anything else, such as ``true``, ``1.5``, a larger integer, an object
    or a list, leaves its attribute out. Each delivery record keeps the delivery's attributes. A
    :ref:`webhook wait <apache-airflow:webhook-waits-match>` selects deliveries by these attributes, and
    compares their values as strings, so an attribute that holds ``42`` matches ``42`` and ``"42"`` alike.

    The correlation key is the value of ``correlation_key_field`` as a string, when that field holds a
    non-empty string or an integer of any size and the string has at most 255 characters; otherwise the
    delivery has none. It is kept in the delivery record and in the ``correlation_key`` of the extra of
    each topic event and asset route event. Deliveries can be filtered by it in the UI, in the
    **Deliveries** tab of **Browse → Webhook Deliveries**, and through the REST API. Webhook waits use it
    to keep deliveries that arrive before their wait: a delivery with a correlation key that records no
    topic or asset route event and goes to no wait is kept as ``early`` for
    ``[webhooks] early_delivery_ttl`` seconds, with its payload, for a wait registered later (see
    :ref:`apache-airflow:webhook-waits-early`). A wait matches attributes, not the key, so map the same
    field into ``attribute_paths`` too if waits are to match it.

    Prefer the fields when the service puts these values in the body. The ``hmac_json`` signature
    covers the body only, and a token covers nothing, so whoever captures a delivery can send its body
    again with any value in ``event_header`` or ``delivery_id_header``: under another event, it starts
    the Dags that subscribe to that event instead.

    Example "extras" field:

    .. code-block:: json

       {
          "source_type": "hmac_json",
          "signature_header": "X-Signature",
          "event_field": "event",
          "path_field": "service",
          "delivery_id_header": "X-Delivery",
          "attribute_paths": {"environment": "deployment.environment", "build": "deployment.build"},
          "correlation_key_field": "deployment.id"
       }

    With an asset route, on the connection ``webhook_etl``:

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

In a URI, such as an environment variable, write the connection type as ``inbound-webhook``, because a
URI scheme cannot contain ``_``:

.. code-block:: bash

   export AIRFLOW_CONN_WEBHOOK_DEMO='inbound-webhook://:my-secret@/?source_type=hmac_json&signature_header=X-Signature&event_field=event'

A URI's extra holds only strings, so give ``attribute_paths`` and ``asset_routes`` there as JSON text,
URL-encoded. Or use the JSON format:

.. code-block:: bash

   export AIRFLOW_CONN_WEBHOOK_DEMO='{"conn_type": "inbound_webhook", "password": "my-secret", "extra": {"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event"}}'
