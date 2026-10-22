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

    ``store_payload``: What is kept of each delivery: ``full``, the payload as parsed from the body;
    ``summary``, the parts the source type keeps; or ``none``. Defaults to the source type's default, or
    else to the ``[webhooks] store_payload`` option, which is what ``hmac_json`` and ``token_header`` use.

    ``max_body_bytes``: Larger deliveries are refused, in bytes. It replaces the
    ``[webhooks] max_body_bytes`` option, higher or lower, so anyone who can edit the connection can
    raise the limit. The option applies when it is not set.

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

    Both read the JSON body's event and path, and the delivery ID the service repeats when it retries
    a delivery. Fields are dotted paths into the body, such as ``data.type``.

    * ``event_field`` or ``event_header`` (exactly one is required): where the event name is.
    * ``path_field``: where the path is, such as a service or project name.
    * ``delivery_id_field`` or ``delivery_id_header``: where the delivery ID is.

    Their attributes are ``event``, and ``path`` when the delivery has one.

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
          "delivery_id_header": "X-Delivery"
       }

In a URI, such as an environment variable, write the connection type as ``inbound-webhook``, because a
URI scheme cannot contain ``_``:

.. code-block:: bash

   export AIRFLOW_CONN_WEBHOOK_DEMO='inbound-webhook://:my-secret@/?source_type=hmac_json&signature_header=X-Signature&event_field=event'

Or use the JSON format:

.. code-block:: bash

   export AIRFLOW_CONN_WEBHOOK_DEMO='{"conn_type": "inbound_webhook", "password": "my-secret", "extra": {"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event"}}'
