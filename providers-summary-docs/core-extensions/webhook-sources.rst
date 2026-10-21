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

Webhook Sources
---------------

|experimental|

This is a summary of all Apache Airflow Community provided implementations of webhook source types
exposed via community-managed providers.

Airflow can be extended by providers with webhook source types. A webhook source type verifies and
parses the inbound webhook deliveries of one kind of sender, and is implemented by a subclass of
``airflow.webhooks.base.WebhookSource``. Each provider can declare its own source types under
``webhook-sources`` in its ``provider.yaml``.

A source type declared by more than one installed provider is served by none of them, unless
``[webhooks] source_type_owners`` (see :doc:`apache-airflow:configurations-ref`) names the package that
serves it, for example ``github=apache-airflow-providers-github``. ``airflow providers webhook-sources``
lists every declared source type and shows such conflicts, and which package ``source_type_owners`` picks.

How Airflow serves webhooks with them is described in the Inbound Webhooks page of the Airflow
documentation.

Community-provided source types
'''''''''''''''''''''''''''''''

These are the webhook source types provided by the community-managed providers:

.. airflow-webhook-sources::
   :tags: None
   :header-separator: "

Writing a webhook source type
'''''''''''''''''''''''''''''

A source type does two things: it checks that a request comes from the holder of the webhook's shared
secret, and it describes the delivery. Airflow does everything else. It serves the route, limits the body
size and the rate, builds the topic names, records the delivery and its topic events, and answers the
sender. A source type never sees assets, Dags, other webhooks' secrets or the database.

The contract
""""""""""""

A source type is a subclass of :class:`~airflow.webhooks.base.WebhookSource`:

``verify(request, secret)``
    Raise :class:`~airflow.webhooks.base.WebhookVerificationError` unless the request carries a valid
    signature or token for ``secret``, the password of the webhook's connection. Any exception from
    ``verify`` is answered 401. Use :func:`~airflow.webhooks.base.verify_hmac_sha256` or
    :func:`~airflow.webhooks.base.verify_token`, which compare in constant time and refuse an empty secret.

``parse(request)``
    Describe a verified request as a :class:`~airflow.webhooks.base.WebhookDelivery`. Raise ``ValueError``
    if the request is malformed: it is answered 400, and its message is sent to the sender, so say what is
    wrong with the request and nothing about the deployment. Any other exception is a bug, answered 500.
    A ``WebhookDelivery`` has:

    * ``path``: where the delivery happened, ``/``-separated from broad to narrow, such as ``my-org/web``,
      or None;
    * ``kinds``: what happened, a list of tuples, each from broad to specific, such as
      ``[("pull_request", "labeled", "deploy-preview")]``. Every prefix of every kind becomes a topic
      name, so one delivery can be subscribed to at several levels, and a delivery can carry several kinds;
    * ``attributes``: the values the source type declares, such as ``repository`` or ``action``, as a flat
      mapping to strings or integers. Leave out an attribute the delivery does not have; any other value
      type raises ``TypeError``, which is answered 500;
    * ``delivery_id``: the sender's id for the delivery, the same across its retries, or None. Airflow
      treats an empty id as None, and answers 400 for one longer than 255 characters or that is not
      printable ASCII;
    * ``payload``: the parsed body, which the webhook's ``store_payload`` decides what to keep of;
    * ``correlation_key``: optionally, what the delivery is about, such as a pull request or a job run, at
      most 255 characters.

``from_extra(extra)``
    Build the source type from the extra of the webhook's connection, and raise ``ValueError`` if an option
    is invalid. The webhook is then misconfigured: its deliveries are answered 503, and
    ``airflow webhooks check`` shows the message. The extra also holds Airflow's own keys, such as
    ``source_type``, so ignore keys you do not know. Keys whose value is null or an empty string, which the
    connection form saves for fields left empty, are removed first, and a connection given as a URI has
    only string values. By default, ``from_extra`` takes no options.

``summarize(payload)`` and ``default_store_payload``
    ``summarize`` returns what ``store_payload`` set to ``summary`` keeps of a payload; by default, all of
    it. Leave out personal data and large repeated objects. ``default_store_payload`` is the source type's
    own default for ``store_payload``, ``full``, ``summary`` or ``none``: the connection's ``store_payload``
    overrides it, and when it is None, the default, ``[webhooks] store_payload`` applies.

``is_handshake(request)`` and ``handshake(request)``
    For senders that check a webhook's URL when it is created, as GitHub does with ``ping``. If
    ``is_handshake`` returns True, the request is answered 200 with the JSON body that ``handshake``
    returns, nothing is recorded, and the rate limit does not count it. ``verify`` runs first. If the
    sender does not sign its handshakes, set the class attribute ``unsigned_handshake`` to True: then
    ``is_handshake`` runs before ``verify`` on requests from anyone, so it must be cheap and return False
    rather than raise for a request it cannot read, and ``handshake`` may only echo a value from the
    request. A ``ValueError`` from ``handshake`` is answered 400.

Airflow builds one instance for each webhook connection and shares it across concurrent requests, so keep
no state for a request on the instance. The helpers :func:`~airflow.webhooks.base.load_json_object`,
:func:`~airflow.webhooks.base.get_field` and :func:`~airflow.webhooks.base.get_text` read JSON bodies:
``load_json_object`` requires the ``application/json`` content type and a JSON object, and refuses with
``ValueError`` what a database cannot store, such as ``NaN`` or ``U+0000``; ``get_text`` returns None for
a field that is missing or not a non-empty string, so a delivery of an unexpected shape gets fewer names or
a 400 rather than a 500.

For example, a source type for a service that signs its body in ``X-Acme-Signature``:

.. code-block:: python

    from __future__ import annotations

    from airflow.exceptions import AirflowOptionalProviderFeatureException

    try:
        from airflow.webhooks.base import (
            WebhookDelivery,
            WebhookRequest,
            WebhookSource,
            get_text,
            load_json_object,
            verify_hmac_sha256,
        )
    except ImportError:
        raise AirflowOptionalProviderFeatureException("Inbound webhook sources need Airflow 3.5+.")


    class AcmeWebhookSource(WebhookSource):
        """
        Acme deployments.

        The path is the project's slug; the kind is the event, then the deployment's status.
        The attributes are ``event``, ``project`` and ``status``.
        """

        def verify(self, request: WebhookRequest, secret: str) -> None:
            signature = request.headers.get("X-Acme-Signature")
            verify_hmac_sha256(signature, secret, request.body, prefix="sha256=")

        def parse(self, request: WebhookRequest) -> WebhookDelivery:
            payload = load_json_object(request)
            if not (event := get_text(payload, "event")):
                raise ValueError("No event in the body")
            project = get_text(payload, "project.slug")
            status = get_text(payload, "deployment.status")
            attributes = {"event": event, "project": project, "status": status}
            return WebhookDelivery(
                path=project,
                kinds=[(event, status) if status else (event,)],
                attributes={key: value for key, value in attributes.items() if value is not None},
                delivery_id=request.headers.get("X-Acme-Delivery"),
                correlation_key=get_text(payload, "deployment.id"),
                payload=payload,
            )

No I/O
""""""

``verify``, ``parse``, ``is_handshake``, ``handshake`` and ``summarize`` run inside the API server, on bytes
from clients that nothing has authenticated yet, and while the sender waits for an answer. They must not
perform network or file I/O, and the module that holds the source type should import nothing from the
provider's client library, so that the webhooks process does not load it. A defect in this code is
reachable by anyone who can reach the webhooks process, so it is handled through the security process even
while the source type is experimental.

The naming contract
"""""""""""""""""""

Dag authors subscribe to the topic names that a source type's ``path`` and ``kinds`` produce, so these are
the provider's public interface:

* Document which field of a delivery fills the path and each part of each kind, which values the source
  type makes up, and the attributes it declares.
* Renaming or removing a part, or changing which field fills it or an attribute, is a breaking change,
  released in a major version of the provider. Adding names, such as a new kind, is not. Values that the
  sender supplies pass through unchanged, so a change on the sender's side is not a provider change.
* A Dag whose name stops matching does not fail: it stops running. So test the names: keep recorded
  deliveries, with secrets and personal data removed, as fixtures, each with the list of names it must
  offer. ``build_candidate_names`` from ``airflow.webhooks.naming`` builds the names exactly as Airflow
  does, from the webhook's name and the ``path`` and ``kinds`` that ``parse`` returns. Community providers
  can also check with ``assert_odd_values_are_parsed_or_refused`` from ``tests_common.test_utils.webhooks``
  that a delivery of any shape is parsed or refused with ``ValueError``.

Registering a source type
"""""""""""""""""""""""""

Declare the source type in ``provider.yaml``. Its name has lowercase letters, digits and ``_``, and starts
with a letter:

.. code-block:: yaml

    webhook-sources:
      - source-type: acme
        classpath: airflow.providers.acme.webhooks.acme.AcmeWebhookSource

Put each class in a module of a ``webhooks`` subpackage of the provider,
``airflow.providers.<provider>.webhooks.<module>``, rather than in its ``__init__.py``. Discovery imports
nothing: Airflow imports the class when a listed webhook first uses its source type. Do not declare a
source type that another provider ships, because a source type declared by two installed packages is
served by neither unless the Deployment Manager picks one with ``[webhooks] source_type_owners``.

Airflow versions before 3.5 never read ``webhook-sources``, so a provider does not need to raise its
minimum Airflow version to ship a source type. Guard the import of ``airflow.webhooks.base`` as in the
example above, so that importing the module on an older Airflow raises
``AirflowOptionalProviderFeatureException``, which marks an optional feature that is not available.
Community providers import it from ``airflow.providers.common.compat.sdk`` instead. Mark each source type
as experimental in its first release.
