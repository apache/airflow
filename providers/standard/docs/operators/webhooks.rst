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



.. _howto/operator:WaitForWebhookOperator:

Waiting for a Webhook Delivery
==============================

|experimental|

Use :class:`~airflow.providers.standard.operators.webhooks.WaitForWebhookOperator` to pause a Dag run
until a service, such as GitHub, sends a matching delivery to one of Airflow's inbound webhooks. The
task registers a wait with the API server and parks in the ``awaiting_input`` state, which holds no
worker slot, no pool slot and no trigger, so no triggerer has to run. The first delivery to the webhook
that matches the wait resumes the task, and the task returns that delivery as its XCom.

Requirements
------------

The operator needs Airflow 3.5 or later. On an earlier version, importing
``airflow.providers.standard.operators.webhooks`` raises ``AirflowOptionalProviderFeatureException``.

The webhook must be served: it is listed in the ``[webhooks] names`` option, its ``webhook_<name>``
connection exists, and its sender delivers to an API server started with ``webhooks`` in its
``--apps``. :ref:`apache-airflow:inbound-webhooks` describes the set-up, and a source type's page, such
as the GitHub provider's, describes how to connect its service.

A task fails without waiting when the API server cannot hold webhook waits, with
``WebhookWaitUnsupported``. The operator has no other way to wait, such as polling. In multi-team mode,
a task of a Dag that has a team cannot wait for a webhook either: the API server refuses its wait, and
the task fails.

Matching a delivery
-------------------

``events`` lists the delivery events the wait accepts, such as ``["pull_request"]``, or is None to accept
any event. ``match`` maps attribute names of the webhook's source type to a value, or to a list of values
of which one must match, and the delivery must match every name in it. Values compare as strings, so
``4242`` and ``"4242"`` match the same deliveries. The GitHub source type's attributes are, for
example, ``event``, ``action``, ``repository``, ``organization``, ``label``, ``branch`` and ``number``.

``webhook``, ``events`` and ``match`` are templated: Jinja templates and XComArgs in them are resolved
when the task runs, and an XComArg in them makes its task upstream of the wait.

Within a webhook, only one running or parked task instance can wait with the same ``events`` and
``match``. The API server refuses a second such wait with 409, and its task fails. A Dag that needs the
same delivery in several tasks can schedule on a webhook topic instead.

A delivery that arrives before its wait is registered is kept for it if the delivery has a correlation
key, such as each GitHub delivery about a pull request or issue, went to no other wait and recorded no
topic or asset route events. Its outcome is then ``early``, and a wait that matches it and is registered
within ``[webhooks] early_delivery_ttl`` of its receipt takes it: the task resumes as soon as it parks. A
delivery that recorded topic or asset route events for a Dag is not kept, so a wait registered after it
never gets it. :ref:`apache-airflow:inbound-webhooks-outcomes` describes the outcomes.

Timeout
-------

``timeout`` is how long the task waits, at most 365 days. When it is None, the API server's
``[webhooks] default_wait_timeout`` option applies; when that is not set either, the API server refuses
the wait and the task fails. The task's ``execution_timeout`` does not run while it waits, so the timeout
is the only thing that ends a wait no delivery matches.

Once the timeout passes, the scheduler resumes the task, which fails with
:class:`~airflow.providers.standard.exceptions.WebhookWaitTimeoutError` and the message
``No matching delivery arrived before <deadline>``. A retry of the task registers a new wait, with a new
timeout.

The returned XCom
-----------------

The task returns the delivery that resumed it:

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

``delivery_id`` is the sender's id for the delivery, or None if it has none, and ``received_at`` is when
Airflow received it, as an ISO 8601 string. ``payload`` is what the webhook's ``store_payload`` keeps of
the delivery: the payload as parsed from the body with ``full``, the parts the source type keeps with
``summary``, or None with ``none``. A key in it that serde reserves for its own encoding, such as
``__id__``, gets one more leading underscore, as ``___id__``.

.. warning::

    The payload is stored in the task's XCom, so everyone who can read the task's XCom can read it, and
    a large payload makes a large XCom. The webhook connection's ``store_payload`` and
    ``max_body_bytes`` bound it: use ``summary`` or ``none`` for senders whose payloads carry sensitive
    data, such as personal data, and a ``max_body_bytes`` that your XCom backend can hold.

Example: a merge gate
---------------------

This Dag opens a pull request, waits up to three days for it to be closed, and applies the change only
if the pull request was merged. It needs the ``github`` webhook connected to the repository with
"Pull requests" events, and a ``github_default`` connection that can open pull requests.

.. code-block:: python

    from datetime import timedelta

    from airflow.providers.standard.operators.webhooks import WaitForWebhookOperator
    from airflow.sdk import dag, task

    REPOSITORY = "my-org/configs"


    @dag(schedule=None)
    def merge_gate():
        @task
        def open_pr() -> int:
            from airflow.providers.github.hooks.github import GithubHook

            repo = GithubHook().get_conn().get_repo(REPOSITORY)
            pull_request = repo.create_pull(
                base="main", head="update-configs", title="Update configs", body="Opened by Airflow"
            )
            return pull_request.number

        wait_for_merge = WaitForWebhookOperator(
            task_id="wait_for_merge",
            webhook="github",
            events=["pull_request"],
            match={"repository": REPOSITORY, "number": open_pr(), "action": "closed"},
            timeout=timedelta(days=3),
        )

        @task
        def apply(delivery: dict) -> str:
            pull_request = delivery["payload"]["pull_request"]
            if not pull_request["merged"]:
                raise RuntimeError(f"Pull request {pull_request['number']} was closed without merging")
            return pull_request["merge_commit_sha"]

        apply(wait_for_merge.output)


    merge_gate()

``open_pr()`` in ``match`` makes ``open_pr`` upstream of ``wait_for_merge``, and its pull request number
is resolved when ``wait_for_merge`` runs. GitHub sends ``closed`` both when a pull request is merged and
when it is closed without merging, so ``apply`` reads ``merged`` from the payload, which the webhook's
``store_payload`` must keep: ``full`` and the GitHub source type's ``summary`` both do.
