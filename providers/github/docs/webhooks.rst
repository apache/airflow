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



.. _howto/webhooks:github:

Inbound Webhooks
================

|experimental|

The ``github`` webhook source type lets Dags run on events from GitHub.com and GitHub Enterprise
Server, such as a label added to a pull request. It is implemented by
:class:`~airflow.providers.github.webhooks.github.GitHubWebhookSource`. Airflow verifies each delivery
with the ``X-Hub-Signature-256`` signature that GitHub computes with the webhook's secret, and records
an event on each of the delivery's topic names that a Dag schedules on. A task can also wait for a
delivery, such as the one that says that a pull request was merged, with a webhook wait, as described in
:ref:`apache-airflow:webhook-waits`.

Inbound webhooks need Airflow 3.5 or later.

Connecting GitHub
-----------------

1. Serve inbound webhooks. They are opt-in: only an API server started with ``webhooks`` in its
   ``--apps``, such as ``airflow api-server --apps webhooks``, serves ``/webhooks/``, and any other
   answers 404. GitHub must reach that server over HTTPS. The inbound webhooks page of the Airflow
   documentation describes the app and the ``[webhooks]`` options.

2. Add ``github`` to the ``[webhooks] names`` option.

3. Create the connection ``webhook_github``, of the Inbound Webhook type from the standard provider,
   with a new secret as its password:

   .. code-block:: bash

      SECRET="$(openssl rand -hex 32)" && echo "$SECRET"
      airflow connections add webhook_github --conn-type inbound_webhook --conn-password "$SECRET"

   The source type defaults to the webhook's name. A second GitHub instance is a second webhook, such
   as ``ghes``, whose connection ``webhook_ghes`` has the extra ``{"source_type": "github"}``.

4. In GitHub, open the repository's **Settings**, then **Webhooks**, then **Add webhook**. To cover
   every repository of an organization, use the organization's settings instead. Set:

   * **Payload URL**: ``https://<your-airflow>/webhooks/github``.
   * **Content type**: ``application/json``. The default, ``application/x-www-form-urlencoded``, is
     refused.
   * **Secret**: the secret from step 3.
   * The events your Dags need, such as pull requests or pushes.

GitHub sends a ``ping`` when the webhook is created. Airflow answers it with 200 once its signature is
verified, and records nothing, so the webhook's **Recent Deliveries** tab shows whether the URL and
the secret work.

Topic names
-----------

A delivery to the webhook ``github`` offers names of the form
``github/<path>/<event>/<action>/<detail>``: every prefix of its event, action and detail, each with its
path and with ``*`` in place of each trailing part of the path, so ``*`` stands for anything below that
point.

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Part
     - Value
   * - path
     - The repository's full name, such as ``my-org/web``, or ``my-org/*`` for an organization event
       that concerns no repository. Names are case-sensitive, and the path is the repository's
       ``full_name`` exactly as GitHub sends it: ``path="my-org/web"`` does not match a repository that
       GitHub names ``My-Org/Web``.
   * - event
     - The ``X-GitHub-Event`` header, such as ``pull_request`` or ``push``.
   * - action
     - The payload's ``action``, such as ``labeled``. A delivery without one, such as a push, has its
       detail in this place.
   * - detail
     - The label for ``labeled`` and ``unlabeled``, the branch for ``push`` (tag pushes have none),
       and the workflow name for a ``workflow_run`` of a commit of the repository itself. A run for a
       pull request from a fork has no detail, because the fork chooses its workflow's name (see
       `Security`_).

A value with a character outside ASCII is never part of a name: a push to a branch named ``maín`` offers
``github/my-org/web/push`` and its wildcard paths, but no name for its branch, so it cannot be taken for a
push to ``main``.

A label ``deploy-preview`` added to a pull request in ``my-org/web`` offers nine names, from broad to
specific:

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

A push to the branch ``main`` of ``my-org/web`` offers six:

.. code-block:: text

   github/my-org/web/push                                   any push to my-org/web
   github/my-org/*/push                                     ... to any repository of my-org
   github/*/push                                            ... to any repository
   github/my-org/web/push/main                              a push to main
   github/my-org/*/push/main
   github/*/push/main

A Dag subscribes to a name by scheduling on a ``WebhookTopic``, and finds the payload in each
event's extra:

.. code-block:: python

   from airflow.sdk import WebhookTopic, dag, task

   preview_requested = WebhookTopic(
       webhook="github",
       path="my-org/web",
       event="pull_request",
       action="labeled",
       detail="deploy-preview",
   )


   @dag(schedule=preview_requested)
   def deploy_preview():
       @task
       def deploy(*, triggering_asset_events):
           for event in triggering_asset_events[preview_requested]:
               if (payload := event.extra.get("payload")) is None:
                   print("No payload kept for delivery", event.extra["delivery_id"])
                   continue
               print("Pull request", payload["pull_request"]["number"])

       deploy()


   deploy_preview()

An event's ``payload`` is null when ``store_payload`` keeps no payload, and when what it keeps is
larger than ``[webhooks] max_event_payload_bytes`` (64 KiB by default) as JSON, in which case the
extra also has ``payload_truncated`` set to true. Pushes and pull requests can be larger than that;
``store_payload`` set to ``summary`` (see below) makes them smaller.

As a push has no action, its branch takes the action's place: pushes to ``main`` are
``WebhookTopic(webhook="github", path="my-org/web", event="push", action="main")``.

Attributes
----------

Each delivery record also keeps the delivery's attributes, as far as the delivery has them:

* ``event``: the ``X-GitHub-Event`` header.
* ``action``: the payload's ``action``.
* ``repository``: the repository's full name, such as ``my-org/web``.
* ``organization``: the organization's login.
* ``label``: the name of the payload's label.
* ``branch``:

  * for ``pull_request``, ``pull_request_review``, ``pull_request_review_comment`` and
    ``pull_request_review_thread``, the pull request's head branch, the branch it merges from, which
    for a pull request from a fork is a branch of the fork;
  * for ``workflow_job`` and ``check_suite``, their ``head_branch``, for ``check_run``, its check
    suite's, and for a ``workflow_run`` of a commit of the repository itself, its ``head_branch``;
  * for ``create`` and ``delete``, the ``ref`` if ``ref_type`` is ``branch``;
  * for any other event, such as ``push`` and ``workflow_dispatch``, the branch that a ``ref`` of the
    form ``refs/heads/<branch>`` names. Tags have no branch.
* ``number``: the number of the pull request or issue.

A webhook wait selects deliveries by these attributes, with ``event`` filtered by its ``events`` and the
others by its ``match``, as in ``match={"repository": "my-org/web", "number": 7, "action": "closed"}``.
It compares their values as strings, so ``7`` and ``"7"`` match the same deliveries (see
:ref:`apache-airflow:webhook-waits-match`).

Correlation key
---------------

A delivery about a pull request or an issue has the correlation key ``<repository>#<number>``, such as
``my-org/web#7``: the repository's full name exactly as GitHub sends it, as in the ``repository``
attribute, and the ``number`` attribute. The repository is part of the key because every repository
numbers its pull requests and issues from 1. Pull requests and issues share one sequence of numbers per
repository, so every delivery about pull request 7 of ``my-org/web`` has the key ``my-org/web#7``,
whether it is a ``pull_request``, ``pull_request_review`` or ``pull_request_review_comment``, or an
``issue_comment`` on the pull request. A delivery without a repository or a number, such as a ``push``,
a ``workflow_run`` or an organization event, has no key. A repository that is renamed or transferred
gets new keys, as its full name changes.

The key is kept in the delivery record and in the ``correlation_key`` of each topic event's extra.
Deliveries can be filtered by it in the UI, in the **Deliveries** tab of **Browse → Webhook Deliveries**,
and through the REST API, with the ``#`` written as ``%23``:
``GET /api/v2/webhooks/github/deliveries?correlation_key=my-org/web%237``.

Webhook waits use the key to keep deliveries that arrive before the wait that they are for is registered.
A delivery with a key that records no topic or asset route event and goes to no wait is kept as ``early``
for ``[webhooks] early_delivery_ttl`` seconds, so that a task that waits for the pull request or issue it
is about still gets it, as when a merge gate's pull request is merged before the task registers its wait
(see :ref:`apache-airflow:webhook-waits-early`). A wait does not match on the key itself: its ``match``
names the ``repository`` and ``number`` attributes instead. A delivery without a key, such as a ``push``,
that arrives before its wait is not kept.

Stored payload
--------------

What is kept of a payload, in the delivery record, in the extra of topic events and in the event that a
webhook wait resumes its task with, is set by ``store_payload`` in the connection's extra, or else by the
``[webhooks] store_payload`` option, whose default keeps the whole payload. GitHub payloads carry personal
data, such as the name and email address of each commit's author in a push, and a kept payload can be read
by every user allowed to read assets, by every Dag scheduled on the topic, and by everyone who can read
the XCom of a task that waited for the delivery. A delivery about a pull request or an issue that nothing
acts on keeps its payload too, as it is kept for webhook waits, until ``airflow db clean`` removes its
record after ``[webhooks] delivery_retention`` days.

With ``store_payload`` set to ``summary``:

* Every ``email`` field is dropped, with the ``name`` beside it. These are the name and email address of
  the pusher and of each commit's author and committer; ``sender`` still names the pusher's account, and
  ``username`` each commit author's and committer's account, if GitHub knows it.
* The ``repository``, ``organization`` and ``sender`` objects, which GitHub repeats in full in its
  deliveries, keep only their ``full_name`` or ``login``, as do a pull request's ``user`` and the
  ``repo`` of its ``head`` and ``base``.

Everything else is kept, including commit messages, which can name people, such as in
``Signed-off-by`` lines. ``store_payload`` set to ``none`` keeps no payload.

Security
--------

GitHub's signature covers the body only. The ``X-GitHub-Event`` and ``X-GitHub-Delivery`` headers are
not signed, and the event in a delivery's names comes from ``X-GitHub-Event``. Airflow drops a repeated
delivery only when both its delivery id and its body are the same, so anyone who obtains one signed
body, from a log or a proxy that records requests for example, can send it again under a new delivery
id and under any event name, and so start the Dags scheduled on the names that this offers. This works
until the secret is changed. Keep delivery bodies and the secret out of logs and proxies, and if a
delivery leaks, change the secret in GitHub and in the ``webhook_github`` connection.

A valid signature shows that GitHub sent a delivery, not that the webhook's owner chose what it
says. Many values in it are chosen by other GitHub users: the head branch, title and body of a pull
request from a fork, issue and comment text, label names that anyone with triage access can apply, and
the names and head branches of workflow runs. A pull request from a fork runs the workflows of its own
commit, so its author can give a workflow any name and make it succeed; that is why such runs offer no
workflow name, but their other values, and those of ``workflow_job``, ``check_suite`` and ``check_run``
deliveries, can still come from a fork. Before a Dag acts on a delivery, such as by deploying the commit
it names, check the values it relies on in the payload, such as the run's ``head_repository`` or the
pull request's ``head.repo``.

The same holds for webhook waits: a wait whose ``match`` names a ``label`` or a ``branch`` can be resumed
by whoever can set them. Match on values that only users you trust choose, such as the ``repository`` and
``number`` of a pull request that the Dag opened, and check the payload before the task acts on it.
