# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Inbound webhook source type for GitHub.

.. note:: |experimental|
"""

from __future__ import annotations

import json
from typing import Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.webhooks.base import (
        WebhookDelivery,
        WebhookRequest,
        WebhookSource,
        get_field,
        get_text,
        load_json_object,
        verify_hmac_sha256,
    )
except ImportError:
    raise AirflowOptionalProviderFeatureException("Inbound webhook sources need Airflow 3.5+.")

#: Objects that ``summarize`` reduces to the field that names them.
_NAMED_OBJECTS = {
    "repository": "full_name",
    "organization": "login",
    "sender": "login",
    "pull_request.user": "login",
    "pull_request.head.repo": "full_name",
    "pull_request.base.repo": "full_name",
}

#: The longest correlation key that ``WebhookDelivery`` allows.
_MAX_CORRELATION_KEY_LENGTH = 255

#: Where events other than push, create and delete name their branch.
_BRANCH_FIELDS = {
    "check_run": "check_run.check_suite.head_branch",
    "check_suite": "check_suite.head_branch",
    "pull_request": "pull_request.head.ref",
    "pull_request_review": "pull_request.head.ref",
    "pull_request_review_comment": "pull_request.head.ref",
    "pull_request_review_thread": "pull_request.head.ref",
    "workflow_job": "workflow_job.head_branch",
    "workflow_run": "workflow_run.head_branch",
}


class GitHubWebhookSource(WebhookSource):
    """
    GitHub.com and GitHub Enterprise Server; an organization webhook covers every repository.

    Kinds use GitHub's event and action names. The detail is the label for ``labeled`` and
    ``unlabeled``, the branch for ``push`` (tag pushes have none), and the workflow name for a
    ``workflow_run`` of a commit of the repository itself. A pull request from a fork runs the workflows
    of its own commit, so whoever opens one chooses the names of those runs, which therefore have no
    detail. Organization-level events have the path ``<org>/*``.

    The attributes are ``event``, and when the delivery has them ``action``, ``repository`` (its full
    name), ``organization``, ``label``, ``branch`` and ``number`` (of the pull request or issue). The
    branch is the head branch of a pull request, check suite, check run, workflow job, or workflow run of
    a commit of the repository itself, the ``ref`` of a ``create`` or ``delete`` whose ``ref_type`` is
    ``branch``, and otherwise the branch that a ``ref`` of the form ``refs/heads/<branch>`` names, as in
    ``push`` and ``workflow_dispatch``.

    A delivery about a pull request or issue has the correlation key ``<repository>#<number>``, such as
    ``my-org/web#7``; pull requests and issues share one sequence of numbers per repository.

    The ``ping`` GitHub sends when a webhook is created is a handshake, answered without recording
    anything.

    .. note:: |experimental|
    """

    def verify(self, request: WebhookRequest, secret: str) -> None:
        verify_hmac_sha256(request.headers.get("X-Hub-Signature-256"), secret, request.body, prefix="sha256=")

    def parse(self, request: WebhookRequest) -> WebhookDelivery:
        payload = load_json_object(request)
        if not (event := request.headers.get("X-GitHub-Event")):
            raise ValueError("Missing X-GitHub-Event header")
        action = get_text(payload, "action")
        detail = self.find_detail(event, action, payload)
        kind = (event, *(segment for segment in (action, detail) if segment))
        found_attributes = {
            "event": event,
            "action": action,
            "repository": get_text(payload, "repository.full_name"),
            "organization": get_text(payload, "organization.login"),
            "label": get_text(payload, "label.name"),
            "branch": self.find_branch(event, payload),
            "number": self.find_number(payload),
        }
        return WebhookDelivery(
            path=self.find_path(payload),
            kinds=[kind],
            attributes={key: value for key, value in found_attributes.items() if value is not None},
            delivery_id=request.headers.get("X-GitHub-Delivery"),
            payload=payload,
            correlation_key=self.find_correlation_key(payload),
        )

    def is_handshake(self, request: WebhookRequest) -> bool:
        return request.headers.get("X-GitHub-Event") == "ping"

    def handshake(self, request: WebhookRequest) -> dict[str, Any]:
        # GitHub sends the ping in the content type the webhook is set up with, so a webhook that
        # would deliver forms is refused when it is created rather than on its first event.
        load_json_object(request)
        return {}

    def summarize(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Return the payload without personal data and with the objects GitHub repeats reduced to names.

        Every ``email`` field is dropped with the ``name`` beside it, which are the name and email address
        of the pusher and of each commit's author and committer. The repository, organization and sender,
        and a pull request's user and the repositories of its head and base, keep only their name.
        """
        summary = json.loads(json.dumps(payload))
        # A loop rather than recursion: the body may nest almost as deep as the recursion limit.
        pending: list[Any] = [summary]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                if "email" in value:
                    del value["email"]
                    value.pop("name", None)
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
        for dotted_path, name_field in _NAMED_OBJECTS.items():
            parent_path, _, key = dotted_path.rpartition(".")
            parent = get_field(summary, parent_path) if parent_path else summary
            if (
                isinstance(parent, dict)
                and isinstance(named := parent.get(key), dict)
                and name_field in named
            ):
                parent[key] = {name_field: named[name_field]}
        return summary

    @staticmethod
    def find_path(payload: dict[str, Any]) -> str | None:
        if repo := get_text(payload, "repository.full_name"):
            return repo
        if org := get_text(payload, "organization.login"):
            return f"{org}/*"
        return None

    @classmethod
    def find_detail(cls, event: str, action: str | None, payload: dict[str, Any]) -> str | None:
        if action in ("labeled", "unlabeled"):
            return get_text(payload, "label.name")
        if event == "push":
            return cls.find_branch(event, payload)
        if event == "workflow_run" and cls.is_run_of_the_repository(payload):
            return get_text(payload, "workflow.name")
        return None

    @staticmethod
    def is_run_of_the_repository(payload: dict[str, Any]) -> bool:
        """Return whether a ``workflow_run`` ran on a commit of its repository rather than of a fork."""
        head_repository = get_text(payload, "workflow_run.head_repository.full_name")
        return head_repository is not None and head_repository == get_text(payload, "repository.full_name")

    @classmethod
    def find_branch(cls, event: str, payload: dict[str, Any]) -> str | None:
        """Return the branch the delivery is about, by the rule in the class docstring, or None."""
        if event == "workflow_run" and not cls.is_run_of_the_repository(payload):
            return None
        if dotted_path := _BRANCH_FIELDS.get(event):
            return get_text(payload, dotted_path)
        ref = get_text(payload, "ref")
        if event in ("create", "delete"):
            # These name the ref without its refs/heads/ or refs/tags/ prefix.
            return ref if get_text(payload, "ref_type") == "branch" else None
        if ref and ref.startswith("refs/heads/"):
            return ref.removeprefix("refs/heads/") or None
        return None

    @staticmethod
    def find_number(payload: dict[str, Any]) -> int | None:
        """Return the number of the pull request or issue the delivery is about, or None."""
        for dotted_path in ("number", "pull_request.number", "issue.number"):
            number = get_field(payload, dotted_path)
            if isinstance(number, int) and not isinstance(number, bool):
                return number
        return None

    @classmethod
    def find_correlation_key(cls, payload: dict[str, Any]) -> str | None:
        """Return ``<repository>#<number>`` for a delivery about a pull request or issue, or None."""
        repository = get_text(payload, "repository.full_name")
        number = cls.find_number(payload)
        if repository is None or number is None:
            return None
        key = f"{repository}#{number}"
        # GitHub's keys are far shorter: an owner has at most 39 characters and a repository 100. Core
        # answers a longer key with a 500, so a delivery that GitHub cannot have sent goes without one.
        return key if len(key) <= _MAX_CORRELATION_KEY_LENGTH else None
