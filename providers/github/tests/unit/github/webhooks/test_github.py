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
from __future__ import annotations

import copy
import hashlib
import hmac
import importlib
import json
import subprocess
import sys
from unittest import mock

import pytest

pytest.importorskip("airflow.webhooks.base")

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.github.webhooks.github import GitHubWebhookSource
from airflow.webhooks.base import WebhookDelivery, WebhookVerificationError
from airflow.webhooks.naming import build_candidate_names

from tests_common.test_utils.webhooks import assert_odd_values_are_parsed_or_refused, build_webhook_request

SECRET = "s3cret"
BODY = b'{"ref": "refs/heads/main"}'
JSON = {"content-type": "application/json"}
REPOSITORY = {"full_name": "my-org/web"}
FORK = {"full_name": "outsider/web"}
PING_BODY = b'{"zen": "Design for failure.", "hook_id": 1}'


def compute_signature(body: bytes = BODY, secret: str = SECRET) -> str:
    return f"sha256={hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()}"


def parse_json(payload: dict, **headers: str) -> WebhookDelivery:
    return GitHubWebhookSource().parse(
        build_webhook_request(json.dumps(payload).encode(), {**JSON, **headers})
    )


@mock.patch.dict(sys.modules, {"airflow.webhooks.base": None})
def test_import_without_inbound_webhooks_is_an_optional_feature():
    del sys.modules["airflow.providers.github.webhooks.github"]

    with pytest.raises(AirflowOptionalProviderFeatureException, match="Airflow 3.5"):
        importlib.import_module("airflow.providers.github.webhooks.github")


def test_verify_accepts_valid_signature():
    GitHubWebhookSource().verify(
        build_webhook_request(BODY, {"X-Hub-Signature-256": compute_signature()}), SECRET
    )


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({}, id="missing"),
        pytest.param(
            {"X-Hub-Signature-256": compute_signature().removeprefix("sha256=")}, id="missing-prefix"
        ),
        pytest.param({"X-Hub-Signature-256": compute_signature(secret="wrong")}, id="wrong-secret"),
        pytest.param({"X-Hub-Signature-256": "sha256=é"}, id="non-ascii"),
    ],
)
def test_verify_rejects(headers):
    with pytest.raises(WebhookVerificationError):
        GitHubWebhookSource().verify(build_webhook_request(BODY, headers), SECRET)


@pytest.mark.parametrize(
    ("event", "payload", "expected_kind", "expected_names"),
    [
        pytest.param(
            "pull_request",
            {"action": "labeled", "label": {"name": "deploy-preview"}, "repository": REPOSITORY},
            ("pull_request", "labeled", "deploy-preview"),
            [
                "github/my-org/web/pull_request",
                "github/my-org/*/pull_request",
                "github/*/pull_request",
                "github/my-org/web/pull_request/labeled",
                "github/my-org/*/pull_request/labeled",
                "github/*/pull_request/labeled",
                "github/my-org/web/pull_request/labeled/deploy-preview",
                "github/my-org/*/pull_request/labeled/deploy-preview",
                "github/*/pull_request/labeled/deploy-preview",
            ],
            id="label-added",
        ),
        pytest.param(
            "pull_request",
            {"action": "unlabeled", "label": {"name": "deploy-preview"}, "repository": REPOSITORY},
            ("pull_request", "unlabeled", "deploy-preview"),
            [
                "github/my-org/web/pull_request",
                "github/my-org/*/pull_request",
                "github/*/pull_request",
                "github/my-org/web/pull_request/unlabeled",
                "github/my-org/*/pull_request/unlabeled",
                "github/*/pull_request/unlabeled",
                "github/my-org/web/pull_request/unlabeled/deploy-preview",
                "github/my-org/*/pull_request/unlabeled/deploy-preview",
                "github/*/pull_request/unlabeled/deploy-preview",
            ],
            id="label-removed",
        ),
        pytest.param(
            "pull_request",
            {"action": "opened", "repository": REPOSITORY},
            ("pull_request", "opened"),
            [
                "github/my-org/web/pull_request",
                "github/my-org/*/pull_request",
                "github/*/pull_request",
                "github/my-org/web/pull_request/opened",
                "github/my-org/*/pull_request/opened",
                "github/*/pull_request/opened",
            ],
            id="no-detail",
        ),
        pytest.param(
            "push",
            {"ref": "refs/heads/feature/x", "repository": REPOSITORY},
            ("push", "feature/x"),
            [
                "github/my-org/web/push",
                "github/my-org/*/push",
                "github/*/push",
                "github/my-org/web/push/feature/x",
                "github/my-org/*/push/feature/x",
                "github/*/push/feature/x",
            ],
            id="branch-push",
        ),
        pytest.param(
            "push",
            {"ref": "refs/tags/v1.0", "repository": REPOSITORY},
            ("push",),
            ["github/my-org/web/push", "github/my-org/*/push", "github/*/push"],
            id="tag-push",
        ),
        pytest.param(
            "workflow_run",
            {
                "action": "completed",
                "workflow": {"name": "build"},
                "workflow_run": {"head_repository": REPOSITORY},
                "repository": REPOSITORY,
            },
            ("workflow_run", "completed", "build"),
            [
                "github/my-org/web/workflow_run",
                "github/my-org/*/workflow_run",
                "github/*/workflow_run",
                "github/my-org/web/workflow_run/completed",
                "github/my-org/*/workflow_run/completed",
                "github/*/workflow_run/completed",
                "github/my-org/web/workflow_run/completed/build",
                "github/my-org/*/workflow_run/completed/build",
                "github/*/workflow_run/completed/build",
            ],
            id="workflow-run",
        ),
        pytest.param(
            "workflow_run",
            {
                "action": "completed",
                "workflow": {"name": "Deploy"},
                "workflow_run": {"head_repository": FORK},
                "repository": REPOSITORY,
            },
            ("workflow_run", "completed"),
            [
                "github/my-org/web/workflow_run",
                "github/my-org/*/workflow_run",
                "github/*/workflow_run",
                "github/my-org/web/workflow_run/completed",
                "github/my-org/*/workflow_run/completed",
                "github/*/workflow_run/completed",
            ],
            id="workflow-run-of-a-fork",
        ),
        pytest.param(
            "organization",
            {"action": "member_added", "organization": {"login": "my-org"}},
            ("organization", "member_added"),
            [
                "github/my-org/*/organization",
                "github/*/organization",
                "github/my-org/*/organization/member_added",
                "github/*/organization/member_added",
            ],
            id="org-level",
        ),
    ],
)
def test_parse_offers_the_proof_of_concept_names(event, payload, expected_kind, expected_names):
    delivery = parse_json(payload, **{"X-GitHub-Event": event, "X-GitHub-Delivery": "d-1"})

    assert (delivery.kinds, delivery.delivery_id) == ([expected_kind], "d-1")
    assert build_candidate_names("github", delivery.path, delivery.kinds) == expected_names


@pytest.mark.parametrize(
    ("event", "payload", "expected"),
    [
        pytest.param(
            "pull_request",
            {
                "action": "labeled",
                "number": 7,
                "label": {"name": "deploy-preview"},
                "pull_request": {"number": 7, "head": {"ref": "feature/x"}, "base": {"ref": "main"}},
                "repository": REPOSITORY,
                "organization": {"login": "my-org"},
            },
            {
                "event": "pull_request",
                "action": "labeled",
                "repository": "my-org/web",
                "organization": "my-org",
                "label": "deploy-preview",
                "branch": "feature/x",
                "number": 7,
            },
            id="pull-request",
        ),
        pytest.param(
            "pull_request_review",
            {
                "action": "submitted",
                "pull_request": {"number": 8, "head": {"ref": "feature/x"}},
                "repository": REPOSITORY,
            },
            {
                "event": "pull_request_review",
                "action": "submitted",
                "repository": "my-org/web",
                "branch": "feature/x",
                "number": 8,
            },
            id="pull-request-review",
        ),
        pytest.param(
            "issues",
            {"action": "opened", "issue": {"number": 9}, "repository": REPOSITORY},
            {"event": "issues", "action": "opened", "repository": "my-org/web", "number": 9},
            id="issue",
        ),
        pytest.param(
            "push",
            {"ref": "refs/heads/feature/x", "repository": REPOSITORY},
            {"event": "push", "repository": "my-org/web", "branch": "feature/x"},
            id="branch-push",
        ),
        pytest.param(
            "push",
            {"ref": "refs/tags/v1.0", "repository": REPOSITORY},
            {"event": "push", "repository": "my-org/web"},
            id="tag-push",
        ),
        pytest.param(
            "push",
            {"ref": "refs/heads/", "repository": REPOSITORY},
            {"event": "push", "repository": "my-org/web"},
            id="push-without-branch-name",
        ),
        pytest.param(
            "workflow_dispatch",
            {"ref": "refs/heads/main", "workflow": ".github/workflows/build.yml", "repository": REPOSITORY},
            {"event": "workflow_dispatch", "repository": "my-org/web", "branch": "main"},
            id="workflow-dispatch",
        ),
        pytest.param(
            "create",
            {"ref": "feature/x", "ref_type": "branch", "repository": REPOSITORY},
            {"event": "create", "repository": "my-org/web", "branch": "feature/x"},
            id="branch-created",
        ),
        pytest.param(
            "delete",
            {"ref": "v1.0", "ref_type": "tag", "repository": REPOSITORY},
            {"event": "delete", "repository": "my-org/web"},
            id="tag-deleted",
        ),
        pytest.param(
            "workflow_run",
            {
                "action": "completed",
                "workflow_run": {"head_branch": "feature/x", "head_repository": REPOSITORY},
                "repository": REPOSITORY,
            },
            {
                "event": "workflow_run",
                "action": "completed",
                "repository": "my-org/web",
                "branch": "feature/x",
            },
            id="workflow-run",
        ),
        pytest.param(
            "workflow_run",
            {
                "action": "completed",
                "workflow_run": {"head_branch": "main", "head_repository": FORK},
                "repository": REPOSITORY,
            },
            {"event": "workflow_run", "action": "completed", "repository": "my-org/web"},
            id="workflow-run-of-a-fork",
        ),
        pytest.param(
            "workflow_job",
            {"action": "queued", "workflow_job": {"head_branch": "feature/x"}, "repository": REPOSITORY},
            {"event": "workflow_job", "action": "queued", "repository": "my-org/web", "branch": "feature/x"},
            id="workflow-job",
        ),
        pytest.param(
            "check_suite",
            {"action": "completed", "check_suite": {"head_branch": "feature/x"}, "repository": REPOSITORY},
            {
                "event": "check_suite",
                "action": "completed",
                "repository": "my-org/web",
                "branch": "feature/x",
            },
            id="check-suite",
        ),
        pytest.param(
            "check_run",
            {
                "action": "completed",
                "check_run": {"check_suite": {"head_branch": "feature/x"}},
                "repository": REPOSITORY,
            },
            {"event": "check_run", "action": "completed", "repository": "my-org/web", "branch": "feature/x"},
            id="check-run",
        ),
        pytest.param(
            "check_suite",
            {"action": "completed", "check_suite": {"head_branch": None}, "repository": REPOSITORY},
            {"event": "check_suite", "action": "completed", "repository": "my-org/web"},
            id="check-suite-without-branch",
        ),
    ],
)
def test_parse_attributes(event, payload, expected):
    assert parse_json(payload, **{"X-GitHub-Event": event}).attributes == expected


@pytest.mark.parametrize(
    "branch",
    [
        pytest.param("ma\u00edn", id="accented"),
        pytest.param("mai\u0301n", id="combining-accent"),
        pytest.param("\uff4d\uff41\uff49\uff4e", id="fullwidth"),
        pytest.param("main\U0001f680", id="emoji-suffix"),
    ],
)
def test_push_to_a_branch_named_like_main_offers_no_name_of_main(branch):
    # Git and GitHub accept these names, and a protection rule on main does not cover them.
    delivery = parse_json(
        {"ref": f"refs/heads/{branch}", "repository": REPOSITORY}, **{"X-GitHub-Event": "push"}
    )

    assert build_candidate_names("github", delivery.path, delivery.kinds) == [
        "github/my-org/web/push",
        "github/my-org/*/push",
        "github/*/push",
    ]


@pytest.mark.parametrize(
    ("headers", "body", "expected_error"),
    [
        pytest.param({**JSON}, b"{}", "Missing X-GitHub-Event header", id="no-event-header"),
        pytest.param(
            {"content-type": "application/x-www-form-urlencoded", "X-GitHub-Event": "push"},
            b"payload=%7B%7D",
            "Expected Content-Type application/json",
            id="form",
        ),
        pytest.param({**JSON, "X-GitHub-Event": "push"}, b"[]", "Expected a JSON object", id="not-an-object"),
    ],
)
def test_parse_rejects(headers, body, expected_error):
    with pytest.raises(ValueError, match=expected_error):
        GitHubWebhookSource().parse(build_webhook_request(body, headers))


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        pytest.param({"X-GitHub-Event": "ping"}, True, id="ping"),
        pytest.param({"X-GitHub-Event": "push"}, False, id="push"),
        pytest.param({}, False, id="no-event-header"),
    ],
)
def test_is_handshake(headers, expected):
    assert GitHubWebhookSource().is_handshake(build_webhook_request(BODY, headers)) is expected


def test_ping_in_a_form_is_refused():
    request = build_webhook_request(
        b"payload=%7B%7D", {"content-type": "application/x-www-form-urlencoded", "X-GitHub-Event": "ping"}
    )

    with pytest.raises(ValueError, match="application/json"):
        GitHubWebhookSource().handshake(request)


class TestPing:
    @staticmethod
    def build_ping(**headers: str):
        return build_webhook_request(PING_BODY, {**JSON, "X-GitHub-Event": "ping", **headers})

    def test_signed_ping_is_a_handshake_answered_with_an_empty_object(self):
        source = GitHubWebhookSource()
        request = self.build_ping(**{"X-Hub-Signature-256": compute_signature(PING_BODY)})

        source.verify(request, SECRET)
        assert source.is_handshake(request) is True
        assert source.handshake(request) == {}

    def test_unsigned_ping_fails_verification(self):
        # Pings are signed, so core verifies them before it asks whether a request is a handshake.
        assert GitHubWebhookSource.unsigned_handshake is False
        with pytest.raises(WebhookVerificationError):
            GitHubWebhookSource().verify(self.build_ping(), SECRET)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        pytest.param(
            {
                "ref": "refs/heads/main",
                "pusher": {"name": "octocat", "email": "octocat@example.com"},
                "head_commit": {
                    "id": "abc123",
                    "message": "Fix the build",
                    "author": {"name": "Mona Lisa", "email": "mona@example.com", "username": "mona"},
                    "committer": {"name": "GitHub", "email": "noreply@github.com", "username": "web-flow"},
                },
                "commits": [
                    {
                        "id": "abc123",
                        "author": {"name": "Mona Lisa", "email": "mona@example.com", "username": "mona"},
                        "committer": {"name": "Mona Lisa", "email": "mona@example.com"},
                    }
                ],
                "repository": {
                    "full_name": "my-org/web",
                    "owner": {"name": "my-org", "email": None, "login": "my-org"},
                },
                "sender": {"login": "octocat", "id": 2, "type": "User"},
            },
            {
                "ref": "refs/heads/main",
                "pusher": {},
                "head_commit": {
                    "id": "abc123",
                    "message": "Fix the build",
                    "author": {"username": "mona"},
                    "committer": {"username": "web-flow"},
                },
                "commits": [{"id": "abc123", "author": {"username": "mona"}, "committer": {}}],
                "repository": {"full_name": "my-org/web"},
                "sender": {"login": "octocat"},
            },
            id="push",
        ),
        pytest.param(
            {
                "action": "labeled",
                "pull_request": {
                    "number": 7,
                    "user": {"login": "octocat", "id": 2, "type": "User"},
                    "head": {
                        "ref": "feature/x",
                        "sha": "abc123",
                        "repo": {"full_name": "octocat/web", "owner": {"login": "octocat"}, "fork": True},
                    },
                    "base": {
                        "ref": "main",
                        "sha": "def456",
                        "repo": {"full_name": "my-org/web", "owner": {"login": "my-org"}, "fork": False},
                    },
                },
                "repository": {"full_name": "my-org/web", "owner": {"login": "my-org"}, "private": False},
                "organization": {"login": "my-org", "id": 1},
                "sender": {"login": "octocat", "id": 2, "type": "User"},
            },
            {
                "action": "labeled",
                "pull_request": {
                    "number": 7,
                    "user": {"login": "octocat"},
                    "head": {"ref": "feature/x", "sha": "abc123", "repo": {"full_name": "octocat/web"}},
                    "base": {"ref": "main", "sha": "def456", "repo": {"full_name": "my-org/web"}},
                },
                "repository": {"full_name": "my-org/web"},
                "organization": {"login": "my-org"},
                "sender": {"login": "octocat"},
            },
            id="pull-request",
        ),
        pytest.param(
            {"repository": {"id": 1}, "organization": "my-org", "pull_request": {"head": "feature/x"}},
            {"repository": {"id": 1}, "organization": "my-org", "pull_request": {"head": "feature/x"}},
            id="unnamed-or-not-objects",
        ),
    ],
)
def test_summarize(payload, expected):
    original = copy.deepcopy(payload)

    assert GitHubWebhookSource().summarize(payload) == expected
    assert payload == original


@pytest.mark.parametrize(
    "event",
    ["push", "pull_request", "workflow_run", "workflow_job", "check_suite", "check_run", "create"],
)
def test_signed_delivery_of_any_shape_is_parsed_or_refused(event):
    assert_odd_values_are_parsed_or_refused(
        GitHubWebhookSource(),
        {
            "action": "labeled",
            "number": 7,
            "label": {"name": "deploy-preview"},
            "pull_request": {"number": 7, "head": {"ref": "feature/x"}},
            "issue": {"number": 7},
            "ref": "refs/heads/main",
            "ref_type": "branch",
            "workflow": {"name": "CI"},
            "workflow_run": {"head_branch": "main", "head_repository": REPOSITORY},
            "workflow_job": {"head_branch": "main"},
            "check_suite": {"head_branch": "main"},
            "check_run": {"check_suite": {"head_branch": "main"}},
            "repository": REPOSITORY,
            "organization": {"login": "my-org"},
        },
        [
            "action",
            "number",
            "label",
            "label.name",
            "pull_request",
            "pull_request.number",
            "pull_request.head",
            "pull_request.head.ref",
            "issue",
            "issue.number",
            "ref",
            "ref_type",
            "workflow",
            "workflow.name",
            "workflow_run",
            "workflow_run.head_branch",
            "workflow_run.head_repository",
            "workflow_run.head_repository.full_name",
            "workflow_job",
            "workflow_job.head_branch",
            "check_suite",
            "check_suite.head_branch",
            "check_run",
            "check_run.check_suite",
            "check_run.check_suite.head_branch",
            "repository",
            "repository.full_name",
            "organization",
            "organization.login",
        ],
        headers={"X-GitHub-Event": event},
    )


def test_import_does_not_load_pygithub():
    code = (
        "import sys; import airflow.providers.github.webhooks.github; "
        "loaded = sorted(name for name in sys.modules if name.partition('.')[0] == 'github'); "
        "assert not loaded, loaded"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
