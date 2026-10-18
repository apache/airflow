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

import pytest

from airflow_shared.webhook_naming import (
    MAX_NAME_LENGTH,
    MAX_PATH_SEGMENTS,
    build_candidate_names,
    build_topic_name,
)

#: A path of as many segments as a topic path can have.
DEEPEST_PATH = "/".join(["a"] * MAX_PATH_SEGMENTS)


@pytest.mark.parametrize(
    ("path", "kinds", "expected"),
    [
        pytest.param(
            "my-org/web",
            [("pull_request", "labeled", "deploy-preview")],
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
            id="repo-org-and-any-scope",
        ),
        pytest.param(
            "platform/backend/api",
            [("pipeline", "success")],
            [
                "github/platform/backend/api/pipeline",
                "github/platform/backend/*/pipeline",
                "github/platform/*/pipeline",
                "github/*/pipeline",
                "github/platform/backend/api/pipeline/success",
                "github/platform/backend/*/pipeline/success",
                "github/platform/*/pipeline/success",
                "github/*/pipeline/success",
            ],
            id="wildcard-at-every-level",
        ),
        pytest.param(
            "my-org/*",
            [("organization", "member_added")],
            [
                "github/my-org/*/organization",
                "github/*/organization",
                "github/my-org/*/organization/member_added",
                "github/*/organization/member_added",
            ],
            id="org-level-event",
        ),
        pytest.param(
            None,
            [("deployed",)],
            ["github/deployed"],
            id="no-path",
        ),
        pytest.param(
            "billing",
            [("deployed",)],
            ["github/billing/deployed", "github/*/deployed"],
            id="single-segment-path",
        ),
        pytest.param(
            "my-org/web",
            [("merge_request", "update"), ("merge_request", "labeled", "a")],
            [
                "github/my-org/web/merge_request",
                "github/my-org/*/merge_request",
                "github/*/merge_request",
                "github/my-org/web/merge_request/update",
                "github/my-org/*/merge_request/update",
                "github/*/merge_request/update",
                "github/my-org/web/merge_request/labeled",
                "github/my-org/*/merge_request/labeled",
                "github/*/merge_request/labeled",
                "github/my-org/web/merge_request/labeled/a",
                "github/my-org/*/merge_request/labeled/a",
                "github/*/merge_request/labeled/a",
            ],
            id="several-kinds-share-prefixes",
        ),
        pytest.param(
            "o/web",
            [("pull_request", "labeled", "\U0001f680 Café")],
            [
                "github/o/web/pull_request",
                "github/o/*/pull_request",
                "github/*/pull_request",
                "github/o/web/pull_request/labeled",
                "github/o/*/pull_request/labeled",
                "github/*/pull_request/labeled",
            ],
            id="segment-outside-ascii-ends-kind",
        ),
        pytest.param(
            "\U0001f680",
            [("deployed",)],
            ["github/*/deployed"],
            id="path-outside-ascii-offered-as-wildcard",
        ),
        pytest.param(
            "ops/\U0001f680/api",
            [("deployed",)],
            ["github/ops/*/deployed", "github/*/deployed"],
            id="path-stops-at-a-segment-outside-ascii",
        ),
        pytest.param(
            "ops//api/",
            [("deployed",)],
            ["github/ops/api/deployed", "github/ops/*/deployed", "github/*/deployed"],
            id="empty-path-segments-dropped",
        ),
        pytest.param(
            "o/web",
            [("pull_request", "labeled", "/kind//bug/")],
            [
                "github/o/web/pull_request",
                "github/o/*/pull_request",
                "github/*/pull_request",
                "github/o/web/pull_request/labeled",
                "github/o/*/pull_request/labeled",
                "github/*/pull_request/labeled",
                "github/o/web/pull_request/labeled/kind/bug",
                "github/o/*/pull_request/labeled/kind/bug",
                "github/*/pull_request/labeled/kind/bug",
            ],
            id="empty-parts-of-a-segment-dropped",
        ),
        pytest.param(
            "o/web",
            [("push", "/")],
            ["github/o/web/push", "github/o/*/push", "github/*/push"],
            id="segment-of-slashes-ends-kind",
        ),
    ],
)
def test_build_candidate_names(path, kinds, expected):
    assert build_candidate_names("github", path, kinds) == expected


@pytest.mark.parametrize(
    ("path", "kinds", "expected"),
    [
        pytest.param(
            "x" * 1488,
            [("push",)],
            [f"github/{'x' * 1488}/push", "github/*/push"],
            id="path-at-the-limit",
        ),
        pytest.param("x" * 1489, [("push",)], ["github/*/push"], id="path-over-the-limit"),
        pytest.param(
            "/".join(["x" * 60] * 30),
            [("push",)],
            # Each segment and its "/" add 61 characters, so the 26th wildcard path would be too long.
            [f"github/{('x' * 60 + '/') * depth}*/push" for depth in range(24, -1, -1)],
            id="long-path",
        ),
        pytest.param(
            "o/web",
            [("push", "x" * 1485)],
            ["github/o/web/push", "github/o/*/push", "github/*/push", f"github/*/push/{'x' * 1485}"],
            id="kind-segment-fits-only-the-broadest-path",
        ),
        pytest.param(
            "o/web",
            [("push", "x" * MAX_NAME_LENGTH, "created")],
            ["github/o/web/push", "github/o/*/push", "github/*/push"],
            id="kind-segment-over-the-limit-ends-kind",
        ),
    ],
)
def test_build_candidate_names_leaves_out_names_over_the_length_limit(path, kinds, expected):
    assert build_candidate_names("github", path, kinds) == expected


@pytest.mark.parametrize(
    ("path", "expected_paths"),
    [
        pytest.param(
            DEEPEST_PATH,
            [DEEPEST_PATH, *(f"{'a/' * depth}*" for depth in range(MAX_PATH_SEGMENTS - 1, -1, -1))],
            id="path-at-the-limit",
        ),
        pytest.param(
            "/".join(["a"] * 4000),
            [f"{'a/' * depth}*" for depth in range(MAX_PATH_SEGMENTS - 1, -1, -1)],
            id="deep-path",
        ),
    ],
)
def test_build_candidate_names_offers_deep_paths_only_under_the_wildcard_paths_a_topic_can_have(
    path, expected_paths
):
    assert build_candidate_names("github", path, [("push",)]) == [
        f"github/{expected_path}/push" for expected_path in expected_paths
    ]


@pytest.mark.parametrize(
    "branch",
    [
        pytest.param("ma\u00edn", id="accented"),
        pytest.param("mai\u0301n", id="combining-accent"),
        pytest.param("\uff4d\uff41\uff49\uff4e", id="fullwidth"),
        pytest.param("main\U0001f680", id="emoji-suffix"),
    ],
)
def test_build_candidate_names_never_offers_another_values_name(branch):
    # Folded to ASCII, each of these branches would be offered as main, which a protection rule on main
    # does not cover.
    assert build_candidate_names("github", "o/web", [("push", branch)]) == [
        "github/o/web/push",
        "github/o/*/push",
        "github/*/push",
    ]


def test_build_candidate_names_rejects_a_string_kind():
    with pytest.raises(TypeError, match="not a string"):
        build_candidate_names("github", "o/web", ["push"])


@pytest.mark.parametrize(
    ("path", "segments", "delivery_path", "expected"),
    [
        pytest.param(
            "my-org/web",
            ("pull_request", "labeled", "deploy-preview"),
            "my-org/web",
            "github/my-org/web/pull_request/labeled/deploy-preview",
            id="full-topic",
        ),
        pytest.param(
            "my-org/web", ("pull_request",), "my-org/web", "github/my-org/web/pull_request", id="event-only"
        ),
        pytest.param("my-org/*", ("push",), "my-org/web", "github/my-org/*/push", id="wildcard-path"),
        pytest.param(
            "a/b/*", ("push",), "a/b/c/d/e", "github/a/b/*/push", id="wildcard-path-matches-any-depth"
        ),
        pytest.param("*", ("push", "created"), "o/web", "github/*/push/created", id="any-path"),
        pytest.param(None, ("deployed",), None, "github/deployed", id="no-path"),
        pytest.param(
            "ops//api/", ("deployed",), "ops//api/", "github/ops/api/deployed", id="path-normalized"
        ),
        pytest.param(
            "ops/*",
            ("deployed",),
            "ops/\U0001f680/api",
            "github/ops/*/deployed",
            id="wildcard-path-matches-a-path-outside-ascii",
        ),
        pytest.param(
            "o/web",
            ("pull_request", "labeled", "kind/bug"),
            "o/web",
            "github/o/web/pull_request/labeled/kind/bug",
            id="slash-in-segment",
        ),
        pytest.param("x" * 1488, ("push",), "x" * 1488, f"github/{'x' * 1488}/push", id="name-at-the-limit"),
        pytest.param(DEEPEST_PATH, ("push",), DEEPEST_PATH, f"github/{DEEPEST_PATH}/push", id="deepest-path"),
        pytest.param(
            f"{'a/' * (MAX_PATH_SEGMENTS - 1)}*",
            ("push",),
            "/".join(["a"] * 4000),
            f"github/{'a/' * (MAX_PATH_SEGMENTS - 1)}*/push",
            id="deepest-wildcard-path-matches-any-depth",
        ),
    ],
)
def test_build_topic_name_is_a_candidate_name(path, segments, delivery_path, expected):
    name = build_topic_name("github", path, *segments)

    assert name == expected
    assert name in build_candidate_names("github", delivery_path, [segments])


@pytest.mark.parametrize(
    ("webhook", "path", "event", "action", "detail", "match"),
    [
        pytest.param("", "o/web", "push", None, None, "must match", id="empty-webhook"),
        pytest.param("GitHub", "o/web", "push", None, None, "must match", id="upper-case-webhook"),
        pytest.param("a/b", "o/web", "push", None, None, "must match", id="slash-in-webhook"),
        pytest.param("_health", "o/web", "push", None, None, "must match", id="leading-underscore-webhook"),
        pytest.param("x" * 65, "o/web", "push", None, None, "must match", id="webhook-too-long"),
        pytest.param(
            "github", "o/web", "push", None, "main", "must also have an action", id="detail-without-action"
        ),
        pytest.param("github", "o/web", "\U0001f680", None, None, "must be ASCII", id="event-outside-ascii"),
        pytest.param(
            "github", "o/web", "push", "ma\u00edn", None, "must be ASCII", id="action-outside-ascii"
        ),
        pytest.param(
            "github", "o/web", "pull_request", "labeled", "Café", "must be ASCII", id="detail-outside-ascii"
        ),
        pytest.param(
            "github", "o/web", " ", None, None, "must not be empty after normalization", id="empty-event"
        ),
        pytest.param(
            "github", "o/w\u00e9b", "push", None, None, "path must be ASCII", id="path-outside-ascii"
        ),
        pytest.param(
            "github",
            "o/web",
            "push",
            "created",
            "/",
            "must not be empty after normalization",
            id="slash-detail",
        ),
        pytest.param(
            "github",
            "o/web",
            "push",
            "created",
            " ",
            "must not be empty after normalization",
            id="whitespace-detail",
        ),
        pytest.param("github", "*", "*", None, None, r"cannot contain '\*'", id="wildcard-event"),
        pytest.param("github", "*", "push", "*", None, r"cannot contain '\*'", id="wildcard-action"),
        pytest.param("github", "*", "push", "created", "*", r"cannot contain '\*'", id="wildcard-detail"),
        pytest.param(
            "github",
            "my-org/*/api",
            "push",
            None,
            None,
            "whole last segment",
            id="wildcard-before-last-segment",
        ),
        pytest.param("github", "my-org/web-*", "push", None, None, "whole last segment", id="glob-segment"),
        pytest.param(
            "github",
            "x" * 1489,
            "push",
            None,
            None,
            "cannot exceed 1500 characters",
            id="name-over-the-limit",
        ),
        pytest.param(
            "github",
            f"{DEEPEST_PATH}/a",
            "push",
            None,
            None,
            "cannot have more than 32 segments, got 33",
            id="path-over-the-segment-limit",
        ),
        pytest.param(
            "github",
            f"{DEEPEST_PATH}/*",
            "push",
            None,
            None,
            "cannot have more than 32 segments, got 33",
            id="wildcard-path-over-the-segment-limit",
        ),
    ],
)
def test_build_topic_name_rejects_names_no_delivery_can_match(webhook, path, event, action, detail, match):
    with pytest.raises(ValueError, match=match):
        build_topic_name(webhook, path, event, action, detail)
