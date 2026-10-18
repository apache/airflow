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
The webhook topic name grammar: ``<webhook>/<path>/<event>/<action>/<detail>``.

A Dag subscribes to a webhook delivery by scheduling on the asset with one of these names. The
Task SDK builds the name of a topic and the API server builds the candidate names of a delivery
with the same rules, so a topic always names one of the candidates.

A value with a character outside ASCII is never part of a name, and is not folded into one either:
two values that fold to the same text, such as the branches ``maín`` and ``main``, would otherwise
offer the same names, so a branch that no protection rule covers could start the Dags subscribed to
the protected one.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

# The SDK refuses longer asset names and the asset tables cannot store them, so no longer name can
# ever match.
MAX_NAME_LENGTH = 1500
# Each path segment adds a candidate name per kind prefix, and a delivery's path comes from its body, so
# this bounds the names a delivery is stored and looked up with.
MAX_PATH_SEGMENTS = 32
# No leading underscore, so no webhook is named like the webhooks app's own ``/_health`` route.
WEBHOOK_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


def normalize_segment(value: str) -> str:
    """Return ``value`` as a part of a name, or ``""`` if it has a character outside ASCII."""
    # MySQL stores asset names as latin1 and fails any lookup that includes a non-ASCII name.
    if not value.isascii():
        return ""
    # Empty parts are dropped so no name contains "//": "/foo" -> "foo", while a branch such as
    # "feature/x" keeps its slash.
    return "/".join(part for part in value.split("/") if part.strip()).strip()


def build_wildcard_paths(path: str, *, max_length: int = MAX_NAME_LENGTH) -> list[str]:
    """
    Return ``path`` with ``*`` in place of each trailing part, narrow to broad (``a/*``, ``*``).

    Wildcard paths longer than ``max_length`` are left out.
    """
    wildcard_paths: list[str] = []
    head = ""
    for segment in path.split("/"):
        if len(head) + 1 > max_length:
            break
        wildcard_paths.append(f"{head}*")
        head = f"{head}{segment}/"
    return wildcard_paths[::-1]


def _split_path(path: str | None) -> tuple[list[str], bool]:
    """
    Return the normalized segments of ``path``, and whether they are all of it.

    Segments that normalize to nothing are left out. The segments stop before the first one with a
    character outside ASCII, which no name can hold.
    """
    segments: list[str] = []
    for segment in (path or "").split("/"):
        if not segment.isascii():
            return segments, False
        if normalized := normalize_segment(segment):
            segments.append(normalized)
    return segments, True


def build_candidate_names(webhook: str, path: str | None, kinds: Iterable[tuple[str, ...]]) -> list[str]:
    """
    Return every asset name a Dag could subscribe to for a delivery, broad to specific.

    Each kind prefix is offered for the delivery's own path and for the path with ``*`` in place of
    each trailing part, so ``*`` means anything below that point: ``platform/backend/api`` is also
    offered as ``platform/backend/*``, ``platform/*`` and ``*``.

    A path segment that normalizes to nothing is left out of the path, and a kind segment that does
    ends the kind, so no name contains ``//``. A kind segment with a character outside ASCII ends the
    kind too, and a path segment with one is offered only as ``*``, under the segments before it, so
    that each name stands for exactly one value. Names longer than ``MAX_NAME_LENGTH`` are left out,
    so a delivery with a very long path is offered only under the wildcard paths that fit. So are
    paths of more than ``MAX_PATH_SEGMENTS`` segments, a final ``*`` included: a deeper delivery is
    offered only under wildcard paths, the narrowest being its first ``MAX_PATH_SEGMENTS - 1``
    segments followed by ``*``.

    :param webhook: The webhook name, used as is.
    :param path: Where the delivery happened, ``/``-separated from broad to narrow, or None.
    :param kinds: What happened, each from broad to specific, such as
        ``("pull_request", "labeled", "deploy-preview")``.
    :raises TypeError: if a kind is a string rather than a tuple of segments.
    """
    # Each scope carries its leading "/", and the shortest kind adds "/" and one character.
    max_path_length = MAX_NAME_LENGTH - len(webhook) - 3
    scopes = [""]
    segments, complete = _split_path(path)
    if segments or not complete:
        normalized_path = "/".join(segments)
        # build_wildcard_paths never offers a path's last segment, so an empty one stands in for the
        # segment that cannot be named.
        offered_segments = segments if complete else [*segments, ""]
        wildcard_paths = build_wildcard_paths(
            "/".join(offered_segments[:MAX_PATH_SEGMENTS]), max_length=max_path_length
        )
        if complete and len(segments) <= MAX_PATH_SEGMENTS and len(normalized_path) <= max_path_length:
            wildcard_paths.insert(0, normalized_path)
        scopes = [f"/{scope}" for scope in dict.fromkeys(wildcard_paths)]
    names: dict[str, None] = {}
    for kind in kinds:
        if isinstance(kind, str):
            raise TypeError(f"A webhook delivery kind must be a tuple of segments, not a string: {kind!r}")
        prefix = ""
        for segment in kind:
            if not (normalized := normalize_segment(segment)):
                break
            prefix = f"{prefix}/{normalized}"
            fitting = [
                scope for scope in scopes if len(webhook) + len(scope) + len(prefix) <= MAX_NAME_LENGTH
            ]
            if not fitting:
                break
            for scope in fitting:
                names[f"{webhook}{scope}{prefix}"] = None
    return list(names)


def build_topic_name(
    webhook: str,
    path: str | None,
    event: str,
    action: str | None = None,
    detail: str | None = None,
) -> str:
    """
    Return the asset name of a webhook topic.

    The name is one that ``build_candidate_names`` offers for a delivery with this ``path`` and the
    kind ``(event, action, detail)``. ``path`` is normalized the same way, and its last segment may
    be ``*`` to subscribe to anything below that point. None matches only deliveries that have no
    path; use ``"*"`` for any path.

    :raises ValueError: if no delivery can be offered this name: ``webhook`` does not match
        ``WEBHOOK_NAME_PATTERN``, ``detail`` is given without ``action``, a given segment normalizes
        to nothing, a part has a character outside ASCII, ``*`` is anywhere but the whole last path
        segment, the path has more than ``MAX_PATH_SEGMENTS`` segments, or the name is longer than
        ``MAX_NAME_LENGTH``.
    """
    if not WEBHOOK_NAME_PATTERN.fullmatch(webhook):
        raise ValueError(f"Webhook name must match {WEBHOOK_NAME_PATTERN.pattern!r}, got {webhook!r}")
    if detail is not None and action is None:
        raise ValueError("A webhook topic with a detail must also have an action")
    segments = [segment for segment in (event, action, detail) if segment is not None]
    if not all(segment.isascii() for segment in segments):
        raise ValueError(
            f"Webhook topic segments must be ASCII, as no delivery offers a name for any other value, got "
            f"{(event, action, detail)!r}"
        )
    kind = [normalize_segment(segment) for segment in segments]
    if not all(kind):
        raise ValueError(
            "Webhook topic segments must not be empty after normalization (whitespace and empty '/' parts "
            f"are dropped), got {(event, action, detail)!r}"
        )
    if any("*" in segment for segment in kind):
        raise ValueError(
            f"A webhook topic's event, action and detail cannot contain '*', got {(event, action, detail)!r}"
        )
    path_segments, complete = _split_path(path)
    if not complete:
        raise ValueError(
            "A webhook topic path must be ASCII, as no delivery offers a name for any other value, "
            f"got {path!r}"
        )
    normalized_path = "/".join(path_segments)
    *parents, last = normalized_path.split("/")
    if any("*" in parent for parent in parents) or ("*" in last and last != "*"):
        raise ValueError(f"'*' can only be the whole last segment of a webhook topic path, got {path!r}")
    if len(parents) >= MAX_PATH_SEGMENTS:
        raise ValueError(
            f"A webhook topic path cannot have more than {MAX_PATH_SEGMENTS} segments, got {len(parents) + 1}"
        )
    name = "/".join(part for part in (webhook, normalized_path, *kind) if part)
    if len(name) > MAX_NAME_LENGTH:
        raise ValueError(f"A webhook topic name cannot exceed {MAX_NAME_LENGTH} characters, got {len(name)}")
    return name
