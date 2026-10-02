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
"""Decide which task-handler artifacts a Dag file's parse must probe, and which recorded answers still hold."""

from __future__ import annotations

from typing import TYPE_CHECKING

import attrs

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from airflow.dag_processing.processor import TaskHandlerArtifact
    from airflow.sdk.execution_time.coordinator import TaskHandlerCandidate  # noqa: SDK001


@attrs.frozen(kw_only=True)
class TaskHandlerProbePlan:
    """What a parse does with each candidate in one coordinator's artifact bundle."""

    cached: list[TaskHandlerArtifact]
    """Recorded answers that still hold, in candidate order."""

    probe: list[TaskHandlerCandidate]
    """Candidates to probe, in candidate order."""

    rejected: list[TaskHandlerCandidate]
    """Candidates the listing found unusable; they are neither probed nor answered from a record."""


def plan_task_handler_probes(
    *,
    bundle_name: str,
    candidates: Sequence[TaskHandlerCandidate],
    known_artifacts: Iterable[TaskHandlerArtifact],
) -> TaskHandlerProbePlan:
    """
    Sort the *candidates* listed in *bundle_name* into recorded answers to reuse, probes, and rejects.

    A probe answer depends only on the artifact, so the answer recorded with an artifact still holds while
    the candidate has the same size and stored cache digest. The size is compared too, because a stored
    digest is read, not recomputed, and survives an in-place edit. A candidate that stores no digest is
    always probed.
    """
    known = {
        artifact.relative_fileloc: artifact
        for artifact in known_artifacts
        if artifact.bundle_name == bundle_name
    }
    cached: list[TaskHandlerArtifact] = []
    probe: list[TaskHandlerCandidate] = []
    rejected: list[TaskHandlerCandidate] = []
    for candidate in candidates:
        if candidate.error is not None:
            rejected.append(candidate)
        elif (
            candidate.cache_digest is not None
            and (artifact := known.get(candidate.rel_path)) is not None
            and (artifact.size_bytes, artifact.cache_digest) == (candidate.size_bytes, candidate.cache_digest)
        ):
            cached.append(artifact)
        else:
            probe.append(candidate)
    return TaskHandlerProbePlan(cached=cached, probe=probe, rejected=rejected)
