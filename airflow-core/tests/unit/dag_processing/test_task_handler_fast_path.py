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

from airflow.dag_processing.processor import TaskHandlerArtifact, TaskHandlerDeclaration
from airflow.dag_processing.task_handler_fast_path import TaskHandlerProbePlan, plan_task_handler_probes
from airflow.sdk.execution_time.coordinator import TaskHandlerCandidate

BUNDLE_NAME = "java-task-handlers"
DIGEST = "a" * 64
OTHER_DIGEST = "b" * 64


def _make_candidate(
    rel_path: str = "handlers.jar",
    *,
    size_bytes: int = 100,
    cache_digest: str | None = DIGEST,
    error: str | None = None,
) -> TaskHandlerCandidate:
    return TaskHandlerCandidate(
        rel_path=rel_path, size_bytes=size_bytes, cache_digest=cache_digest, error=error
    )


def _make_artifact(
    rel_path: str = "handlers.jar",
    *,
    bundle_name: str = BUNDLE_NAME,
    size_bytes: int = 100,
    cache_digest: str | None = DIGEST,
) -> TaskHandlerArtifact:
    # Two Dags, as if several Dag files resolve against the artifact.
    return TaskHandlerArtifact(
        bundle_name=bundle_name,
        relative_fileloc=rel_path,
        size_bytes=size_bytes,
        cache_digest=cache_digest,
        task_handlers={
            "etl": [TaskHandlerDeclaration(task_id="extract", binding="named", params=[])],
            "reporting": [TaskHandlerDeclaration(task_id="publish", binding="positional", params=[])],
        },
    )


@pytest.mark.parametrize(
    ("candidate", "known", "outcome"),
    [
        pytest.param(_make_candidate(), [_make_artifact()], "cached", id="same-size-and-digest"),
        pytest.param(_make_candidate(), [], "probe", id="added"),
        pytest.param(_make_candidate(size_bytes=101), [_make_artifact()], "probe", id="resized-same-digest"),
        pytest.param(
            _make_candidate(cache_digest=OTHER_DIGEST),
            [_make_artifact()],
            "probe",
            id="digest-changed-same-size",
        ),
        pytest.param(_make_candidate(cache_digest=None), [_make_artifact()], "probe", id="stores-no-digest"),
        pytest.param(_make_candidate(cache_digest=None), [], "probe", id="stores-no-digest-and-unknown"),
        pytest.param(
            _make_candidate(), [_make_artifact(cache_digest=None)], "probe", id="recorded-without-a-digest"
        ),
        pytest.param(
            _make_candidate(cache_digest=None),
            [_make_artifact(cache_digest=None)],
            "probe",
            id="neither-has-a-digest",
        ),
        pytest.param(
            _make_candidate(),
            [_make_artifact(bundle_name="other-bundle")],
            "probe",
            id="same-path-other-bundle",
        ),
        pytest.param(
            _make_candidate(
                error="handlers.jar has an Airflow-Cache-Digest manifest attribute but no Main-Class"
            ),
            [_make_artifact()],
            "rejected",
            id="listing-error-despite-a-matching-record",
        ),
    ],
)
def test_decides_each_candidate(candidate, known, outcome):
    plan = plan_task_handler_probes(bundle_name=BUNDLE_NAME, candidates=[candidate], known_artifacts=known)

    expected = {
        "cached": TaskHandlerProbePlan(cached=known, probe=[], rejected=[]),
        "probe": TaskHandlerProbePlan(cached=[], probe=[candidate], rejected=[]),
        "rejected": TaskHandlerProbePlan(cached=[], probe=[], rejected=[candidate]),
    }
    assert plan == expected[outcome]


def test_reuses_the_whole_recorded_answer():
    artifact = _make_artifact()

    plan = plan_task_handler_probes(
        bundle_name=BUNDLE_NAME, candidates=[_make_candidate()], known_artifacts=[artifact]
    )

    assert plan.cached[0] is artifact
    assert set(plan.cached[0].task_handlers) == {"etl", "reporting"}


def test_decides_candidates_independently_in_listing_order():
    unchanged_a = _make_candidate("a.jar")
    rebuilt = _make_candidate("b.jar", cache_digest=OTHER_DIGEST)
    unusable = _make_candidate(
        "c.jar", error="c.jar has an Airflow-Cache-Digest manifest attribute but no Main-Class"
    )
    unchanged_d = _make_candidate("d.jar")
    added = _make_candidate("e.jar")
    known = [
        _make_artifact("d.jar"),
        _make_artifact("gone.jar"),
        _make_artifact("b.jar"),
        _make_artifact("a.jar"),
    ]

    plan = plan_task_handler_probes(
        bundle_name=BUNDLE_NAME,
        candidates=[unchanged_a, rebuilt, unusable, unchanged_d, added],
        known_artifacts=known,
    )

    assert plan == TaskHandlerProbePlan(
        cached=[known[3], known[0]], probe=[rebuilt, added], rejected=[unusable]
    )
