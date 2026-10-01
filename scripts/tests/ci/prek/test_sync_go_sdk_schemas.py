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
import sync_go_sdk_schemas as syncer

DAG_SCHEMA, SUPERVISOR_SCHEMA = syncer.VENDORED_SCHEMAS


@pytest.fixture
def repo(tmp_path):
    """A checkout holding both sources and both vendored copies, all in step."""
    for schema in syncer.VENDORED_SCHEMAS:
        for relative_path in (schema.source, schema.vendored):
            path = tmp_path / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f'{{"title": "{schema.source.name}"}}')
    return tmp_path


def test_both_schemas_are_vendored_inside_the_go_module():
    # A copy outside go-sdk/ would leave `go generate` reaching across the monorepo,
    # which is the coupling vendoring removes.
    for schema in syncer.VENDORED_SCHEMAS:
        assert str(schema.vendored).startswith("go-sdk/schema/"), schema.vendored
        assert not str(schema.source).startswith("go-sdk/"), schema.source


def test_a_copy_in_step_is_left_alone(repo):
    assert syncer.refresh(DAG_SCHEMA, repo) is False


def test_a_stale_copy_is_overwritten_from_its_source(repo):
    vendored_path = repo / SUPERVISOR_SCHEMA.vendored
    vendored_path.write_text('{"title": "stale"}')

    assert syncer.refresh(SUPERVISOR_SCHEMA, repo) is True
    assert vendored_path.read_bytes() == (repo / SUPERVISOR_SCHEMA.source).read_bytes()


def test_a_missing_copy_is_created(repo):
    vendored_path = repo / DAG_SCHEMA.vendored
    vendored_path.unlink()

    assert syncer.refresh(DAG_SCHEMA, repo) is True
    assert vendored_path.read_bytes() == (repo / DAG_SCHEMA.source).read_bytes()


def test_a_missing_source_is_an_error_not_a_silent_pass(repo):
    # Overwriting the copy with nothing, or reporting success, would both hide that the
    # schema moved on the Python side.
    (repo / DAG_SCHEMA.source).unlink()

    with pytest.raises(SystemExit, match=str(DAG_SCHEMA.source)):
        syncer.refresh(DAG_SCHEMA, repo)


def test_copies_in_step_pass_and_name_what_was_compared():
    exit_code, report = syncer.format_report(())

    assert exit_code == 0
    assert "go-sdk/schema/dag-schema.json" in report
    assert "go-sdk/schema/supervisor-schema.json" in report


def test_a_refreshed_dag_schema_fails_and_says_what_to_regenerate():
    exit_code, report = syncer.format_report((DAG_SCHEMA,))

    assert exit_code == 1
    assert "Refreshed go-sdk/schema/dag-schema.json" in report
    assert "just generate-specs" in report
    assert "go-sdk/airflow/spec.gen.go" in report
    # A property that should not reach a Dag author is a decision, not a copy.
    assert "go-sdk/internal/genspec/authoring.go" in report


def test_a_refreshed_supervisor_schema_points_at_the_version_constant():
    exit_code, report = syncer.format_report((SUPERVISOR_SCHEMA,))

    assert exit_code == 1
    assert "just generate-models" in report
    # api_version and SupervisorSchemaVersion have to move together.
    assert "go-sdk/pkg/execution/messages.go" in report
    assert "just generate-specs" not in report
