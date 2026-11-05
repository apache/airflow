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

import json

import pytest
import sync_java_sdk_supervisor_schema as syncer

VERSION = "2027-02-28"


def write_schema(path, api_version=VERSION, **extra):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"api_version": api_version, **extra}))
    return path


@pytest.fixture
def repo(tmp_path):
    versions = tmp_path / syncer.SCHEMA_VERSIONS
    versions.mkdir(parents=True, exist_ok=True)
    for version in (VERSION, "2026-10-30"):
        (versions / f"v{version.replace('-', '_')}.py").touch()
    (tmp_path / syncer.GRADLE_PROPERTIES).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / syncer.GRADLE_PROPERTIES).write_text(
        f"org.gradle.caching=true\nairflowSupervisorSchemaVersion={VERSION}\n"
    )
    write_schema(tmp_path / syncer.SOURCE_SCHEMA, title="supervisor")
    write_schema(tmp_path / syncer.VENDORED_SCHEMA, title="supervisor")
    return tmp_path


def test_the_declared_version_is_read_from_gradle_properties(repo):
    assert syncer.configured_version(repo) == VERSION


def test_a_gradle_properties_without_the_version_is_an_error(repo):
    (repo / syncer.GRADLE_PROPERTIES).write_text("org.gradle.caching=true\n")

    with pytest.raises(SystemExit, match="airflowSupervisorSchemaVersion"):
        syncer.configured_version(repo)


def test_a_copy_in_step_is_left_alone(repo):
    exit_code, report = syncer.refresh_from_source(VERSION, repo)

    assert exit_code == 0
    assert report == (
        f"java-sdk/sdk/schema/schema.json matches the schema it is vendored from (api_version={VERSION})."
    )


def test_a_copy_that_drifted_without_moving_api_version_is_still_refreshed(repo):
    # Same api_version, different content.
    vendored = write_schema(repo / syncer.VENDORED_SCHEMA, title="stale")

    exit_code, report = syncer.refresh_from_source(VERSION, repo)

    assert exit_code == 1
    assert report == (
        "Refreshed java-sdk/sdk/schema/schema.json from "
        "task-sdk/src/airflow/sdk/execution_time/schema/schema.json.\n"
        "Review the diff, then git add it and recommit."
    )
    assert vendored.read_bytes() == (repo / syncer.SOURCE_SCHEMA).read_bytes()


def test_a_missing_copy_is_created(repo):
    (repo / syncer.VENDORED_SCHEMA).unlink()

    exit_code, _ = syncer.refresh_from_source(VERSION, repo)

    assert exit_code == 1
    assert (repo / syncer.VENDORED_SCHEMA).read_bytes() == (repo / syncer.SOURCE_SCHEMA).read_bytes()


def test_an_older_pin_keeps_its_vendored_copy(repo):
    vendored = write_schema(repo / syncer.VENDORED_SCHEMA, api_version="2026-10-30", title="released")

    exit_code, report = syncer.refresh_from_source("2026-10-30", repo)

    assert exit_code == 0
    assert report == (
        f"java-sdk/sdk/schema/schema.json is pinned to api_version=2026-10-30 (source is {VERSION})."
    )
    assert json.loads(vendored.read_text())["title"] == "released"


def test_an_older_pin_with_a_different_vendored_version_is_reported(repo):
    exit_code, report = syncer.refresh_from_source("2026-10-30", repo)

    assert exit_code == 1
    assert report == (
        f"java-sdk/sdk/schema/schema.json is api_version='{VERSION}' but "
        "java-sdk/gradle.properties pins '2026-10-30'.\n"
        "Run ./gradlew -p java-sdk :sdk:syncSupervisorSchema to download the pinned schema."
    )
    assert json.loads((repo / syncer.VENDORED_SCHEMA).read_text())["title"] == "supervisor"


def test_a_pin_the_bundle_does_not_define_is_reported_not_copied(repo):
    exit_code, report = syncer.refresh_from_source("2025-01-01", repo)

    assert exit_code == 1
    assert report == (
        "java-sdk/gradle.properties declares airflowSupervisorSchemaVersion='2025-01-01', which "
        "task-sdk/src/airflow/sdk/execution_time/schema/versions does not define.\n"
        f"Known versions: 2026-10-30, {VERSION}."
    )
    assert json.loads((repo / syncer.VENDORED_SCHEMA).read_text())["title"] == "supervisor"


def test_without_the_source_it_falls_back_to_the_download(repo, monkeypatch):
    (repo / syncer.SOURCE_SCHEMA).unlink()
    monkeypatch.setattr(syncer, "download_via_gradle", lambda expected, root: 7)

    assert syncer.main(repo) == 7


def test_the_download_is_skipped_when_the_copy_already_declares_the_version(repo):
    (repo / syncer.SOURCE_SCHEMA).unlink()

    assert syncer.main(repo) == 0
