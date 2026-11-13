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

import errno
import json
import os
import subprocess
import sys
from unittest import mock

import pytest

from airflow.cli.commands import dag_bundle_command
from airflow.dag_processing.bundles import filesystem_artifact_store
from airflow.dag_processing.bundles.artifact_store import BundleManifestRefDurabilityError

from tests_common.test_utils.config import conf_vars

MANIFEST_LOCAL_CLASSPATH = "airflow.dag_processing.bundles.manifest_local.ManifestLocalDagBundle"
STALE_VERSION = f"sha256-{'a' * 64}"


@pytest.fixture
def source(tmp_path):
    source = tmp_path / "source"
    (source / "sub").mkdir(parents=True)
    (source / "dag.py").write_text("print('dag')")
    (source / "sub" / "helper.py").write_text("x = 1")
    return source


@pytest.fixture
def published_root(tmp_path):
    return tmp_path / "published"


# caplog keeps log lines out of the captured stdout that the tests parse as JSON.
@pytest.fixture(autouse=True)
def bundle_config(tmp_path, source, published_root, caplog):
    (tmp_path / "not-a-dir").write_text("")
    config = [
        {
            "name": "manifest-local",
            "classpath": MANIFEST_LOCAL_CLASSPATH,
            "kwargs": {"published_root": str(published_root)},
        },
        # Publishing any other bundle must not need this class to be importable.
        {"name": "not-installed", "classpath": "not_installed_module.Bundle", "kwargs": {}},
        # Constructing this bundle fails, so it shows the type is checked first.
        {
            "name": "plain-local",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"unknown_kwarg": True},
        },
        {"name": "not-a-class", "classpath": "os.getcwd", "kwargs": {}},
        {"name": "no-published-root", "classpath": MANIFEST_LOCAL_CLASSPATH, "kwargs": {}},
        {
            "name": "published-root-under-file",
            "classpath": MANIFEST_LOCAL_CLASSPATH,
            "kwargs": {"published_root": str(tmp_path / "not-a-dir" / "published")},
        },
    ]
    with conf_vars(
        {
            ("core", "load_examples"): "False",
            ("dag_processor", "dag_bundle_config_list"): json.dumps(config),
            ("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "cache"),
        }
    ):
        yield


def _publish(parser, capsys, bundle_name, source_path, *extra_args):
    args = parser.parse_args(
        ["dag-bundles", "publish-local", bundle_name, str(source_path), "--output", "json", *extra_args]
    )
    dag_bundle_command.dag_bundle_publish_local(args)
    [result] = json.loads(capsys.readouterr().out)
    return result


def test_publish_local_releases_source_snapshot(parser, capsys, source, published_root):
    result = _publish(parser, capsys, "manifest-local", source)

    manifest_ref_path = published_root / "refs" / "manifest-local" / "latest.json"
    version_path = published_root / "versions" / "manifest-local" / result["version"]
    manifest_ref = json.loads(manifest_ref_path.read_text())
    assert result == {
        "bundle_name": "manifest-local",
        "version": manifest_ref["version"],
        "version_path": str(version_path),
        "manifest_ref_path": str(manifest_ref_path),
        "manifest_sha256": manifest_ref["manifest"]["sha256"],
        "file_count": "2",
        "total_size": str(len("print('dag')") + len("x = 1")),
        "created_snapshot": "True",
    }
    assert (version_path / "dag.py").read_text() == "print('dag')"
    assert (version_path / "sub" / "helper.py").read_text() == "x = 1"


@pytest.mark.parametrize(
    ("bundle_name", "source_name", "extra_args", "message"),
    [
        pytest.param(
            "missing",
            "source",
            [],
            "Cannot load Dag bundle 'missing': Requested bundle 'missing' is not configured.",
            id="unconfigured",
        ),
        pytest.param(
            "not-installed",
            "source",
            [],
            "Cannot load Dag bundle 'not-installed': No module named 'not_installed_module'",
            id="class-not-importable",
        ),
        pytest.param(
            "no-published-root",
            "source",
            [],
            "Cannot load Dag bundle 'no-published-root': published_root must be provided",
            id="invalid-kwargs",
        ),
        pytest.param(
            "plain-local",
            "source",
            [],
            "Dag bundle 'plain-local' is configured with airflow.dag_processing.bundles.local.LocalDagBundle, "
            "not a ManifestLocalDagBundle",
            id="not-manifest-local",
        ),
        pytest.param(
            "not-a-class",
            "source",
            [],
            "Dag bundle 'not-a-class' is configured with os.getcwd, not a ManifestLocalDagBundle",
            id="not-a-class",
        ),
        pytest.param(
            "manifest-local",
            "source",
            ["--expected-current-version", STALE_VERSION],
            "Failed to publish Dag bundle 'manifest-local': Bundle 'manifest-local' has no published version",
            id="expected-version-before-first-release",
        ),
        pytest.param(
            "published-root-under-file",
            "source",
            [],
            "Failed to publish Dag bundle 'published-root-under-file': .*Not a directory",
            id="os-error",
        ),
        pytest.param(
            "manifest-local",
            "missing-source",
            [],
            "Failed to publish Dag bundle 'manifest-local': Bundle source path does not exist",
            id="missing-source",
        ),
        pytest.param(
            "manifest-local",
            "published/source",
            [],
            "Failed to publish Dag bundle 'manifest-local': source_path must not be inside published_root",
            id="overlapping-paths",
        ),
    ],
)
def test_publish_local_reports_expected_failures_without_traceback(
    parser, tmp_path, bundle_name, source_name, extra_args, message
):
    source_path = tmp_path / source_name
    if source_name != "missing-source":
        source_path.mkdir(parents=True, exist_ok=True)
        (source_path / "dag.py").write_text("print('dag')")
    args = parser.parse_args(["dag-bundles", "publish-local", bundle_name, str(source_path), *extra_args])

    with pytest.raises(SystemExit, match=message):
        dag_bundle_command.dag_bundle_publish_local(args)


@conf_vars({("dag_processor", "dag_bundle_config_list"): "[{"})
def test_publish_local_reports_invalid_bundle_config(parser, source):
    args = parser.parse_args(["dag-bundles", "publish-local", "manifest-local", str(source)])

    with pytest.raises(SystemExit, match="Cannot load Dag bundle 'manifest-local': Unable to parse"):
        dag_bundle_command.dag_bundle_publish_local(args)


@pytest.mark.parametrize("expected_current_version", ["", "latest", STALE_VERSION.upper()])
def test_publish_local_rejects_malformed_expected_version_before_writing(
    parser, source, published_root, expected_current_version
):
    args = parser.parse_args(
        [
            "dag-bundles",
            "publish-local",
            "manifest-local",
            str(source),
            "--expected-current-version",
            expected_current_version,
        ]
    )

    with pytest.raises(SystemExit, match="--expected-current-version .* is not a bundle version"):
        dag_bundle_command.dag_bundle_publish_local(args)
    assert not published_root.exists()


def test_publish_local_keeps_traceback_when_verbose(parser, source):
    args = parser.parse_args(["dag-bundles", "publish-local", "missing", str(source), "--verbose"])

    with pytest.raises(ValueError, match="Requested bundle 'missing' is not configured."):
        dag_bundle_command.dag_bundle_publish_local(args)


@mock.patch.object(filesystem_artifact_store, "fsync_directory", autospec=True)
@pytest.mark.parametrize("verbose", [False, True])
def test_publish_local_reports_uncertain_durability_after_reference_replacement(
    mock_fsync_directory, parser, capsys, source, published_root, verbose
):
    first = _publish(parser, capsys, "manifest-local", source)
    manifest_ref_path = published_root / "refs" / "manifest-local" / "latest.json"
    (source / "dag.py").write_text("print('new release')")
    sync_error = OSError(errno.EIO, "directory sync failed")

    def fail_after_reference_replacement(path):
        if path == manifest_ref_path.parent:
            current_version = json.loads(manifest_ref_path.read_text())["version"]
            if current_version != first["version"]:
                raise sync_error

    mock_fsync_directory.side_effect = fail_after_reference_replacement
    args = parser.parse_args(
        [
            "dag-bundles",
            "publish-local",
            "manifest-local",
            str(source),
            "--expected-current-version",
            first["version"],
            "--output",
            "json",
        ]
    )
    args.verbose = verbose
    error_type = BundleManifestRefDurabilityError if verbose else SystemExit
    message = (
        "Release reference .* was replaced, but durability could not be confirmed"
        if verbose
        else "Cannot confirm publication of Dag bundle 'manifest-local': Release reference"
    )

    with pytest.raises(error_type, match=message) as exc_info:
        dag_bundle_command.dag_bundle_publish_local(args)

    current_version = json.loads(manifest_ref_path.read_text())["version"]
    assert current_version != first["version"]
    assert (published_root / "versions" / "manifest-local" / current_version / "dag.py").read_text() == (
        "print('new release')"
    )
    assert "Check the current reference before retrying publication" in str(exc_info.value)
    assert capsys.readouterr().out == ""
    if verbose:
        assert exc_info.value.__cause__ is sync_error
        assert exc_info.value.ref_path == manifest_ref_path
        assert exc_info.value.version == current_version

    mock_fsync_directory.side_effect = None
    args.verbose = False
    with pytest.raises(SystemExit, match="manifest reference changed: expected"):
        dag_bundle_command.dag_bundle_publish_local(args)


def test_publish_local_outputs_pure_json_without_metadata_db(tmp_path, source, published_root):
    """Run the real CLI, as a deployment pipeline would, against a database that must stay unused."""
    db_path = tmp_path / "unused.db"
    # Unit test mode logs while Airflow is imported, before any command can suppress logs.
    env = {
        **{key: value for key, value in os.environ.items() if key != "AIRFLOW__CORE__UNIT_TEST_MODE"},
        "AIRFLOW_HOME": str(tmp_path / "home"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{db_path}",
        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH": str(tmp_path / "cache"),
        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(
            [
                {
                    "name": "manifest-local",
                    "classpath": MANIFEST_LOCAL_CLASSPATH,
                    "kwargs": {"published_root": str(published_root)},
                }
            ]
        ),
        # CI systems commonly force colour, which must not reach JSON output.
        "FORCE_COLOR": "1",
        "TTY_COMPATIBLE": "1",
    }

    process = subprocess.run(
        [sys.executable, "-m", "airflow", "dag-bundles", "publish-local", "manifest-local", str(source)]
        + ["--output", "json"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    assert process.returncode == 0, process.stderr
    [result] = json.loads(process.stdout)
    assert (published_root / "versions" / "manifest-local" / result["version"] / "dag.py").exists()
    # SQLite creates the database file on first connection.
    assert not db_path.exists()
