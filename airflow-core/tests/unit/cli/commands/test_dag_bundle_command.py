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
import os
import subprocess
import sys

import pytest

from airflow.cli.commands import dag_bundle_command

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
        {
            "name": "plain-local",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": str(source)},
        },
        {"name": "no-published-root", "classpath": MANIFEST_LOCAL_CLASSPATH, "kwargs": {}},
        {
            "name": "unwritable",
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
    ("bundle_name", "extra_args", "message"),
    [
        pytest.param(
            "missing",
            [],
            "Cannot load Dag bundle 'missing': Requested bundle 'missing' is not configured.",
            id="unconfigured",
        ),
        pytest.param(
            "no-published-root",
            [],
            "Cannot load Dag bundle 'no-published-root': published_root must be provided",
            id="invalid-kwargs",
        ),
        pytest.param(
            "plain-local",
            [],
            "Dag bundle 'plain-local' is a LocalDagBundle, not a ManifestLocalDagBundle",
            id="not-manifest-local",
        ),
        pytest.param(
            "manifest-local",
            ["--expected-current-version", STALE_VERSION],
            "Failed to publish Dag bundle 'manifest-local': Bundle 'manifest-local' has no published version",
            id="stale-expected-version",
        ),
        pytest.param(
            "unwritable",
            [],
            "Failed to publish Dag bundle 'unwritable': .*Not a directory",
            id="os-error",
        ),
    ],
)
def test_publish_local_reports_expected_failures_without_traceback(
    parser, source, bundle_name, extra_args, message
):
    args = parser.parse_args(["dag-bundles", "publish-local", bundle_name, str(source), *extra_args])

    with pytest.raises(SystemExit, match=message):
        dag_bundle_command.dag_bundle_publish_local(args)


def test_publish_local_reports_overlapping_paths(parser, published_root):
    source = published_root / "source"
    source.mkdir(parents=True)
    (source / "dag.py").write_text("print('dag')")
    args = parser.parse_args(["dag-bundles", "publish-local", "manifest-local", str(source)])

    with pytest.raises(SystemExit, match="source_path must not be inside published_root"):
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
    }

    process = subprocess.run(
        [sys.executable, "-m", "airflow", "dag-bundles", "publish-local", "manifest-local", str(source)]
        + ["--output", "json"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert process.returncode == 0, process.stderr
    [result] = json.loads(process.stdout)
    assert (published_root / "versions" / "manifest-local" / result["version"] / "dag.py").exists()
    # SQLite creates the database file on first connection.
    assert not db_path.exists()
