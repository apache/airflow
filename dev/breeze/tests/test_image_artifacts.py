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

import argparse
import hashlib
import io
import json
import zipfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from airflow_breeze.utils.image_artifacts import (
    DOWNLOAD_SPEED_PROBE_SECONDS,
    DownloadTooSlowError,
    GithubArtifacts,
    artifact_name,
    download,
    fingerprint,
    is_build_input,
    main,
    publication_exists,
    resolve,
    restore_selection,
    select_local,
)

REFERENCE_TIME = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def fixed_clock():
    """Keep freshness tests deterministic without adding a Breeze runtime dependency."""
    with patch("airflow_breeze.utils.image_artifacts.datetime", spec=datetime, wraps=datetime) as clock:
        clock.now.return_value = REFERENCE_TIME
        yield


@pytest.fixture
def inputs():
    return {"schema": 1, "kind": "ci", "python": "3.12", "platform": "linux/amd64", "fingerprint": "a" * 64}


@pytest.fixture
def artifact(inputs):
    return {
        "id": 123,
        "name": artifact_name(inputs),
        "expired": False,
        "created_at": REFERENCE_TIME.isoformat(),
        "workflow_run": {"id": 456, "head_sha": "b" * 40},
        "digest": "sha256:" + "c" * 64,
    }


@pytest.fixture
def run():
    return {
        "id": 456,
        "run_attempt": 1,
        "repository": {"full_name": "apache/airflow"},
        "head_repository": {"full_name": "apache/airflow"},
        "head_branch": "main",
        "event": "push",
        "path": ".github/workflows/publish-main-images.yml",
        "status": "completed",
        "conclusion": "success",
        "head_sha": "b" * 40,
    }


class TestFingerprint:
    @pytest.mark.parametrize(
        ("path", "ci", "prod"),
        [
            ("airflow-core/src/airflow/api_fastapi/app.py", False, True),
            ("airflow-core/src/airflow/ui/src/App.tsx", False, True),
            ("airflow-core/src/airflow/ui/package.json", True, True),
            ("providers/amazon/pyproject.toml", True, True),
            ("providers/amazon/provider.yaml", True, True),
            ("providers/amazon/src/airflow/providers/amazon/get_provider_info.py", True, True),
            ("shared/logging/src/airflow_shared/logging/foo.py", True, True),
            ("airflow-core/src/airflow/__init__.py", True, True),
            ("uv.lock", True, True),
            ("airflow-core/docs/installation.rst", False, False),
            ("providers/amazon/docs/operators/example.rst", False, False),
            ("task-sdk/newsfragments/123.bugfix.rst", False, False),
            ("airflow-core/newsfragments/123.feature.rst", False, False),
            ("docs/conf.py", False, False),
            ("dev/breeze/doc/ci/04_selective_checks.md", False, False),
            ("README.md", False, False),
            ("airflow-core/README.md", True, True),
            ("providers/amazon/README.rst", True, True),
            ("generated/PYPI_README.md", True, True),
            ("airflow-core/hatch_build.py", True, True),
            ("scripts/docker/install_airflow.sh", True, True),
            ("chart/templates/test.yaml", False, False),
            ("airflow-core/tests/unit/test_api.py", False, False),
        ],
    )
    def test_build_inputs(self, path, ci, prod):
        assert is_build_input(path, "ci") is ci
        assert is_build_input(path, "prod") is prod

    @patch("subprocess.check_output", autospec=True, return_value=b"")
    def test_constraints_ignore_generated_comments(self, _files, tmp_path):
        first = fingerprint(
            tmp_path, "prod", "3.12", "linux/amd64", constraints=b"# generated yesterday\na==1\n"
        )
        second = fingerprint(
            tmp_path, "prod", "3.12", "linux/amd64", constraints=b"# generated today\na==1\n"
        )
        changed = fingerprint(
            tmp_path, "prod", "3.12", "linux/amd64", constraints=b"# generated today\na==2\n"
        )
        assert first == second
        assert first != changed

    def test_actual_checkout_and_dimensions(self, tmp_path):
        lock = tmp_path / "uv.lock"
        lock.write_text("old")
        with patch("subprocess.check_output", autospec=True, return_value=b"uv.lock\0"):
            original = fingerprint(tmp_path, "ci", "3.12", "linux/amd64")
            lock.write_text("new")
            assert fingerprint(tmp_path, "ci", "3.12", "linux/amd64") != original
            lock.write_text("old")
            assert fingerprint(tmp_path, "ci", "3.13", "linux/amd64") != original
            assert fingerprint(tmp_path, "ci", "3.12", "linux/arm64") != original
            assert fingerprint(tmp_path, "ci", "3.12", "linux/amd64", ("mysql=8",)) != original
            assert fingerprint(tmp_path, "ci", "3.12", "linux/amd64", constraints=b"updated") != original
            lock.unlink()
            assert fingerprint(tmp_path, "ci", "3.12", "linux/amd64") != original


class TestTrustedProducer:
    def test_success(self, artifact, run):
        api = GithubArtifacts()
        api.get = Mock(spec=GithubArtifacts().get, return_value=run)
        assert api.validate(artifact, artifact["name"])["artifact-id"] == 123
        api.get.assert_called_once_with("actions/runs/456")

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            ("repository", {"full_name": "fork/airflow"}),
            ("head_repository", {"full_name": "fork/airflow"}),
            ("head_branch", "feature"),
            ("event", "pull_request"),
            ("path", ".github/workflows/ci-amd.yml"),
            ("status", "in_progress"),
            ("conclusion", "cancelled"),
            ("head_sha", "wrong"),
        ],
    )
    def test_reject_provenance(self, artifact, run, key, value):
        run[key] = value
        api = GithubArtifacts()
        api.get = Mock(spec=GithubArtifacts().get, return_value=run)
        with pytest.raises(ValueError, match="producer"):
            api.validate(artifact, artifact["name"])

    @pytest.mark.parametrize("age", [timedelta(hours=49), timedelta(hours=-1)])
    def test_reject_age(self, artifact, age):
        artifact["created_at"] = (REFERENCE_TIME - age).isoformat()
        with pytest.raises(ValueError, match="freshness"):
            GithubArtifacts().validate(artifact, artifact["name"])

    def test_reject_unverifiable_digest(self, artifact, run):
        artifact["digest"] = None
        api = GithubArtifacts()
        api.get = Mock(spec=GithubArtifacts().get, return_value=run)
        with pytest.raises(ValueError, match="digest"):
            api.validate(artifact, artifact["name"])

    def test_resolve_failure_is_miss(self, inputs):
        api = Mock(spec=GithubArtifacts)
        api.find.side_effect = requests.Timeout()
        assert resolve(inputs, api)["hit"] is False

    def test_disabled_does_not_lookup(self, inputs):
        api = Mock(spec=GithubArtifacts)
        assert resolve(inputs, api, disabled=True)["hit"] is False
        api.find.assert_not_called()

    def test_resolve_skips_bad_producer(self, inputs, artifact):
        api = Mock(spec=GithubArtifacts)
        api.find.return_value = [artifact, artifact]
        api.validate.side_effect = [ValueError("bad producer"), {"hit": True}]
        assert resolve(inputs, api)["hit"] is True


class TestDownload:
    def test_digest_mismatch(self):
        api = GithubArtifacts()
        response = MagicMock(spec=requests.Response)
        response.iter_content.return_value = [b"corrupt"]
        api.session.get = Mock(spec=api.session.get, return_value=response)
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        with pytest.raises(ValueError, match="digest mismatch"), api.archive({"id": 1, "digest": "wrong"}):
            pytest.fail("must not open corrupt archive")

    def test_slow_download_aborts_without_retry(self):
        api = GithubArtifacts()
        response = MagicMock(spec=requests.Response)
        response.iter_content.return_value = [b"x" * 100]
        api.session.get = Mock(spec=api.session.get, return_value=response)
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        with patch(
            "airflow_breeze.utils.image_artifacts.time.monotonic",
            side_effect=[0, DOWNLOAD_SPEED_PROBE_SECONDS],
        ):
            with pytest.raises(DownloadTooSlowError, match="MB/s"):
                with api.archive({"id": 1, "digest": "sha256:" + "a" * 64}):
                    pytest.fail("must not open archive when download is too slow")
        api.session.get.assert_called_once()

    def test_download_at_or_above_speed_floor_continues(self):
        api = GithubArtifacts()
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w"):
            pass
        chunk = payload.getvalue()
        response = MagicMock(spec=requests.Response)
        response.iter_content.return_value = [chunk]
        api.session.get = Mock(spec=api.session.get, return_value=response)
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        digest = "sha256:" + hashlib.sha256(chunk).hexdigest()
        with (
            patch("airflow_breeze.utils.image_artifacts.DOWNLOAD_SPEED_PROBE_SECONDS", 1),
            patch("airflow_breeze.utils.image_artifacts.MIN_DOWNLOAD_SPEED_BYTES_PER_SECOND", len(chunk)),
            patch("airflow_breeze.utils.image_artifacts.time.monotonic", side_effect=[0, 1]),
        ):
            with api.archive({"id": 1, "digest": digest}) as archive:
                assert archive.namelist() == []

    def test_selected_image_remains_downloadable_after_freshness_window(
        self, inputs, artifact, run, tmp_path
    ):
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, return_value=run)
        artifact["created_at"] = (REFERENCE_TIME - timedelta(hours=47)).isoformat()
        selection = {**inputs, **api.validate(artifact, artifact["name"])}
        artifact["created_at"] = (REFERENCE_TIME - timedelta(hours=49)).isoformat()
        with pytest.raises(ValueError, match="freshness"):
            api.validate(artifact, artifact["name"])
        api.get = Mock(spec=GithubArtifacts().get, side_effect=[artifact, run])
        archive = MagicMock(spec=zipfile.ZipFile)
        archive.infolist.return_value = []
        api.archive = Mock(spec=api.archive, return_value=archive)
        archive.__enter__.return_value = archive
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", autospec=True, return_value=api):
            download(selection, tmp_path)
        archive.extractall.assert_called_once_with(tmp_path)

    @pytest.mark.parametrize("member", ["../escape", "/tmp/escape"])
    def test_reject_archive_escape(self, inputs, artifact, run, tmp_path, member):
        api = GithubArtifacts()
        api.get = Mock(spec=GithubArtifacts().get, return_value=run)
        selection = {**inputs, **api.validate(artifact, artifact["name"])}
        api.get = Mock(spec=GithubArtifacts().get, side_effect=[artifact, run])

        @contextmanager
        def archive(_artifact):
            payload = io.BytesIO()
            with zipfile.ZipFile(payload, "w") as zipped:
                zipped.writestr(member, "bad")
            payload.seek(0)
            with zipfile.ZipFile(payload) as zipped:
                yield zipped

        api.archive = archive
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", autospec=True, return_value=api):
            with pytest.raises(ValueError, match="archive member"):
                download(selection, tmp_path)


class TestRestoreSelection:
    def test_missing_selection_uses_existing_build(self, tmp_path):
        api = Mock(spec=GithubArtifacts)
        api.find.return_value = []
        args = argparse.Namespace(
            repository="fork/airflow",
            kind="ci",
            python="3.12",
            platform="linux/amd64",
            run_id=100,
            run_attempt=2,
            require_selection=False,
            output_directory=tmp_path,
        )
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", autospec=True, return_value=api):
            assert restore_selection(args)["hit"] is False
        api.find.assert_called_once_with("selected-ci-3.12-amd64-", 100, prefix=True)

    def test_lookup_error_must_not_silently_restore_old_image(self, tmp_path):
        api = Mock(spec=GithubArtifacts)
        api.find.side_effect = requests.Timeout("unavailable")
        args = argparse.Namespace(
            repository="fork/airflow",
            kind="ci",
            python="3.12",
            platform="linux/amd64",
            run_id=100,
            run_attempt=2,
            require_selection=False,
            output_directory=tmp_path,
        )
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", autospec=True, return_value=api):
            with pytest.raises(requests.Timeout, match="unavailable"):
                restore_selection(args)


class TestLocalSelection:
    def test_select_current_run(self, artifact, run):
        artifact["name"] = "built-ci-3.12-amd64-1"
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, side_effect=[artifact, run])
        selection = select_local(api, 123, 456, "ci", "3.12", "linux/amd64")
        assert selection["scope"] == "current-run"
        assert selection["artifact-id"] == 123

    @pytest.mark.parametrize("wrong", ["run", "repository", "sha", "name", "expired", "digest"])
    def test_reject_wrong_provenance(self, artifact, run, wrong):
        artifact["name"] = "built-ci-3.12-amd64-1"
        if wrong == "run":
            artifact["workflow_run"]["id"] = 999
        elif wrong == "repository":
            run["repository"]["full_name"] = "another/repository"
        elif wrong == "sha":
            artifact["workflow_run"]["head_sha"] = "wrong"
        elif wrong == "name":
            artifact["name"] = "main-image-ci-other"
        elif wrong == "expired":
            artifact["expired"] = True
        else:
            artifact["digest"] = None
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, side_effect=[artifact, run])
        with pytest.raises(ValueError, match="current workflow run"):
            select_local(api, 123, 456, "ci", "3.12", "linux/amd64")

    @pytest.mark.parametrize(
        ("repository", "run_id"), [("another/repo", 456), ("apache/airflow", 999), (None, None)]
    )
    def test_manifest_cannot_authorize_another_run(self, artifact, run, tmp_path, repository, run_id):
        artifact["name"] = "built-ci-3.12-amd64-1"
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, side_effect=[artifact, run])
        selection = select_local(api, 123, 456, "ci", "3.12", "linux/amd64")
        with pytest.raises(ValueError, match="consumer workflow run"):
            download(selection, tmp_path, repository, run_id)


class TestRequiredSelection:
    def test_missing_is_error(self, tmp_path):
        args = argparse.Namespace(
            repository="apache/airflow",
            kind="ci",
            python="3.12",
            platform="linux/amd64",
            run_id=456,
            run_attempt=2,
            require_selection=True,
            output_directory=tmp_path,
        )
        api = Mock(spec=GithubArtifacts)
        api.find.return_value = []
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", autospec=True, return_value=api):
            with pytest.raises(ValueError, match="Required image selection"):
                restore_selection(args)

    def test_previous_attempt_selected_before_newer_future_attempt(self, tmp_path, inputs):
        args = argparse.Namespace(
            repository="apache/airflow",
            kind="ci",
            python="3.12",
            platform="linux/amd64",
            run_id=456,
            run_attempt=2,
            require_selection=True,
            output_directory=tmp_path,
        )
        api = Mock(spec=GithubArtifacts)
        previous = {"id": 1, "name": "selected-ci-3.12-amd64-1"}
        api.find.return_value = [
            {"id": 3, "name": "selected-ci-3.12-amd64-3"},
            previous,
        ]
        selection = {**inputs, "hit": True}

        @contextmanager
        def archive(artifact):
            assert artifact == previous
            payload = io.BytesIO()
            with zipfile.ZipFile(payload, "w") as zipped:
                zipped.writestr("selection.json", json.dumps(selection))
            payload.seek(0)
            with zipfile.ZipFile(payload) as zipped:
                yield zipped

        api.archive = archive
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", autospec=True, return_value=api):
            with patch("airflow_breeze.utils.image_artifacts.download", autospec=True) as restore:
                assert restore_selection(args) == selection
        restore.assert_called_once_with(selection, tmp_path, "apache/airflow", 456)

    def test_slow_download_falls_back_to_build_instead_of_failing(self, tmp_path, inputs):
        args = argparse.Namespace(
            repository="apache/airflow",
            kind="ci",
            python="3.12",
            platform="linux/amd64",
            run_id=456,
            run_attempt=2,
            require_selection=True,
            output_directory=tmp_path,
        )
        api = Mock(spec=GithubArtifacts)
        selected = {"id": 1, "name": "selected-ci-3.12-amd64-1"}
        api.find.return_value = [selected]
        selection = {**inputs, "hit": True}

        @contextmanager
        def archive(artifact):
            assert artifact == selected
            payload = io.BytesIO()
            with zipfile.ZipFile(payload, "w") as zipped:
                zipped.writestr("selection.json", json.dumps(selection))
            payload.seek(0)
            with zipfile.ZipFile(payload) as zipped:
                yield zipped

        api.archive = archive
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", autospec=True, return_value=api):
            with patch(
                "airflow_breeze.utils.image_artifacts.download",
                autospec=True,
                side_effect=DownloadTooSlowError("too slow"),
            ):
                assert restore_selection(args) == {"hit": False, "reason": "too slow"}


class TestCLI:
    @pytest.mark.parametrize(
        "command", ["fingerprint", "resolve", "download", "restore-selection", "select-local"]
    )
    def test_required_arguments_are_reported(self, command, tmp_path, capsys):
        with patch("sys.argv", ["image_artifacts", command, "--output", str(tmp_path / "out.json")]):
            with pytest.raises(SystemExit) as error:
                main()
        assert error.value.code == 2
        assert f"{command} requires" in capsys.readouterr().err


class TestArtifactListing:
    def test_incomplete_listing_cannot_mean_miss(self):
        api = GithubArtifacts()
        item = {"name": "unrelated", "expired": False, "created_at": "2026-09-09"}
        api.get = Mock(spec=api.get, return_value={"artifacts": [item] * 100})
        with pytest.raises(ValueError, match="lookup limit"):
            api.find("selected-ci-", 456, prefix=True)

    def test_exact_name_search_uses_server_filter(self):
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, return_value={"artifacts": []})
        assert api.find("main-image-ci-fingerprint") == []
        api.get.assert_called_once_with(
            "actions/artifacts", per_page=100, page=1, name="main-image-ci-fingerprint"
        )


class TestPublicationExists:
    @pytest.mark.parametrize("present", [True, False])
    def test_current_run_artifact_can_survive_publisher_retry(self, artifact, run, present):
        run["status"] = "in_progress"
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, return_value=run)
        api.find = Mock(spec=api.find, return_value=[artifact] if present else [])
        assert publication_exists(api, artifact["name"], 456) is present
        api.find.assert_called_once_with(artifact["name"], 456)

    def test_other_workflow_cannot_skip_publication(self, artifact, run):
        run["path"] = ".github/workflows/ci-amd.yml"
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, return_value=run)
        with pytest.raises(ValueError, match="main publisher"):
            publication_exists(api, artifact["name"], 456)

    def test_other_run_artifact_does_not_skip_publication(self, artifact, run):
        artifact["workflow_run"]["id"] = 999
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, return_value=run)
        api.find = Mock(spec=api.find, return_value=[artifact])
        assert not publication_exists(api, artifact["name"], 456)


class TestPublisherAttempt:
    @pytest.mark.parametrize(("status", "conclusion"), [("in_progress", None), ("completed", "failure")])
    def test_selected_success_survives_a_publisher_retry(
        self, inputs, artifact, run, tmp_path, status, conclusion
    ):
        api = GithubArtifacts()
        api.get = Mock(spec=api.get, return_value=run)
        selection = {**inputs, **api.validate(artifact, artifact["name"])}
        retried_run = {**run, "run_attempt": 2, "status": status, "conclusion": conclusion}
        responses = {
            "actions/artifacts/123": artifact,
            "actions/runs/456": retried_run,
            "actions/runs/456/attempts/1": run,
        }
        api.get = Mock(spec=GithubArtifacts().get, side_effect=responses.__getitem__)
        archive = MagicMock(spec=zipfile.ZipFile)
        archive.infolist.return_value = []
        archive.__enter__.return_value = archive
        api.archive = Mock(spec=api.archive, return_value=archive)
        with patch("airflow_breeze.utils.image_artifacts.GithubArtifacts", autospec=True, return_value=api):
            download(selection, tmp_path)
        api.get.assert_any_call("actions/runs/456/attempts/1")
        archive.extractall.assert_called_once_with(tmp_path)

    @pytest.mark.parametrize(
        ("attempt", "actual_attempt", "conclusion"),
        [(2, 1, "success"), (2, 2, "failure"), (0, 1, "success"), (True, 1, "success")],
    )
    def test_forged_or_unsuccessful_attempt_is_rejected(
        self, artifact, run, attempt, actual_attempt, conclusion
    ):
        api = GithubArtifacts()
        api.get = Mock(
            spec=api.get, return_value={**run, "run_attempt": actual_attempt, "conclusion": conclusion}
        )
        with pytest.raises(ValueError, match="publisher"):
            api.validate(artifact, artifact["name"], check_freshness=False, run_attempt=attempt)
