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

import io
import json
import subprocess
import tempfile
import zipfile
from unittest import mock

import pytest
import requests
import yaml

from airflow_breeze.utils.github import (
    PR_IMAGES_WORKFLOW,
    download_artifact_from_pr,
    download_artifact_from_run_id,
    download_constraints_file,
    download_file_from_github,
    env_without_github_tokens,
    format_github_token_scope_guidance,
    get_branch_commit,
    retrieve_github_token,
    run_gh_command,
)
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.shared_options import set_dry_run

IMAGE_PREFIX = "ci-image-save-v3-linux_amd64-3.11"
IMAGE_ARTIFACT = f"{IMAGE_PREFIX}-74488_merge"
PR_URL = "https://api.github.com/repos/apache/airflow/pulls/74488"
WORKFLOW_RUNS_URL = "https://api.github.com/repos/apache/airflow/actions/workflows/ci-amd.yml/runs"
ARTIFACTS_URL = "https://api.github.com/repos/apache/airflow/actions/runs/123/artifacts?per_page=100"
ZIP_URL = "https://api.github.com/repos/apache/airflow/actions/artifacts/1/zip"


def _completed_process(returncode: int, stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["gh"], returncode=returncode, stdout=stdout, stderr="")


def make_response(
    body: dict | bytes = b"", status_code: int = 200, next_url: str | None = None
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.raw = io.BytesIO(json.dumps(body).encode() if isinstance(body, dict) else body)
    if next_url:
        response.headers["Link"] = f'<{next_url}>; rel="next"'
    return response


def make_artifacts_response(*names: str, next_url: str | None = None) -> requests.Response:
    artifacts = [{"name": name, "archive_download_url": ZIP_URL} for name in names]
    return make_response({"artifacts": artifacts}, next_url=next_url)


def make_zip_response(*members: str) -> requests.Response:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for member in members:
            archive.writestr(member, f"content of {member}")
    return make_response(buffer.getvalue())


def answer_github_requests(mock_get, responses: dict[str, requests.Response]) -> None:
    """Answer each request by its URL. A request for a URL without an answer fails the test with a KeyError."""
    mock_get.side_effect = lambda _session, url, **_: responses[url]


def get_printed_text(mock_console_print) -> str:
    return "\n".join(call.args[0] for call in mock_console_print.call_args_list)


def test_env_without_github_tokens_removes_ambient_token_vars(monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "gh-token")
    monkeypatch.setenv("GITHUB_TOKEN", "github-token")
    monkeypatch.setenv("OTHER_VAR", "kept")

    cleaned_env = env_without_github_tokens()

    assert "GH_TOKEN" not in cleaned_env
    assert "GITHUB_TOKEN" not in cleaned_env
    assert cleaned_env["OTHER_VAR"] == "kept"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_prefers_clean_gh_auth_token(mock_run, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "env-gh-token")
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.return_value = _completed_process(returncode=0, stdout="stored-gh-token\n")

    assert retrieve_github_token(description="airflow-ci-upgrade", scopes="public_repo") == "stored-gh-token"

    mock_run.assert_called_once()
    call_env = mock_run.call_args.kwargs["env"]
    assert "GH_TOKEN" not in call_env
    assert "GITHUB_TOKEN" not in call_env


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_falls_back_to_env_token(mock_run, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "env-gh-token")
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.return_value = _completed_process(returncode=1)

    assert retrieve_github_token(description="airflow-ci-upgrade", scopes="public_repo") == "env-gh-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_falls_back_to_env_token_when_gh_is_missing(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.side_effect = FileNotFoundError

    assert retrieve_github_token() == "env-github-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_falls_back_to_env_token_when_gh_returns_whitespace(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.return_value = _completed_process(returncode=0, stdout="  \n")

    assert retrieve_github_token() == "env-github-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_keeps_explicit_token(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-token")

    assert (
        retrieve_github_token("explicit-token", description="airflow-ci-upgrade", scopes="public_repo")
        == "explicit-token"
    )

    mock_run.assert_not_called()


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_does_not_treat_env_token_argument_as_explicit(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-token")
    mock_run.return_value = _completed_process(returncode=0, stdout="stored-gh-token\n")

    assert retrieve_github_token("env-token") == "stored-gh-token"


def test_format_github_token_scope_guidance_includes_description_and_scope():
    assert (
        format_github_token_scope_guidance(description="airflow-ci-upgrade", scopes="public_repo")
        == "If creating a token manually for airflow-ci-upgrade, it needs public_repo scope."
    )


def test_format_github_token_scope_guidance_is_generic_without_metadata():
    guidance = format_github_token_scope_guidance()

    assert (
        guidance == "If creating a token manually, make sure it has the permissions required by the command."
    )


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_retries_with_original_env_after_clean_env_failure(mock_run, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "env-gh-token")
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.side_effect = [
        _completed_process(returncode=1),
        _completed_process(returncode=0),
    ]

    result = run_gh_command(["gh", "workflow", "run", "docs.yml"], capture_output=True)

    assert result.returncode == 0
    assert mock_run.call_count == 2
    first_env = mock_run.call_args_list[0].kwargs["env"]
    second_env = mock_run.call_args_list[1].kwargs["env"]
    assert "GH_TOKEN" not in first_env
    assert "GITHUB_TOKEN" not in first_env
    assert second_env["GH_TOKEN"] == "env-gh-token"
    assert second_env["GITHUB_TOKEN"] == "env-github-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_does_not_retry_after_clean_env_success(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-token")
    mock_run.return_value = _completed_process(returncode=0)

    result = run_gh_command(["gh", "api", "repos/apache/airflow"], capture_output=True)

    assert result.returncode == 0
    mock_run.assert_called_once()


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_raises_when_check_true_and_no_env_token_to_retry(mock_run, monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    mock_run.return_value = _completed_process(returncode=1)

    with pytest.raises(subprocess.CalledProcessError) as ctx:
        run_gh_command(["gh", "api", "repos/apache/airflow"], capture_output=True, check=True)

    assert ctx.value.returncode == 1


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_raises_when_gh_is_missing(mock_run):
    mock_run.side_effect = FileNotFoundError

    with pytest.raises(FileNotFoundError):
        run_gh_command(["gh", "api", "repos/apache/airflow"], capture_output=True)


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_skips_subprocess_in_dry_run(mock_run):
    set_dry_run(True)
    try:
        result = run_gh_command(["gh", "workflow", "run", "docs.yml"], capture_output=True)
    finally:
        set_dry_run(False)

    assert result.returncode == 0
    mock_run.assert_not_called()


@mock.patch("requests.get")
def test_get_branch_commit_returns_the_commit_sha(mock_get, monkeypatch):
    monkeypatch.delenv("CONSTRAINTS_GITHUB_REPOSITORY", raising=False)
    mock_get.return_value = mock.Mock(status_code=200, text="c" * 40 + "\n")

    assert get_branch_commit("constraints-main", github_token="token") == "c" * 40
    assert (
        mock_get.call_args.args[0] == "https://api.github.com/repos/apache/airflow/commits/constraints-main"
    )
    assert mock_get.call_args.kwargs["headers"]["Authorization"] == "Bearer token"


@pytest.mark.parametrize(
    "response",
    [
        mock.Mock(status_code=403, text="", headers={}, **{"json.return_value": {"message": "rate limit"}}),
        mock.Mock(status_code=200, text="<html>not a sha</html>", headers={}),
    ],
)
@mock.patch("requests.get")
def test_get_branch_commit_returns_none_instead_of_exiting(mock_get, response):
    mock_get.return_value = response

    assert get_branch_commit("constraints-main") is None


@mock.patch("requests.get")
def test_get_branch_commit_returns_none_when_github_is_unreachable(mock_get):
    import requests

    mock_get.side_effect = requests.ConnectionError("unreachable")

    assert get_branch_commit("constraints-main") is None


@pytest.mark.parametrize(
    ("constraints_github_repository", "expected_repository"),
    [(None, "apache/airflow"), ("", "apache/airflow"), ("my-org/airflow", "my-org/airflow")],
)
@mock.patch("requests.get")
def test_constraints_are_read_from_constraints_github_repository(
    mock_get, constraints_github_repository, expected_repository, monkeypatch, tmp_path
):
    if constraints_github_repository is None:
        monkeypatch.delenv("CONSTRAINTS_GITHUB_REPOSITORY", raising=False)
    else:
        monkeypatch.setenv("CONSTRAINTS_GITHUB_REPOSITORY", constraints_github_repository)
    mock_get.return_value = mock.Mock(status_code=200, text="c" * 40, content=b"pkg-a==1.0.0\n")

    assert get_branch_commit("constraints-main") == "c" * 40
    assert download_constraints_file(
        constraints_reference="c" * 40,
        python_version="3.14",
        github_token=None,
        airflow_constraints_mode="constraints",
        output_file=tmp_path / "constraints.txt",
    )
    assert [call.args[0] for call in mock_get.call_args_list] == [
        f"https://api.github.com/repos/{expected_repository}/commits/constraints-main",
        f"https://api.github.com/repos/{expected_repository}/contents/constraints-3.14.txt?ref={'c' * 40}",
    ]


@mock.patch("requests.get")
def test_other_files_are_still_downloaded_from_apache_airflow(mock_get, monkeypatch, tmp_path):
    monkeypatch.setenv("CONSTRAINTS_GITHUB_REPOSITORY", "my-org/airflow")
    mock_get.return_value = mock.Mock(status_code=200, content=b"{}")

    assert download_file_from_github(
        reference="3.1.0",
        path="airflow-core/src/airflow/ui/package.json",
        output_file=tmp_path / "package.json",
    )
    assert mock_get.call_args.args[0] == (
        "https://api.github.com/repos/apache/airflow/contents/airflow-core/src/airflow/ui/package.json?ref=3.1.0"
    )


@pytest.mark.parametrize(
    "member",
    [
        pytest.param("ci-image-save-v3-linux_amd64-custom-3.11.tar", id="same-name-as-the-output-file"),
        pytest.param("ci-image-save-v3-linux_amd64-3.11.tar", id="other-name-than-the-output-file"),
    ],
)
@mock.patch("requests.Session.get", autospec=True)
def test_download_artifact_from_run_id_stores_the_image_at_the_output_file(
    mock_get, member, tmp_path, monkeypatch
):
    answer_github_requests(
        mock_get,
        {ARTIFACTS_URL: make_artifacts_response(IMAGE_ARTIFACT), ZIP_URL: make_zip_response(member)},
    )
    output_file = tmp_path / "images" / "ci-image-save-v3-linux_amd64-custom-3.11.tar"
    # --image-file-dir and the temp dir can be on different disks, and os.replace fails in that case.
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "missing-temp-dir"))

    download_artifact_from_run_id("123", IMAGE_PREFIX, output_file, "apache/airflow", "token")

    assert output_file.read_text() == f"content of {member}"
    assert list(output_file.parent.iterdir()) == [output_file]


@mock.patch("requests.Session.get", autospec=True)
def test_download_artifact_from_run_id_reads_all_pages_of_artifacts(mock_get, tmp_path):
    second_page_url = f"{ARTIFACTS_URL}&page=2"
    answer_github_requests(
        mock_get,
        {
            ARTIFACTS_URL: make_artifacts_response("test-results", next_url=second_page_url),
            second_page_url: make_artifacts_response(IMAGE_ARTIFACT),
            ZIP_URL: make_zip_response("image.tar"),
        },
    )
    output_file = tmp_path / "image.tar"

    download_artifact_from_run_id("123", IMAGE_PREFIX, output_file, "apache/airflow", "token")

    assert output_file.exists()


@mock.patch("airflow_breeze.utils.github.console_print", autospec=True)
@mock.patch("requests.Session.get", autospec=True)
def test_download_artifact_from_run_id_explains_a_digest_of_a_runson_run(
    mock_get, mock_console_print, tmp_path
):
    digest_artifact = f"{IMAGE_PREFIX}-s3-76225_merge"
    answer_github_requests(
        mock_get,
        {ARTIFACTS_URL: make_artifacts_response(digest_artifact), ZIP_URL: make_zip_response("sha256")},
    )
    output_file = tmp_path / "image.tar"

    with pytest.raises(SystemExit) as exit_info:
        download_artifact_from_run_id("123", IMAGE_PREFIX, output_file, "apache/airflow", "token")

    assert exit_info.value.code == 1
    printed = get_printed_text(mock_console_print)
    assert f"Artifact {digest_artifact} of run 123 holds no image" in printed
    assert "RunsOn" in printed
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("artifacts", "expected_listing"),
    [
        pytest.param(
            ["test-results", "ci-image-save-v3-linux_amd64-3.12-74488_merge"],
            "Image artifacts of this run: ci-image-save-v3-linux_amd64-3.12-74488_merge.",
            id="image-of-another-python-version",
        ),
        pytest.param(["test-results"], None, id="no-image-at-all"),
    ],
)
@mock.patch("airflow_breeze.utils.github.console_print", autospec=True)
@mock.patch("requests.Session.get", autospec=True)
def test_download_artifact_from_run_id_exits_when_no_artifact_matches(
    mock_get, mock_console_print, artifacts, expected_listing, tmp_path
):
    answer_github_requests(mock_get, {ARTIFACTS_URL: make_artifacts_response(*artifacts)})

    with pytest.raises(SystemExit) as exit_info:
        download_artifact_from_run_id("123", IMAGE_PREFIX, tmp_path / "image.tar", "apache/airflow", "token")

    assert exit_info.value.code == 1
    printed = get_printed_text(mock_console_print)
    assert f"Run 123 has no artifact for {IMAGE_PREFIX}" in printed
    if expected_listing:
        assert expected_listing in printed
    else:
        assert "Image artifacts of this run" not in printed


@mock.patch("requests.Session.get", autospec=True)
def test_download_artifact_from_pr_uses_the_latest_run_of_the_head_commit(mock_get, tmp_path):
    answer_github_requests(
        mock_get,
        {
            PR_URL: make_response({"head": {"sha": "abc123", "ref": "main"}}),
            WORKFLOW_RUNS_URL: make_response(
                {
                    "workflow_runs": [
                        {"id": 122, "created_at": "2026-11-01T10:00:00Z"},
                        {"id": 123, "created_at": "2026-11-02T10:00:00Z"},
                        {"id": 121, "created_at": "2026-10-31T10:00:00Z"},
                    ]
                }
            ),
            ARTIFACTS_URL: make_artifacts_response(IMAGE_ARTIFACT),
            ZIP_URL: make_zip_response("image.tar"),
        },
    )
    output_file = tmp_path / "image.tar"

    download_artifact_from_pr("#74488", IMAGE_PREFIX, output_file, "apache/airflow", "token")

    assert output_file.exists()
    runs_request = next(call for call in mock_get.call_args_list if call.args[1] == WORKFLOW_RUNS_URL)
    assert runs_request.kwargs["params"] == {"event": "pull_request", "head_sha": "abc123"}


@mock.patch("airflow_breeze.utils.github.console_print", autospec=True)
@mock.patch("requests.Session.get", autospec=True)
def test_download_artifact_from_pr_exits_when_the_head_commit_has_no_run(
    mock_get, mock_console_print, tmp_path
):
    answer_github_requests(
        mock_get,
        {
            PR_URL: make_response({"head": {"sha": "abc123", "ref": "main"}}),
            WORKFLOW_RUNS_URL: make_response({"workflow_runs": []}),
        },
    )

    with pytest.raises(SystemExit) as exit_info:
        download_artifact_from_pr("74488", IMAGE_PREFIX, tmp_path / "image.tar", "apache/airflow", "token")

    assert exit_info.value.code == 1
    assert f"PR 74488 has no {PR_IMAGES_WORKFLOW} run for its head commit abc123" in get_printed_text(
        mock_console_print
    )


@mock.patch("airflow_breeze.utils.github.console_print", autospec=True)
@mock.patch("requests.Session.get", autospec=True)
def test_download_artifact_from_pr_prints_why_workflow_runs_cannot_be_read(
    mock_get, mock_console_print, tmp_path
):
    answer_github_requests(
        mock_get,
        {
            PR_URL: make_response({"head": {"sha": "abc123", "ref": "main"}}),
            WORKFLOW_RUNS_URL: make_response(b"Bad credentials", status_code=401),
        },
    )

    with pytest.raises(SystemExit) as exit_info:
        download_artifact_from_pr("74488", IMAGE_PREFIX, tmp_path / "image.tar", "apache/airflow", "token")

    assert exit_info.value.code == 1
    assert "status code 401: Bad credentials" in get_printed_text(mock_console_print)


def test_pr_images_workflow_is_the_workflow_that_builds_images_for_pull_requests():
    workflows_building_images_for_prs = []
    for path in (AIRFLOW_ROOT_PATH / ".github" / "workflows").glob("*.yml"):
        workflow = yaml.safe_load(path.read_text())
        # YAML reads an unquoted `on` key as the boolean true.
        triggers = workflow[True] if True in workflow else workflow["on"]
        jobs = workflow["jobs"].values()
        if "pull_request" in triggers and any(
            job.get("uses", "").endswith("-image-build.yml") for job in jobs
        ):
            workflows_building_images_for_prs.append(path.name)

    assert workflows_building_images_for_prs == [PR_IMAGES_WORKFLOW]
