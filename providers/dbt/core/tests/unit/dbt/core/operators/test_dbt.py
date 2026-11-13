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
"""Unit tests for :class:`DbtKubernetesRunOperator`.

Kubernetes is never contacted: the parent ``KubernetesPodOperator.execute`` is
replaced with a ``MagicMock`` so no pod is created, and every Airflow
connection / hook lookup is mocked. The tests assert on the operator attributes
(``cmds``, ``arguments``, ``env_vars``) that the operator sets *before* handing
off to ``super().execute()``.
"""

from __future__ import annotations

import json
import sys
from unittest import mock

import pytest

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.models.connection import Connection
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.dbt.core.operators import dbt as dbt_module
from airflow.providers.dbt.core.operators.dbt import DbtKubernetesRunOperator

# Where the operator module resolves the names it imports. Patch here so we
# affect the exact objects the operator uses (BaseHook is re-exported from
# version_compat and bound into the operator module namespace).
DBT_MODULE = "airflow.providers.dbt.core.operators.dbt"

IMAGE = "example.com/dbt-runner:latest"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _make_operator(**kwargs) -> DbtKubernetesRunOperator:
    """Build the operator with sensible required defaults."""
    kwargs.setdefault("task_id", "run_dbt")
    kwargs.setdefault("image", IMAGE)
    kwargs.setdefault("steps", ["dbt build"])
    return DbtKubernetesRunOperator(**kwargs)


def _make_conn(**attrs) -> mock.MagicMock:
    """A stand-in for an Airflow Connection with the attributes the operator reads."""
    conn = mock.MagicMock(spec=Connection)
    for attr in ("host", "login", "password", "schema", "port"):
        setattr(conn, attr, attrs.get(attr))
    conn.extra_dejson = attrs.get("extra_dejson", {})
    return conn


def _env_dict(op: DbtKubernetesRunOperator) -> dict[str, str]:
    """Convert the operator's ``list[V1EnvVar]`` back to a plain ``{name: value}`` dict."""
    return {var.name: var.value for var in (op.env_vars or [])}


def _make_context(
    *,
    dag_id: str = "my_dag",
    run_id: str = "run_1",
    try_number: int = 1,
    task_id: str = "run_dbt",
    group_id: str | None = None,
) -> dict:
    """Build a realistic ``execute`` context so ``_artifact_dest`` computes a real path.

    The operator reads ``context["dag_run"].dag_id`` / ``.run_id``,
    ``context["ti"].try_number`` and ``context["task"].task_id`` /
    ``.task_group.group_id``. When ``group_id`` is ``None`` the task is not in a
    task group; otherwise it belongs to a group whose ``group_id`` is the given
    string.
    """
    dag_run = mock.MagicMock(spec_set=["dag_id", "run_id"])
    dag_run.dag_id = dag_id
    dag_run.run_id = run_id

    ti = mock.MagicMock(spec_set=["try_number"])
    ti.try_number = try_number

    task = mock.MagicMock(spec_set=["task_id", "task_group"])
    task.task_id = task_id
    if group_id is None:
        task.task_group = None
    else:
        task_group = mock.MagicMock(spec_set=["group_id"])
        task_group.group_id = group_id
        task.task_group = task_group

    return {"dag_run": dag_run, "ti": ti, "task": task}


def _run_execute(op: DbtKubernetesRunOperator, context: dict | None = None):
    """Run ``execute`` with the parent pod-launching ``execute`` mocked out.

    Uses a realistic context (see :func:`_make_context`) unless one is supplied,
    so ``_artifact_dest`` produces a concrete per-run path.

    Returns the ``(result, super_execute_mock)`` tuple so callers can assert on
    the passthrough return value and that the pod launch was invoked exactly once.
    """
    if context is None:
        context = _make_context()
    with mock.patch.object(KubernetesPodOperator, "execute", return_value="POD_RESULT") as super_execute:
        result = op.execute(context=context)
    return result, super_execute


# --------------------------------------------------------------------------- #
# (a) command / arguments assembly
# --------------------------------------------------------------------------- #
def test_execute_assembles_bash_cmds_and_script_arguments():
    op = _make_operator(steps=["dbt build", "dbt test"])

    result, super_execute = _run_execute(op)

    super_execute.assert_called_once()
    assert result == "POD_RESULT"
    assert op.cmds == ["bash", "-c"]
    assert op.arguments == [dbt_module._ENTRYPOINT_SCRIPT]
    assert len(op.arguments) == 1
    assert "trap _upload_artifacts EXIT" in op.arguments[0]


def test_volume_mount_prepends_bin_dir_to_path():
    from kubernetes.client import models as k8s

    vol = k8s.V1Volume(name="aws-cli", empty_dir=k8s.V1EmptyDirVolumeSource())
    mount = k8s.V1VolumeMount(name="aws-cli", mount_path="/aws-cli")
    op = DbtKubernetesRunOperator(
        task_id="run_dbt",
        image=IMAGE,
        steps=["dbt build"],
        volumes=[vol],
        volume_mounts=[mount],
    )

    # PATH prepend is the first line; the entrypoint script follows.
    assert op.arguments[0].startswith('export PATH="/aws-cli/bin:')
    assert dbt_module._ENTRYPOINT_SCRIPT in op.arguments[0]


# --------------------------------------------------------------------------- #
# (b) core script_env
# --------------------------------------------------------------------------- #
def test_script_env_contains_steps_project_dir_and_install_deps():
    op = _make_operator(
        steps=["dbt build --select tag:hourly", "dbt test"],
        project_dir="/opt/dbt",
        install_deps=True,
    )

    _run_execute(op)
    env = _env_dict(op)

    assert env["DBT_STEPS"] == "dbt build --select tag:hourly\ndbt test"
    assert env["DBT_PROJECT_DIR"] == "/opt/dbt"
    assert env["INSTALL_DEPS"] == "1"


def test_install_deps_false_sets_empty_flag():
    op = _make_operator(install_deps=False)

    _run_execute(op)

    assert _env_dict(op)["INSTALL_DEPS"] == ""


def test_default_project_dir():
    op = _make_operator()

    _run_execute(op)

    assert _env_dict(op)["DBT_PROJECT_DIR"] == "/dbt"


def test_user_env_vars_preserved_before_script_env():
    from kubernetes.client import models as k8s

    op = _make_operator(env_vars=[k8s.V1EnvVar(name="EXTRA_ENV", value="keep-me")])

    _run_execute(op)
    env = _env_dict(op)

    assert env["EXTRA_ENV"] == "keep-me"
    assert env["DBT_STEPS"] == "dbt build"


# --------------------------------------------------------------------------- #
# (b2) command_prefix -> CMD_PREFIX
# --------------------------------------------------------------------------- #
def test_command_prefix_sets_cmd_prefix_env():
    op = _make_operator(command_prefix="uv run")

    _run_execute(op)

    assert _env_dict(op)["CMD_PREFIX"] == "uv run"


def test_command_prefix_defaults_to_empty_string():
    op = _make_operator()

    _run_execute(op)
    env = _env_dict(op)

    assert "CMD_PREFIX" in env
    assert env["CMD_PREFIX"] == ""


# --------------------------------------------------------------------------- #
# (c) git connection -> GIT_TOKEN
# --------------------------------------------------------------------------- #
def test_git_repo_with_conn_injects_token_and_branch():
    conn = _make_conn(password="ghp_secrettoken")
    with mock.patch.object(dbt_module.BaseHook, "get_connection", return_value=conn) as get_conn:
        op = _make_operator(
            git_repo_url="github.com/acme/dbt-project.git",
            git_branch="develop",
            git_conn_id="acme_git",
        )
        _run_execute(op)

    env = _env_dict(op)
    assert env["GIT_REPO_URL"] == "github.com/acme/dbt-project.git"
    assert env["GIT_BRANCH"] == "develop"
    assert env["GIT_TOKEN"] == "ghp_secrettoken"
    get_conn.assert_called_once_with("acme_git")


def test_git_repo_without_conn_has_no_token():
    op = _make_operator(git_repo_url="github.com/acme/dbt-project.git")

    _run_execute(op)
    env = _env_dict(op)

    assert env["GIT_REPO_URL"] == "github.com/acme/dbt-project.git"
    assert env["GIT_BRANCH"] == "main"
    assert "GIT_TOKEN" not in env


def test_no_git_repo_omits_git_env():
    op = _make_operator()

    _run_execute(op)
    env = _env_dict(op)

    assert "GIT_REPO_URL" not in env
    assert "GIT_BRANCH" not in env
    assert "GIT_TOKEN" not in env


# --------------------------------------------------------------------------- #
# (d) warehouse connection -> DBT_HOST / DBT_USER / ...
# --------------------------------------------------------------------------- #
def test_warehouse_conn_maps_to_dbt_profile_env():
    conn = _make_conn(
        host="warehouse.example.com",
        login="dbt_user",
        password="wh_pw",
        schema="analytics",
        port=5439,
    )
    with mock.patch.object(dbt_module.BaseHook, "get_connection", return_value=conn) as get_conn:
        op = _make_operator(warehouse_conn_id="snowflake_default")
        _run_execute(op)

    env = _env_dict(op)
    assert env["DBT_HOST"] == "warehouse.example.com"
    assert env["DBT_USER"] == "dbt_user"
    assert env["DBT_PASSWORD"] == "wh_pw"
    assert env["DBT_SCHEMA"] == "analytics"
    assert env["DBT_PORT"] == "5439"
    get_conn.assert_called_once_with("snowflake_default")


def test_warehouse_conn_omits_empty_fields():
    conn = _make_conn(host="warehouse.example.com")
    with mock.patch.object(dbt_module.BaseHook, "get_connection", return_value=conn):
        op = _make_operator(warehouse_conn_id="snowflake_default")
        _run_execute(op)

    env = _env_dict(op)
    assert env["DBT_HOST"] == "warehouse.example.com"
    for dropped in ("DBT_USER", "DBT_PASSWORD", "DBT_SCHEMA", "DBT_PORT"):
        assert dropped not in env


# --------------------------------------------------------------------------- #
# (e) artifact_dest scheme routing
# --------------------------------------------------------------------------- #
def test_artifact_s3_resolves_aws_credentials():
    creds = mock.MagicMock(spec_set=["access_key", "secret_key", "token"])
    creds.access_key = "AKIAEXAMPLE"
    creds.secret_key = "secretkey"
    creds.token = "sessiontoken"
    with mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook") as s3_hook:
        s3_hook.return_value.get_credentials.return_value = creds
        op = _make_operator(
            artifact_dest="s3://my-bucket/dbt/artifacts",
            artifact_conn_id="aws_default",
        )
        _run_execute(op)

    env = _env_dict(op)
    assert env["ARTIFACT_DEST"] == "s3://my-bucket/dbt/artifacts/my_dag/run_dbt/run_1/attempt_1"
    assert env["AWS_ACCESS_KEY_ID"] == "AKIAEXAMPLE"
    assert env["AWS_SECRET_ACCESS_KEY"] == "secretkey"
    assert env["AWS_SESSION_TOKEN"] == "sessiontoken"
    s3_hook.assert_called_once_with(aws_conn_id="aws_default")


def test_artifact_s3_without_session_token():
    creds = mock.MagicMock(spec_set=["access_key", "secret_key", "token"])
    creds.access_key = "AKIAEXAMPLE"
    creds.secret_key = "secretkey"
    creds.token = None
    with mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook") as s3_hook:
        s3_hook.return_value.get_credentials.return_value = creds
        op = _make_operator(
            artifact_dest="s3://my-bucket/dbt/artifacts",
            artifact_conn_id="aws_default",
        )
        _run_execute(op)

    env = _env_dict(op)
    assert env["AWS_ACCESS_KEY_ID"] == "AKIAEXAMPLE"
    assert "AWS_SESSION_TOKEN" not in env


def test_artifact_dest_without_conn_id_raises():
    with pytest.raises(ValueError, match="artifact_conn_id"):
        _make_operator(artifact_dest="s3://my-bucket/dbt/artifacts")


def test_artifact_gcs_resolves_keyfile_dict():
    keyfile = {"type": "service_account", "project_id": "my-proj"}
    conn = _make_conn(extra_dejson={"keyfile_dict": keyfile})
    with (
        mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook"),
        mock.patch.object(dbt_module.BaseHook, "get_connection", return_value=conn),
    ):
        op = _make_operator(
            artifact_dest="gs://my-bucket/dbt/artifacts",
            artifact_conn_id="google_cloud_default",
        )
        _run_execute(op)

    env = _env_dict(op)
    assert env["ARTIFACT_DEST"] == "gs://my-bucket/dbt/artifacts/my_dag/run_dbt/run_1/attempt_1"
    assert json.loads(env["GOOGLE_APPLICATION_CREDENTIALS_JSON"]) == keyfile


def test_artifact_gcs_accepts_json_string_keyfile():
    keyfile_str = json.dumps({"type": "service_account", "project_id": "my-proj"})
    conn = _make_conn(extra_dejson={"keyfile_dict": keyfile_str})
    with (
        mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook"),
        mock.patch.object(dbt_module.BaseHook, "get_connection", return_value=conn),
    ):
        op = _make_operator(
            artifact_dest="gs://my-bucket/dbt/artifacts",
            artifact_conn_id="google_cloud_default",
        )
        _run_execute(op)

    assert _env_dict(op)["GOOGLE_APPLICATION_CREDENTIALS_JSON"] == keyfile_str


def test_artifact_gcs_accepts_legacy_extra_key():
    keyfile = {"type": "service_account"}
    conn = _make_conn(extra_dejson={"extra__google_cloud_platform__keyfile_dict": keyfile})
    with (
        mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook"),
        mock.patch.object(dbt_module.BaseHook, "get_connection", return_value=conn),
    ):
        op = _make_operator(
            artifact_dest="gs://my-bucket/dbt/artifacts",
            artifact_conn_id="google_cloud_default",
        )
        _run_execute(op)

    assert json.loads(_env_dict(op)["GOOGLE_APPLICATION_CREDENTIALS_JSON"]) == keyfile


def test_artifact_gcs_missing_keyfile_raises():
    conn = _make_conn(extra_dejson={})
    with (
        mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook"),
        mock.patch.object(dbt_module.BaseHook, "get_connection", return_value=conn),
    ):
        op = _make_operator(
            artifact_dest="gs://my-bucket/dbt/artifacts",
            artifact_conn_id="google_cloud_default",
        )
        with pytest.raises(ValueError, match="keyfile_dict"):
            op.execute(context=_make_context())


def test_artifact_unknown_scheme_raises():
    op = _make_operator(
        artifact_dest="ftp://my-bucket/dbt/artifacts",
        artifact_conn_id="some_conn",
    )
    with pytest.raises(ValueError, match="s3:// or gs://"):
        op.execute(context=_make_context())


def test_artifact_s3_missing_amazon_provider_raises_optional_feature():
    op = _make_operator(
        artifact_dest="s3://my-bucket/dbt/artifacts",
        artifact_conn_id="aws_default",
    )
    with mock.patch.dict(sys.modules, {"airflow.providers.amazon.aws.hooks.s3": None}):
        with pytest.raises(AirflowOptionalProviderFeatureException):
            op.execute(context=_make_context())


def test_artifact_gcs_missing_google_provider_raises_optional_feature():
    op = _make_operator(
        artifact_dest="gs://my-bucket/dbt/artifacts",
        artifact_conn_id="google_cloud_default",
    )
    with mock.patch.dict(sys.modules, {"airflow.providers.google.cloud.hooks.gcs": None}):
        with pytest.raises(AirflowOptionalProviderFeatureException):
            op.execute(context=_make_context())


# --------------------------------------------------------------------------- #
# (e2) _artifact_dest per-run layout
# --------------------------------------------------------------------------- #
def _s3_op_with_creds(**kwargs):
    """Helper: operator with mocked S3 creds so artifact path tests don't need real AWS."""
    creds = mock.MagicMock(spec_set=["access_key", "secret_key", "token"])
    creds.access_key = "AK"
    creds.secret_key = "SK"
    creds.token = None
    kwargs.setdefault("artifact_dest", "s3://my-dbt-artifacts")
    kwargs.setdefault("artifact_conn_id", "aws_default")
    with mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook") as s3_hook:
        s3_hook.return_value.get_credentials.return_value = creds
        op = _make_operator(**kwargs)
    return op, s3_hook


def test_artifact_dest_layout_uses_task_id_when_not_in_group():
    op, s3_hook = _s3_op_with_creds()
    ctx = _make_context(
        dag_id="analytics",
        run_id="scheduled__2024-06-01",
        try_number=1,
        task_id="hourly_build",
        group_id=None,
    )
    with mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", s3_hook):
        _run_execute(op, context=ctx)

    assert (
        _env_dict(op)["ARTIFACT_DEST"]
        == "s3://my-dbt-artifacts/analytics/hourly_build/scheduled__2024-06-01/attempt_1"
    )


def test_artifact_dest_layout_uses_group_id_when_in_group():
    op, s3_hook = _s3_op_with_creds()
    ctx = _make_context(
        dag_id="analytics",
        run_id="scheduled__2024-06-01",
        try_number=3,
        task_id="hourly_build",
        group_id="dbt_group",
    )
    with mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", s3_hook):
        _run_execute(op, context=ctx)

    assert (
        _env_dict(op)["ARTIFACT_DEST"]
        == "s3://my-dbt-artifacts/analytics/dbt_group/scheduled__2024-06-01/attempt_3"
    )


def test_artifact_dest_base_trailing_slash_is_normalized():
    op, s3_hook = _s3_op_with_creds(artifact_dest="s3://my-dbt-artifacts/")
    ctx = _make_context(dag_id="d", run_id="r", try_number=2, task_id="t", group_id=None)
    with mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", s3_hook):
        _run_execute(op, context=ctx)

    assert _env_dict(op)["ARTIFACT_DEST"] == "s3://my-dbt-artifacts/d/t/r/attempt_2"


# --------------------------------------------------------------------------- #
# (e3) _git_cache_path format
# --------------------------------------------------------------------------- #
def test_git_cache_path_format_and_uniqueness():
    op, s3_hook = _s3_op_with_creds(
        git_repo_url="github.com/acme/dbt-project.git",
        git_branch="main",
        git_cache_dest="s3://my-cache",
    )
    ctx = _make_context(dag_id="sales_hourly")
    with mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", s3_hook):
        _run_execute(op, context=ctx)

    env = _env_dict(op)
    cache_path = env["GIT_CACHE_DEST"]
    # Structure: s3://my-cache/<dag_id>/<slug>-<8-char-hash>/repo.tar.gz
    assert cache_path.startswith("s3://my-cache/sales_hourly/")
    assert cache_path.endswith("/repo.tar.gz")
    # slug is derived from repo path + branch
    assert "acme-dbt-project-main" in cache_path


# --------------------------------------------------------------------------- #
# (f) validation
# --------------------------------------------------------------------------- #
def test_empty_steps_raises_value_error():
    with pytest.raises(ValueError, match="steps"):
        DbtKubernetesRunOperator(task_id="run_dbt", image=IMAGE, steps=[])


def test_git_cache_dest_without_git_repo_url_raises():
    with pytest.raises(ValueError, match="git_repo_url"):
        DbtKubernetesRunOperator(
            task_id="run_dbt",
            image=IMAGE,
            steps=["dbt build"],
            git_cache_dest="s3://my-cache",
            artifact_conn_id="aws_default",
        )


def test_git_conn_id_without_git_repo_url_raises():
    with pytest.raises(ValueError, match="git_repo_url"):
        DbtKubernetesRunOperator(
            task_id="run_dbt",
            image=IMAGE,
            steps=["dbt build"],
            git_conn_id="my_git_conn",
        )
