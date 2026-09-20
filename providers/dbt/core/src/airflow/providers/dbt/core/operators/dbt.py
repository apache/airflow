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
"""Run a dbt Core job inside a single Kubernetes pod."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.dbt.core.version_compat import BaseHook

if TYPE_CHECKING:
    from airflow.sdk import Context

_ENTRYPOINT_SCRIPT = (Path(__file__).parent / "run.sh").read_text()


class DbtKubernetesRunOperator(KubernetesPodOperator):
    """
    Run ordered dbt commands in a single Kubernetes pod.

    Lifecycle (each phase is a collapsible log group):
    clone → dbt deps → steps → upload artifacts

    All :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`
    parameters (``namespace``, ``kubernetes_conn_id``, ``container_resources``,
    ``deferrable``, ``secrets``, ``init_containers``, …) pass straight through via ``**kwargs``.

    Pre-execution system setup (installing tools, activating environments) should use
    native Kubernetes ``init_containers`` passed via ``**kwargs``. This keeps the operator
    lean and works with any package manager (uv, pip, poetry, conda, pre-baked images).

    :param steps: Ordered shell commands executed sequentially inside the pod. Typically
        dbt CLI commands e.g. ``["dbt build --select tag:hourly", "dbt test"]``, but any
        shell command is valid — leading setup steps like ``"pip install -r requirements.txt"``
        or ``"uv sync"`` are supported. Set ``install_deps=False`` when managing ``dbt deps``
        manually within steps.
    :param image: Container image with ``bash`` and ``git`` available. ``awscli``/``gsutil``
        required only when ``artifact_dest`` or ``git_cache_dest`` is set.
    :param project_dir: Path inside the pod where the dbt project lives (default ``/dbt``).
    :param install_deps: Run ``dbt deps`` before steps (default ``True``).
    :param command_prefix: Prefix prepended to every step, e.g. ``"uv run"`` (auto-syncs
        the project venv before each command), ``"poetry run"``, or ``"conda run -n myenv"``.
        Default empty (executables on PATH).
    :param git_repo_url: Repo to clone, e.g. ``"github.com/acme/dbt-project.git"``.
        Omit when the project is baked into the image.
    :param git_branch: Branch to clone (default ``"main"``).
    :param git_conn_id: Airflow connection whose ``password`` holds the git token.
        Alternative: pass ``GIT_TOKEN`` directly in ``env_vars``.
    :param git_cache_dest: Base ``s3://bucket/prefix`` or ``gs://bucket/prefix`` for git
        repo caching. When set: on a successful clone the repo is compressed and uploaded
        so subsequent runs can fall back to the cache if git is unreachable. The cache path
        is unique per DAG + repo + branch. Requires ``artifact_conn_id`` for credentials
        (or ambient IAM/Workload Identity on the pod).
    :param warehouse_conn_id: Connection mapped to ``DBT_HOST`` / ``DBT_USER`` /
        ``DBT_PASSWORD`` / ``DBT_SCHEMA`` / ``DBT_PORT`` for ``profiles.yml`` ``env_var()``
        lookups. Omit if credentials are supplied directly via ``env_vars`` or the
        ``profiles.yml`` uses a different auth mechanism.
    :param artifact_dest: Base ``s3://bucket/prefix`` or ``gs://bucket/prefix``.
        The operator appends ``/<dag_id>/<task_id>/<run_id>/attempt_<n>`` — each run and
        retry gets its own path. Upload runs on both success and failure. Omit to skip
        artifact upload entirely.
    :param artifact_conn_id: AWS or GCP connection used to resolve upload credentials for
        both ``artifact_dest`` and ``git_cache_dest``. Omit when the pod has ambient
        IAM / Workload Identity permissions.
    """

    template_fields: Sequence[str] = (
        *KubernetesPodOperator.template_fields,
        "steps",
        "git_repo_url",
        "git_branch",
        "git_cache_dest",
        "artifact_dest",
    )

    def __init__(
        self,
        *,
        steps: list[str],
        image: str,
        project_dir: str = "/dbt",
        install_deps: bool = True,
        command_prefix: str = "",
        git_repo_url: str | None = None,
        git_branch: str = "main",
        git_conn_id: str | None = None,
        git_cache_dest: str | None = None,
        warehouse_conn_id: str | None = None,
        artifact_dest: str | None = None,
        artifact_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("cmds", None)
        kwargs.pop("arguments", None)
        super().__init__(image=image, cmds=["bash", "-c"], arguments=[_ENTRYPOINT_SCRIPT], **kwargs)
        _bin_dirs = [f"{vm.mount_path}/bin" for vm in (self.volume_mounts or []) if vm.mount_path]
        if _bin_dirs:
            self.arguments = [f'export PATH="{":".join(_bin_dirs)}:${{PATH}}"\n' + _ENTRYPOINT_SCRIPT]
        if not steps:
            raise ValueError("steps must be a non-empty list of shell commands")
        if (artifact_dest or git_cache_dest) and not artifact_conn_id:
            raise ValueError("artifact_conn_id is required when artifact_dest or git_cache_dest is set.")
        if (git_cache_dest or git_conn_id) and not git_repo_url:
            raise ValueError("git_repo_url is required when git_cache_dest or git_conn_id is set.")
        self.steps = steps
        self.project_dir = project_dir
        self.install_deps = install_deps
        self.command_prefix = command_prefix
        self.git_repo_url = git_repo_url
        self.git_branch = git_branch
        self.git_conn_id = git_conn_id
        self.git_cache_dest = git_cache_dest
        self.warehouse_conn_id = warehouse_conn_id
        self.artifact_dest = artifact_dest
        self.artifact_conn_id = artifact_conn_id
        self._user_env_vars = list(self.env_vars or [])

    def execute(self, context: Context) -> Any:
        _raw_env: dict[str, str | None] = {
            "DBT_PROJECT_DIR": self.project_dir,
            "DBT_STEPS": "\n".join(self.steps),
            "INSTALL_DEPS": "1" if self.install_deps else "",
            "CMD_PREFIX": self.command_prefix,
            "GIT_REPO_URL": self.git_repo_url,
            "GIT_BRANCH": self.git_branch if self.git_repo_url else None,
            "GIT_CACHE_DEST": self._git_cache_path(context)
            if (self.git_cache_dest and self.git_repo_url)
            else None,
        }
        script_env: dict[str, str] = {k: v for k, v in _raw_env.items() if v is not None}
        if self.git_conn_id and self.git_repo_url:
            script_env["GIT_TOKEN"] = BaseHook.get_connection(self.git_conn_id).password or ""
        if self.warehouse_conn_id:
            script_env.update(self._warehouse_profile_env())
        if self.artifact_dest:
            script_env["ARTIFACT_DEST"] = self._build_artifact_path(context)
        # Credentials serve both artifact upload and git cache — inject once.
        _storage_dest = self.artifact_dest or self.git_cache_dest
        if _storage_dest and self.artifact_conn_id:
            script_env.update(self._storage_credentials_env(_storage_dest))

        self.env_vars = [*self._user_env_vars, *convert_env_vars(script_env)]
        return super().execute(context)

    def _build_artifact_path(self, context: Context) -> str:
        base = cast("str", self.artifact_dest).rstrip("/")
        dag_run = context["dag_run"]
        ti = context["ti"]
        task = context["task"]
        group = getattr(task, "task_group", None)
        leaf = getattr(group, "group_id", None) or task.task_id
        return f"{base}/{dag_run.dag_id}/{leaf}/{dag_run.run_id}/attempt_{ti.try_number}"

    def _git_cache_path(self, context: Context) -> str:
        git_repo_url = cast("str", self.git_repo_url)
        url = git_repo_url.removesuffix(".git")
        repo_path = url.split("/", 1)[-1] if "/" in url else url
        slug = re.sub(r"[^a-z0-9]+", "-", f"{repo_path}/{self.git_branch}".lower()).strip("-")
        short_hash = hashlib.sha256(f"{git_repo_url}:{self.git_branch}".encode()).hexdigest()[:8]
        dag_id = context["dag_run"].dag_id
        return f"{cast('str', self.git_cache_dest).rstrip('/')}/{dag_id}/{slug[:60]}-{short_hash}/repo.tar.gz"

    def _warehouse_profile_env(self) -> dict[str, str]:
        conn = BaseHook.get_connection(self.warehouse_conn_id)
        env = {
            "DBT_HOST": conn.host or "",
            "DBT_USER": conn.login or "",
            "DBT_PASSWORD": conn.password or "",
            "DBT_SCHEMA": conn.schema or "",
            "DBT_PORT": str(conn.port) if conn.port else "",
        }
        return {k: v for k, v in env.items() if v}

    def _storage_credentials_env(self, dest: str) -> dict[str, str]:
        scheme = urlparse(dest).scheme
        if scheme == "s3":
            try:
                from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            except ImportError as e:
                raise AirflowOptionalProviderFeatureException(e)
            creds = S3Hook(aws_conn_id=self.artifact_conn_id).get_credentials()
            env = {
                "AWS_ACCESS_KEY_ID": creds.access_key or "",
                "AWS_SECRET_ACCESS_KEY": creds.secret_key or "",
            }
            if creds.token:
                env["AWS_SESSION_TOKEN"] = creds.token
            return env
        if scheme == "gs":
            try:
                from airflow.providers.google.cloud.hooks.gcs import GCSHook  # noqa: F401
            except ImportError as e:
                raise AirflowOptionalProviderFeatureException(e)
            extra = BaseHook.get_connection(self.artifact_conn_id).extra_dejson
            keyfile = extra.get("keyfile_dict") or extra.get("extra__google_cloud_platform__keyfile_dict")
            if not keyfile:
                raise ValueError(
                    "GCS requires the connection to define a service-account "
                    "'keyfile_dict' (JSON) so credentials can be passed into the pod."
                )
            if isinstance(keyfile, dict):
                keyfile = json.dumps(keyfile)
            return {"GOOGLE_APPLICATION_CREDENTIALS_JSON": keyfile}
        raise ValueError(
            f"artifact_dest / git_cache_dest must start with s3:// or gs:// (got scheme {scheme!r})"
        )
