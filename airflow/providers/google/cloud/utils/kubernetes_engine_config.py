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

import functools
import logging
import os
import tempfile
import uuid
from contextlib import contextmanager
from typing import Generator, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.process_utils import execute_in_subprocess, patch_environ

KUBE_CONFIG_ENV_VAR = "KUBECONFIG"


@contextmanager
def temporary_gke_config_file(
    gcp_conn_id,
    project_id: str | None,
    cluster_name: str,
    impersonation_chain: str | Sequence[str] | None,
    regional: bool,
    location: str,
    use_internal_ip: bool,
) -> Generator[str, None, None]:
    conf_file = None
    try:
        hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
        project_id = project_id or hook.project_id

        if not project_id:
            raise AirflowException(
                "The project id must be passed either as "
                "keyword project_id parameter or as project_id extra "
                "in Google Cloud connection definition. Both are not set!"
            )

        # Write config to a temp file and set the environment variable to point to it.
        # This is to avoid race conditions of reading/writing a single file
        conf_file = tempfile.NamedTemporaryFile()
        with patch_environ(
            {KUBE_CONFIG_ENV_VAR: conf_file.name}
        ), hook.provide_authorized_gcloud():
            # Attempt to get/update credentials
            # We call gcloud directly instead of using google-cloud-python api
            # because there is no way to write kubernetes config to a file, which is
            # required by KubernetesPodOperator.
            # The gcloud command looks at the env variable `KUBECONFIG` for where to save
            # the kubernetes config file.

            cmd = _build_gcloud_cmd(
                project_id=project_id,
                cluster_name=cluster_name,
                impersonation_chain=impersonation_chain,
                regional=regional,
                location=location,
                use_internal_ip=use_internal_ip,
            )
            execute_in_subprocess(cmd)

            # Tell the kubernetes api where the config file is located
            yield os.environ[KUBE_CONFIG_ENV_VAR]
    finally:
        if conf_file is not None:
            logging.info("Closing temporary file")
            conf_file.close()
        else:
            logging.info("Config file is None")


def write_permanent_gke_config_file(
    gcp_conn_id: str,
    project_id: str | None,
    cluster_name: str,
    impersonation_chain: str | Sequence[str] | None,
    regional: bool,
    location: str,
    use_internal_ip: bool,
):
    hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
    project_id = project_id or hook.project_id

    if not project_id:
        raise AirflowException(
            "The project id must be passed either as "
            "keyword project_id parameter or as project_id extra "
            "in Google Cloud connection definition. Both are not set!"
        )

    # Writing permanent file to be able to use configuration in asynchronous execution.
    filename = f"/files/kube_config_{str(uuid.uuid4())[:8]}"
    with open(filename, "w") as conf_file, patch_environ(
        {KUBE_CONFIG_ENV_VAR: conf_file.name}
    ), hook.provide_authorized_gcloud():
        # Attempt to get/update credentials
        # We call gcloud directly instead of using google-cloud-python api
        # because there is no way to write kubernetes config to a file, which is
        # required by KubernetesPodOperator.
        # The gcloud command looks at the env variable `KUBECONFIG` for where to save
        # the kubernetes config file.

        cmd = _build_gcloud_cmd(
            project_id=project_id,
            cluster_name=cluster_name,
            impersonation_chain=impersonation_chain,
            regional=regional,
            location=location,
            use_internal_ip=use_internal_ip,
        )
        execute_in_subprocess(cmd)

        # Tell the kubernetes api where the config file is located
        return os.environ[KUBE_CONFIG_ENV_VAR]


def _build_gcloud_cmd(
    project_id: str,
    cluster_name: str,
    impersonation_chain: str | Sequence[str] | None,
    regional: bool,
    location: str,
    use_internal_ip: bool,
) -> list[str]:
    cmd = [
        "gcloud",
        "container",
        "clusters",
        "get-credentials",
        cluster_name,
        "--project",
        project_id,
    ]
    if impersonation_chain:
        if isinstance(impersonation_chain, str):
            impersonation_account = impersonation_chain
        elif len(impersonation_chain) == 1:
            impersonation_account = impersonation_chain[0]
        else:
            raise AirflowException(
                "Chained list of accounts is not supported, please specify only one service account"
            )

        cmd.extend(
            [
                "--impersonate-service-account",
                impersonation_account,
            ]
        )
    if regional:
        cmd.append("--region")
    else:
        cmd.append("--zone")
    cmd.append(location)
    if use_internal_ip:
        cmd.append("--internal-ip")
    return cmd
