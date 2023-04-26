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

import socket

import paramiko

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.utils.module_loading import import_string
from openlineage.client.run import Dataset

SFTPHook = import_string("airflow.providers.sftp.hooks.sftp.SFTPHook")
SFTPOperation = import_string("airflow.providers.sftp.operators.sftp.SFTPOperation")


class SFTPExtractor(BaseExtractor):
    """SFTP extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["SFTPOperator"]

    def extract(self) -> OperatorLineage | None:
        scheme = "file"

        local_host = socket.gethostname()
        try:
            local_host = socket.gethostbyname(local_host)
        except Exception as e:
            self.log.warning(
                f"Failed to resolve local hostname. Using the hostname got by socket.gethostbyname() without resolution. {e}",  # noqa: E501
                exc_info=True,
            )

        # As of version 4.1.0, SFTPOperator accepts either of SFTPHook, SSHHook, or ssh_conn_id
        # as a constructor parameter. In the last case, SFTPOperator creates hook object in the
        # `execute` method rather than its constructor for some reason. So we have to manually
        # create a hook in the same way so that we can access connection information via that.
        if hasattr(self.operator, "sftp_hook") and self.operator.sftp_hook is not None:
            hook = self.operator.sftp_hook
        elif hasattr(self.operator, "ssh_hook") and self.operator.ssh_hook is not None:
            hook = self.operator.ssh_hook
        else:
            hook = SFTPHook(ssh_conn_id=self.operator.ssh_conn_id)

        if hasattr(self.operator, "remote_host") and self.operator.remote_host is not None:
            remote_host = self.operator.remote_host
        else:
            remote_host = hook.get_connection(hook.ssh_conn_id).host
        try:
            remote_host = socket.gethostbyname(remote_host)
        except Exception as e:
            self.log.warning(
                f"Failed to resolve remote hostname. Using the provided hostname without resolution. {e}",  # noqa: E501
                exc_info=True,
            )

        print("REMOTE HOST")
        print(remote_host)

        if hasattr(hook, "port"):
            remote_port = hook.port
        elif hasattr(hook, "ssh_hook"):
            remote_port = hook.ssh_hook.port

        # Since v4.1.0, SFTPOperator accepts both a string (single file) and a list of
        # strings (multiple files) as local_filepath and remote_filepath, and internally
        # keeps them as list in both cases. But before 4.1.0, only single string is
        # allowed. So we consider both cases here for backward compatibility.
        if isinstance(self.operator.local_filepath, str):
            local_filepath = [self.operator.local_filepath]
        else:
            local_filepath = self.operator.local_filepath
        if isinstance(self.operator.remote_filepath, str):
            remote_filepath = [self.operator.remote_filepath]
        else:
            remote_filepath = self.operator.remote_filepath

        local_datasets = [
            Dataset(namespace=self._get_namespace(scheme, local_host, None, path), name=path)
            for path in local_filepath
        ]
        remote_datasets = [
            Dataset(namespace=self._get_namespace(scheme, remote_host, remote_port, path), name=path)
            for path in remote_filepath
        ]
        print(remote_datasets)

        if self.operator.operation.lower() == SFTPOperation.GET:
            inputs = remote_datasets
            outputs = local_datasets
        else:
            inputs = local_datasets
            outputs = remote_datasets

        return OperatorLineage(
            inputs=inputs,
            outputs=outputs,
            run_facets={},
            job_facets={},
        )

    def _get_namespace(self, scheme, host, port, path) -> str:
        port = port or paramiko.config.SSH_PORT
        authority = f"{host}:{port}"
        return f"{scheme}://{authority}"
