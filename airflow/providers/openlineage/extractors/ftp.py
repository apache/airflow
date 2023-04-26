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
from contextlib import suppress
from ftplib import FTP_PORT

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.utils.module_loading import import_string
from openlineage.client.run import Dataset

with suppress(ImportError):
    FTPOperation = import_string("airflow.providers.ftp.operators.ftp.FTPOperation")


class FTPExtractor(BaseExtractor):
    """FTP extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["FTPFileTransmitOperator"]

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

        conn = self.operator.hook.get_conn()
        remote_host = conn.host
        remote_port = conn.port

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

        if self.operator.operation.lower() == FTPOperation.GET:
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
        port = port or FTP_PORT
        authority = f"{host}:{port}"
        return f"{scheme}://{authority}"
