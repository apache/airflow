#
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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.common.io.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.sdk import Context

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import ObjectStoragePath
    from airflow.sdk.bases.operator import BaseOperator
else:
    from airflow.io.path import ObjectStoragePath  # type: ignore[no-redef]
    from airflow.models import BaseOperator  # type: ignore[no-redef]


class FileTransferOperator(BaseOperator):
    """
    Copies a file from a source to a destination.

    This streams the file from the source to the destination if required
    , so it does not need to fit into memory.

    :param src: The source file path or ObjectStoragePath object.
    :param dst: The destination file path or ObjectStoragePath object.
    :param source_conn_id: The optional source connection id.
    :param dest_conn_id: The optional destination connection id.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FileTransferOperator`
    """

    template_fields: Sequence[str] = ("src", "dst")

    def __init__(
        self,
        *,
        src: str | ObjectStoragePath,
        dst: str | ObjectStoragePath,
        source_conn_id: str | None = None,
        dest_conn_id: str | None = None,
        overwrite: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.src = src
        self.dst = dst
        self.source_conn_id = source_conn_id
        self.dst_conn_id = dest_conn_id
        self.overwrite = overwrite

    def execute(self, context: Context) -> None:
        src: ObjectStoragePath = self._get_path(self.src, self.source_conn_id)
        dst: ObjectStoragePath = self._get_path(self.dst, self.dst_conn_id)

        if not self.overwrite:
            if dst.exists() and dst.is_file():
                raise ValueError(f"Destination {dst} already exists")

        src.copy(dst)

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.openlineage.extractors import OperatorLineage

        def _prepare_ol_dataset(path: ObjectStoragePath) -> Dataset:
            if hasattr(path, "namespace"):
                # namespace has been added in Airflow 2.9.0; #36410
                return Dataset(namespace=path.namespace, name=path.key)
            # manually recreating namespace
            return Dataset(
                namespace=f"{path.protocol}://{path.bucket}" if path.bucket else path.protocol,
                name=path.key.lstrip(path.sep),
            )

        src: ObjectStoragePath = self._get_path(self.src, self.source_conn_id)
        dst: ObjectStoragePath = self._get_path(self.dst, self.dst_conn_id)

        input_dataset = _prepare_ol_dataset(src)
        output_dataset = _prepare_ol_dataset(dst)

        return OperatorLineage(
            inputs=[input_dataset],
            outputs=[output_dataset],
        )

    @staticmethod
    def _get_path(path: str | ObjectStoragePath, conn_id: str | None) -> ObjectStoragePath:
        if isinstance(path, str):
            return ObjectStoragePath(path, conn_id=conn_id)
        return path
