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

from typing import Any

from airflow.models import BaseOperator
from airflow.providers.docling.hooks.docling import DoclingHook


class DoclingConvertSourceOperator(BaseOperator):
    """
    Convert a document from a source URL using the Docling webserver.

    :param docling_conn_id: The connection ID to use to connect to the Docling server.
    :param source: The URL of the source file.
    :param parameters: Optional dictionary of processing parameters (options).
    """

    def __init__(
        self,
        *,
        docling_conn_id: str = "docling_default",
        source: str,
        parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.docling_conn_id = docling_conn_id
        self.source = source
        self.parameters = parameters

    def execute(self, context: Any) -> dict[str, Any]:
        """Execute the operator: instantiate the hook and calls the source upload method."""
        hook = DoclingHook(http_conn_id=self.docling_conn_id)
        self.log.info("Uploading source %s.", self.source)

        result = hook.upload_source(source=self.source, data=self.parameters)
        return result
