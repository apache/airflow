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
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.docling.hooks.docling_hook import DoclingHook


class DoclingConvertOperator(BaseOperator):
    """
    Sends a document to the Docling webserver to be converted.

    :param docling_conn_id: The connection ID to use to connect to the Docling server.
    :param filename: The name of the file being converted.
    :param parameters: Optional dictionary of processing parameters (options).
    """

    template_fields: Sequence[str] = ("filename",)

    def __init__(
        self,
        *,
        filename: str,
        docling_conn_id: str = "docling_default",
        parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.docling_conn_id = docling_conn_id
        self.filename = filename
        self.parameters = parameters

    def execute(self, context: Any) -> dict[str, Any]:
        """Execute the operator: instantiate the hook and calls the document processing method."""
        hook = DoclingHook(http_conn_id=self.docling_conn_id)
        self.log.info("Sending document %s for processing.", self.filename)

        result = hook.process_document(
            filename=self.filename,
            data=self.parameters
        )
        return result
