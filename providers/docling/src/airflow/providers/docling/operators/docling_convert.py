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

from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.docling.hooks.docling import DoclingHook, get_file_accessor_hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DoclingConvertOperator(BaseOperator):
    """
    Convert a document using the Docling API by reading from a generic storage location.

    :param docling_conn_id: Airflow connection ID for the Docling service.
    :param storage_conn_id: Airflow connection ID for the source file storage (e.g., MinIO, GCS).
    :param file_location: A dictionary describing the file's location, conforming to the
                        requirements of the appropriate FileAccessorHook.
                        e.g., {"bucket": "my-bucket", "key": "path/to/file.pdf"} for S3.
    :param parameters: Optional dictionary of parameters for the Docling API.
    """

    template_fields = ("storage_conn_id", "file_location", "parameters")

    def __init__(
        self,
        *,
        docling_conn_id: str,
        storage_conn_id: str,
        file_location: dict[str, Any],
        parameters: dict[str, Any] | None = None,
        timeout: int = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.docling_conn_id = docling_conn_id
        self.storage_conn_id = storage_conn_id
        self.file_location = file_location
        self.parameters = parameters
        self.timeout = timeout

    def execute(self, context: Context) -> dict[str, Any]:
        """
        Execute the operator to read a file from a specified storage location and send it to Docling for processing.

        This method performs the following steps:
        1. Retrieves the appropriate file accessor hook based on the storage connection ID.
        2. Reads the file content from the specified file location using the storage hook.
        3. Sends the file content along with additional parameters to the Docling service via the Docling hook.

        Args:
            context (Context): The Airflow context dictionary containing runtime information.

        Returns:
            dict[str, Any]: The response from the Docling service after processing the document.

        Raises:
            AirflowException: If there is an error reading the file or sending the document.
        """
        # 1. Get the correct file reader using the factory based on the connection type.
        storage_hook = get_file_accessor_hook(self.storage_conn_id)

        # 2. Read the file. The operator doesn't know or care if it's from S3, GCS, etc.
        filename, file_content = storage_hook.read_file(location=self.file_location)

        # 3. Pass the file bytes to the Docling hook for processing.
        docling_hook = DoclingHook(http_conn_id=self.docling_conn_id)
        return docling_hook.send_document(
            filename=filename,
            file_content=file_content,
            data=self.parameters,
            timeout=self.timeout,
        )


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
        timeout: int = 3600,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.docling_conn_id = docling_conn_id
        self.source = source
        self.parameters = parameters
        self.timeout = timeout

    def execute(self, context: Any) -> dict[str, Any]:
        """Execute the operator: instantiate the hook and calls the source upload method."""
        hook = DoclingHook(http_conn_id=self.docling_conn_id)
        self.log.info("Uploading source %s.", self.source)

        result = hook.upload_source(
            source=self.source,
            data=self.parameters,
            timeout=self.timeout,
        )
        return result
