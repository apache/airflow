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

import mimetypes
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class DoclingHook(HttpHook):
    """
    Interacts with the Docling Webserver API.

    :param http_conn_id: The HTTP Connection ID to use for connecting to the Docling server.
    """

    def __init__(self, http_conn_id: str = "docling_default", **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method="POST", **kwargs)

    def _read_file_content(self, file_path: str) -> dict:
        """Read a file from a local path and returns its content, name, and format."""
        p = Path(file_path)
        content = p.read_bytes()

        # Guess the MIME type based on the file extension
        file_format, _ = mimetypes.guess_type(p)

        return {
            "content": content,
            "filename": p.name,
            "format": file_format or "application/octet-stream",  # Fallback
        }

    def process_document(self, filename: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Submit document content to the /api/v1/convert/file endpoint.

        :param filename: The name of the file to process.
        :param data: Optional dictionary of processing parameters.
        :return: The JSON response from the server.
        """
        self.log.info("Submitting document '%s' to Docling server for processing...", filename)

        _file = self._read_file_content(filename)

        if data:
            self.log.info("Processing with parameters: %s", data)

        try:
            files = {"file": (_file["filename"], _file["content"], _file["format"])}

            # Pass the data argument directly.
            # If `data` is None, the requests library will simply ignore it.
            response = self.run(endpoint="/api/v1/convert/file", files=files, data=data)
            response.raise_for_status()

            self.log.info("Document '%s' processed successfully.", filename)
            return response.json()

        except Exception as e:
            self.log.error("Failed to process document '%s' with Docling server: %s", filename, e)
            raise AirflowException(f"Docling API request failed for {filename}: {e}")

    def upload_source(self, source: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Get a source file from a URL using the /api/v1/convert/source endpoint.

        :param source: The URL of the source file.
        :param data: Optional dictionary of processing parameters (options).
        :return: The JSON response from the server.
        """
        self.log.info("Converting source %s using Docling server...", source)

        try:
            payload: dict[str, Any] = {"http_sources": [{"url": source}]}

            if data:
                payload["options"] = data
                self.log.info("Processing with parameters: %s", data)

            response = self.run(endpoint="/api/v1/convert/source", json=payload)
            response.raise_for_status()

            self.log.info("Source '%s' converted successfully.", source)
            return response.json()

        except Exception as e:
            self.log.error("Failed to use source '%s' with Docling server: %s", source, e)
            raise AirflowException(f"Docling source upload failed for {source}: {e}")
