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

import asyncio
from typing import Any, AsyncIterator

import aiohttp

from airflow.triggers.base import BaseTrigger, TriggerEvent


class CloudFunctionInvokeTrigger(BaseTrigger):
    """
    Trigger that makes an HTTP POST request to a Google Cloud Function and waits for the response.

    :param function_uri: The HTTPS trigger URL of the Cloud Function.
    :param json_payload: The JSON payload to send in the request body.
    :param headers: The headers to send in the request, including authentication headers.
    :param timeout: Optional. The timeout in seconds for the HTTP request.
    """

    def __init__(
        self,
        function_uri: str,
        json_payload: dict[str, Any] | None,
        headers: dict[str, str],
        timeout: float | None = None,
    ):
        super().__init__()
        self.function_uri = function_uri
        self.json_payload = json_payload
        self.headers = headers
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.functions.CloudFunctionInvokeTrigger",
            {
                "function_uri": self.function_uri,
                "json_payload": self.json_payload,
                "headers": self.headers,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make an async HTTP request to the Cloud Function."""
        try:
            # We use aiohttp instead of the synchronous requests library
            timeout_obj = (
                aiohttp.ClientTimeout(total=self.timeout) if self.timeout else aiohttp.ClientTimeout()
            )
            async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                async with session.post(
                    self.function_uri,
                    json=self.json_payload,
                    headers=self.headers,
                ) as response:
                    # Cloud Functions return whatever the user code returns.
                    # We capture the status code and text/json.
                    status_code = response.status
                    response_text = await response.text()

                    if status_code >= 400:
                        yield TriggerEvent(
                            {
                                "status": "error",
                                "message": f"Cloud Function invocation failed with status {status_code}: {response_text}",
                                "status_code": status_code,
                            }
                        )
                    else:
                        try:
                            response_json = await response.json()
                            yield TriggerEvent(
                                {
                                    "status": "success",
                                    "response": response_json,
                                    "status_code": status_code,
                                }
                            )
                        except Exception:
                            # If response is not JSON, yield the text
                            yield TriggerEvent(
                                {
                                    "status": "success",
                                    "response": response_text,
                                    "status_code": status_code,
                                }
                            )
        except asyncio.TimeoutError:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": "Cloud Function invocation timed out.",
                    "status_code": None,
                }
            )
        except Exception as e:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(e),
                    "status_code": None,
                }
            )
