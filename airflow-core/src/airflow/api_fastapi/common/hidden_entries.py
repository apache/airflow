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

from typing import TYPE_CHECKING

from airflow.secrets.hidden_entries import get_hidden_entry_sources

if TYPE_CHECKING:
    from fastapi import Response

LIST_INCOMPLETE_HEADER = "X-Airflow-List-Incomplete"


def set_list_incomplete_header(response: Response, env_prefix: str) -> None:
    """
    Flag a list response that only covers the metadata database.

    Environment variables with ``env_prefix`` or a configured secrets backend can also define entries
    (and override database ones); those are not part of the listing, so tell the client.
    """
    if get_hidden_entry_sources(env_prefix):
        response.headers[LIST_INCOMPLETE_HEADER] = "true"
