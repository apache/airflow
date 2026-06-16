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

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict, Field

from airflow.api_fastapi.core_api.base import BaseModel


def _remove_kwargs(_: object) -> str:
    """
    Return empty trigger kwargs for API responses.

    Trigger ``kwargs`` may contain sensitive values (for example credentials a deferred
    operator hands to its trigger -- an API key, a token), so they are never exposed through
    the REST API. The field is kept in the response schema for backwards compatibility -- so
    existing API consumers do not break on a missing property -- but it is always returned
    empty, as ``"{}"`` (the stringified empty dict, matching the string format the field has
    always used). The triggerer still decrypts and uses the real kwargs at runtime; only the
    API representation is emptied.
    """
    return "{}"


class TriggerResponse(BaseModel):
    """Trigger serializer for responses."""

    model_config = ConfigDict(populate_by_name=True)

    id: int
    classpath: str
    # Deprecated: always emptied (see ``_remove_kwargs``). Marked deprecated in the schema so
    # consumers are nudged off it. ``deprecated`` only warns on direct attribute access, not
    # during model serialization, so our own response rendering stays warning-free.
    kwargs: Annotated[str, BeforeValidator(_remove_kwargs), Field(deprecated=True)]
    created_date: datetime
    queue: str | None
    triggerer_id: int | None
