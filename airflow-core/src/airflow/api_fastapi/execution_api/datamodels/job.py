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

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


class JobRegisterBody(StrictBaseModel):
    """Body for registering a new ``Job`` row (e.g. a DB-free triggerer claiming an identity)."""

    job_type: str
    """The job type, e.g. ``TriggererJob``."""
    hostname: str
    """Hostname of the registering process, so the Job row records it (not the api-server's)."""


class JobRegisterResponse(BaseModel):
    """Response carrying the id of the freshly registered ``Job`` row."""

    job_id: int
