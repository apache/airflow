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

from dataclasses import dataclass
from enum import Enum


@dataclass
class ConnectionDetails:
    """Represents the details of a connection."""

    conn_id: str


@dataclass
class DagDetails:
    """Represents the details of a DAG."""

    id: str


class DagAccessEntity(Enum):
    """Enum of DAG entities the user tries to access."""

    AUDIT_LOG = "AUDIT_LOG"
    CODE = "CODE"
    DATASET = "DATASET"
    DEPENDENCIES = "DEPENDENCIES"
    RUN = "RUN"
    TASK_INSTANCE = "TASK_INSTANCE"
    TASK_LOGS = "TASK_LOGS"
    XCOM = "XCOM"
