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
class ConfigurationDetails:
    """Represents the details of a configuration."""

    section: str | None = None


@dataclass
class ConnectionDetails:
    """Represents the details of a connection."""

    conn_id: str | None = None


@dataclass
class DagDetails:
    """Represents the details of a DAG."""

    id: str | None = None


@dataclass
class DatasetDetails:
    """Represents the details of a dataset."""

    uri: str | None = None


@dataclass
class PoolDetails:
    """Represents the details of a pool."""

    name: str | None = None


@dataclass
class VariableDetails:
    """Represents the details of a variable."""

    key: str | None = None


class AccessView(Enum):
    """Enum of specific views the user tries to access."""

    CLUSTER_ACTIVITY = "CLUSTER_ACTIVITY"
    DOCS = "DOCS"
    IMPORT_ERRORS = "IMPORT_ERRORS"
    JOBS = "JOBS"
    PLUGINS = "PLUGINS"
    PROVIDERS = "PROVIDERS"
    TRIGGERS = "TRIGGERS"
    WEBSITE = "WEBSITE"


class DagAccessEntity(Enum):
    """Enum of DAG entities the user tries to access."""

    AUDIT_LOG = "AUDIT_LOG"
    CODE = "CODE"
    DEPENDENCIES = "DEPENDENCIES"
    RUN = "RUN"
    SLA_MISS = "SLA_MISS"
    TASK = "TASK"
    TASK_INSTANCE = "TASK_INSTANCE"
    TASK_RESCHEDULE = "TASK_RESCHEDULE"
    TASK_LOGS = "TASK_LOGS"
    WARNING = "WARNING"
    XCOM = "XCOM"
