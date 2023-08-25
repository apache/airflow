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

from enum import Enum


class ResourceType(Enum):
    """
    Define all the resource types in Airflow.

    This is used when doing authorization check to define the type of resource the user is trying to access.
    """

    AUDIT_LOG = "Audit Logs"
    CLUSTER_ACTIVITY = "Cluster Activity"
    CONFIG = "Configurations"
    CONNECTION = "Connections"
    DAG = "DAGs"
    DAG_CODE = "DAG Code"
    DAG_DEPENDENCIES = "DAG Dependencies"
    DAG_RUN = "DAG Runs"
    DATASET = "Datasets"
    PLUGIN = "Plugins"
    PROVIDER = "Providers"
    TASK_INSTANCE = "Task Instances"
    TASK_LOG = "Task Logs"
    VARIABLE = "Variables"
    WEBSITE = "Website"
    XCOM = "XComs"
