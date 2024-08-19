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
"""Enums for DAG serialization."""

from __future__ import annotations

from enum import Enum, unique


# Fields of an encoded object in serialization.
@unique
class Encoding(str, Enum):
    """Enum of encoding constants."""

    TYPE = "__type"
    VAR = "__var"


# Supported types for encoding. primitives and list are not encoded.
@unique
class DagAttributeTypes(str, Enum):
    """Enum of supported attribute types of DAG."""

    DAG = "dag"
    DATASET_EVENT_ACCESSORS = "dataset_event_accessors"
    DATASET_EVENT_ACCESSOR = "dataset_event_accessor"
    OP = "operator"
    DATETIME = "datetime"
    TIMEDELTA = "timedelta"
    TIMEZONE = "timezone"
    RELATIVEDELTA = "relativedelta"
    BASE_TRIGGER = "base_trigger"
    AIRFLOW_EXC_SER = "airflow_exc_ser"
    BASE_EXC_SER = "base_exc_ser"
    DICT = "dict"
    SET = "set"
    TUPLE = "tuple"
    POD = "k8s.V1Pod"
    TASK_GROUP = "taskgroup"
    EDGE_INFO = "edgeinfo"
    PARAM = "param"
    XCOM_REF = "xcomref"
    DATASET = "dataset"
    ASSET_ALIAS = "asset_alias"
    ASSET_ANY = "asset_any"
    ASSET_ALL = "asset_all"
    SIMPLE_TASK_INSTANCE = "simple_task_instance"
    BASE_JOB = "Job"
    TASK_INSTANCE = "task_instance"
    DAG_RUN = "dag_run"
    DAG_MODEL = "dag_model"
    DATA_SET = "data_set"
    LOG_TEMPLATE = "log_template"
    CONNECTION = "connection"
    TASK_CONTEXT = "task_context"
    ARG_NOT_SET = "arg_not_set"
    TASK_CALLBACK_REQUEST = "task_callback_request"
    DAG_CALLBACK_REQUEST = "dag_callback_request"
    TASK_INSTANCE_KEY = "task_instance_key"
    TRIGGER = "trigger"
