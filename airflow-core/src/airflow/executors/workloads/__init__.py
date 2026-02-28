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
"""Workload schemas for executor communication."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from airflow.executors.workloads.base import BaseWorkload, BundleInfo
from airflow.executors.workloads.callback import CallbackFetchMethod, ExecuteCallback
from airflow.executors.workloads.task import ExecuteTask
from airflow.executors.workloads.trigger import RunTrigger

All = Annotated[
    ExecuteTask | ExecuteCallback | RunTrigger,
    Field(discriminator="type"),
]

__all__ = ["All", "BaseWorkload", "BundleInfo", "CallbackFetchMethod", "ExecuteCallback", "ExecuteTask"]
