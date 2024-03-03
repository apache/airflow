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
#
# This is automatically generated stub for the `common.sql` provider
#
# This file is generated automatically by the `update-common-sql-api stubs` pre-commit
# and the .pyi file represents part of the "public" API that the
# `common.sql` provider exposes to other providers.
#
# Any, potentially breaking change in the stubs will require deliberate manual action from the contributor
# making a change to the `common.sql` provider. Those stubs are also used by MyPy automatically when checking
# if only public API of the common.sql provider is used by all the other providers.
#
# You can read more in the README_API.md file
#
"""
Definition of the public interface for airflow.providers.common.sql.sensors.sql
isort:skip_file
"""
from _typeshed import Incomplete
from airflow.sensors.base import BaseSensorOperator
from typing import Any, Sequence

class SqlSensor(BaseSensorOperator):
    template_fields: Sequence[str]
    template_ext: Sequence[str]
    ui_color: str
    conn_id: Incomplete
    sql: Incomplete
    parameters: Incomplete
    success: Incomplete
    failure: Incomplete
    fail_on_empty: Incomplete
    hook_params: Incomplete
    def __init__(
        self,
        *,
        conn_id,
        sql,
        parameters: Incomplete | None = ...,
        success: Incomplete | None = ...,
        failure: Incomplete | None = ...,
        fail_on_empty: bool = ...,
        hook_params: Incomplete | None = ...,
        **kwargs,
    ) -> None: ...
    def poke(self, context: Any): ...
