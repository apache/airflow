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

import warnings
from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG

DAG_ID = "test_dag_warnings"


class TestOperator(BaseOperator):
    def __init__(
        self,
        *,
        parameter: str | None = None,
        deprecated_parameter: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if deprecated_parameter:
            warnings.warn(
                "Deprecated Parameter", category=DeprecationWarning, stacklevel=2
            )
            parameter = deprecated_parameter
        self.parameter = parameter

    def execute(self, context):
        return None


def some_warning():
    warnings.warn("Some Warning", category=UserWarning, stacklevel=1)


with DAG(DAG_ID, start_date=datetime(2024, 1, 1), schedule=None):
    TestOperator(task_id="test-task", parameter="foo")
    TestOperator(task_id="test-task-deprecated", deprecated_parameter="bar")

some_warning()
