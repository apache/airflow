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
"""
Test case for GitHub issue #56529: Param defaults with strftime-converted dates.

This test verifies that DAGs with Param defaults containing strftime-formatted
date strings can be properly serialized and deserialized without causing
TypeError during DAG processing.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sdk.definitions.param import Param

with DAG(
    dag_id="test_param_strftime_dates",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    params={
        "efun_query_start_date": Param(
            (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d"),
            "Provide date yyyy-mm-dd",
        ),
        "efun_query_end_date": Param(
            datetime.today().strftime("%Y-%m-%d"),
            "Provide date yyyy-mm-dd",
        ),
        "start_timestamp": Param(
            int((datetime.today() - timedelta(days=2)).timestamp()),
            "Start timestamp as integer",
        ),
        "end_timestamp": Param(
            int(datetime.today().timestamp()),
            "End timestamp as integer",
        ),
    },
):
    task = EmptyOperator(task_id="test_task")
