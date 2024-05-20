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

import re
from datetime import datetime, timedelta
from airflow.utils import timezone
from unittest.mock import MagicMock, call, patch

import pytest
import responses
from responses import matchers

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.ydb.operators.ydb import YDBOperator

@pytest.mark.db_test
def test_sql_templating(create_task_instance_of_operator):
    ti = create_task_instance_of_operator(
        YDBOperator,
        sql="SELECT * FROM pet WHERE birth_date BETWEEN '{{params.begin_date}}' AND '{{params.end_date}}'",
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        ydb_conn_id="ydb_default1",
        dag_id="test_template_body_templating_dag",
        task_id="test_template_body_templating_task",
        execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
    )
    ti.render_templates()
    task: YDBOperator = ti.task
    assert task.sql == "SELECT * FROM pet WHERE birth_date BETWEEN '2020-01-01' AND '2020-12-31'"
