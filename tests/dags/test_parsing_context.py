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
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import Context
from airflow.utils.dag_parsing_context import (
    _AIRFLOW_PARSING_CONTEXT_DAG_ID,
    _AIRFLOW_PARSING_CONTEXT_TASK_ID,
)
from airflow.utils.timezone import datetime


class DagWithParsingContext(EmptyOperator):
    def execute(self, context: Context):
        import os

        parsing_context_file = Path("/tmp/airflow_parsing_context")
        self.log.info("Executing")
        # signal to the test that we've started
        parsing_context = (
            f"{_AIRFLOW_PARSING_CONTEXT_DAG_ID}={os.environ.get(_AIRFLOW_PARSING_CONTEXT_DAG_ID)}\n"
            f"{_AIRFLOW_PARSING_CONTEXT_TASK_ID}={os.environ.get(_AIRFLOW_PARSING_CONTEXT_TASK_ID)}\n"
        )

        parsing_context_file.write_text(parsing_context)
        self.log.info("Executed")


dag1 = DAG(dag_id='test_parsing_context', start_date=datetime(2015, 1, 1))

dag1_task1 = DagWithParsingContext(task_id='task1', dag=dag1, owner='airflow')
