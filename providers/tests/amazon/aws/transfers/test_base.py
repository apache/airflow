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

import pytest

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.providers.amazon.aws.transfers.base import AwsToAwsBaseOperator
from airflow.utils import timezone
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2020, 1, 1)


class TestAwsToAwsBaseOperator:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    @pytest.mark.db_test
    def test_render_template(self, session, clean_dags_and_dagruns):
        operator = AwsToAwsBaseOperator(
            task_id="dynamodb_to_s3_test_render",
            dag=self.dag,
            source_aws_conn_id="{{ ds }}",
            dest_aws_conn_id="{{ ds }}",
        )
        ti = TaskInstance(operator, run_id="something")
        ti.dag_run = DagRun(
            dag_id=self.dag.dag_id,
            run_id="something",
            execution_date=timezone.datetime(2020, 1, 1),
            run_type=DagRunType.MANUAL,
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        assert "2020-01-01" == getattr(operator, "source_aws_conn_id")
        assert "2020-01-01" == getattr(operator, "dest_aws_conn_id")
