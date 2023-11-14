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

from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG
from airflow.providers.databricks.hooks.databricks import RunState
from airflow.providers.databricks.operators.databricks import (
    DatabricksCreateJobsOperator,
    DatabricksRunNowDeferrableOperator,
    DatabricksRunNowOperator,
    DatabricksSubmitRunDeferrableOperator,
    DatabricksSubmitRunOperator,
)
from airflow.providers.databricks.triggers.databricks import DatabricksExecutionTrigger
from airflow.providers.databricks.utils import databricks as utils

pytestmark = pytest.mark.db_test

DATE = "2017-04-20"
TASK_ID = "databricks-operator"
DEFAULT_CONN_ID = "databricks_default"
NOTEBOOK_TASK = {"notebook_path": "/test"}
TEMPLATED_NOTEBOOK_TASK = {"notebook_path": "/test-{{ ds }}"}
RENDERED_TEMPLATED_NOTEBOOK_TASK = {"notebook_path": f"/test-{DATE}"}
SPARK_JAR_TASK = {"main_class_name": "com.databricks.Test"}
SPARK_PYTHON_TASK = {"python_file": "test.py", "parameters": ["--param", "123"]}
PIPELINE_ID_TASK = {"pipeline_id": "1234abcd"}
PIPELINE_NAME_TASK = {"pipeline_name": "This is a test pipeline"}
SPARK_SUBMIT_TASK = {
    "parameters": ["--class", "org.apache.spark.examples.SparkPi", "dbfs:/path/to/examples.jar", "10"]
}
NEW_CLUSTER = {
    "spark_version": "2.0.x-scala2.10",
    "node_type_id": "development-node",
    "num_workers": 1,
    "enable_elastic_disk": True,
}
EXISTING_CLUSTER_ID = "existing-cluster-id"
RUN_NAME = "run-name"
RUN_ID = 1
RUN_PAGE_URL = "run-page-url"
JOB_ID = "42"
JOB_NAME = "job-name"
NOTEBOOK_PARAMS = {"dry-run": "true", "oldest-time-to-consider": "1457570074236"}
JAR_PARAMS = ["param1", "param2"]
RENDERED_TEMPLATED_JAR_PARAMS = [f"/test-{DATE}"]
TEMPLATED_JAR_PARAMS = ["/test-{{ ds }}"]
PYTHON_PARAMS = ["john doe", "35"]
SPARK_SUBMIT_PARAMS = ["--class", "org.apache.spark.examples.SparkPi"]
DBT_TASK = {
    "commands": ["dbt deps", "dbt seed", "dbt run"],
    "schema": "jaffle_shop",
    "warehouse_id": "123456789abcdef0",
}
TAGS = {
    "cost-center": "engineering",
    "team": "jobs",
}
TASKS = [
    {
        "task_key": "Sessionize",
        "description": "Extracts session data from events",
        "existing_cluster_id": "0923-164208-meows279",
        "spark_jar_task": {
            "main_class_name": "com.databricks.Sessionize",
            "parameters": [
                "--data",
                "dbfs:/path/to/data.json",
            ],
        },
        "libraries": [
            {"jar": "dbfs:/mnt/databricks/Sessionize.jar"},
        ],
        "timeout_seconds": 86400,
        "max_retries": 3,
        "min_retry_interval_millis": 2000,
        "retry_on_timeout": False,
    },
    {
        "task_key": "Orders_Ingest",
        "description": "Ingests order data",
        "job_cluster_key": "auto_scaling_cluster",
        "spark_jar_task": {
            "main_class_name": "com.databricks.OrdersIngest",
            "parameters": ["--data", "dbfs:/path/to/order-data.json"],
        },
        "libraries": [
            {"jar": "dbfs:/mnt/databricks/OrderIngest.jar"},
        ],
        "timeout_seconds": 86400,
        "max_retries": 3,
        "min_retry_interval_millis": 2000,
        "retry_on_timeout": False,
    },
    {
        "task_key": "Match",
        "description": "Matches orders with user sessions",
        "depends_on": [
            {"task_key": "Orders_Ingest"},
            {"task_key": "Sessionize"},
        ],
        "new_cluster": {
            "spark_version": "7.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "spark_conf": {
                "spark.speculation": True,
            },
            "aws_attributes": {
                "availability": "SPOT",
                "zone_id": "us-west-2a",
            },
            "autoscale": {
                "min_workers": 2,
                "max_workers": 16,
            },
        },
        "notebook_task": {
            "notebook_path": "/Users/user.name@databricks.com/Match",
            "source": "WORKSPACE",
            "base_parameters": {
                "name": "John Doe",
                "age": "35",
            },
        },
        "timeout_seconds": 86400,
        "max_retries": 3,
        "min_retry_interval_millis": 2000,
        "retry_on_timeout": False,
    },
]
JOB_CLUSTERS = [
    {
        "job_cluster_key": "auto_scaling_cluster",
        "new_cluster": {
            "spark_version": "7.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "spark_conf": {
                "spark.speculation": True,
            },
            "aws_attributes": {
                "availability": "SPOT",
                "zone_id": "us-west-2a",
            },
            "autoscale": {
                "min_workers": 2,
                "max_workers": 16,
            },
        },
    },
]
EMAIL_NOTIFICATIONS = {
    "on_start": [
        "user.name@databricks.com",
    ],
    "on_success": [
        "user.name@databricks.com",
    ],
    "on_failure": [
        "user.name@databricks.com",
    ],
    "no_alert_for_skipped_runs": False,
}
WEBHOOK_NOTIFICATIONS = {
    "on_start": [
        {
            "id": "03dd86e4-57ef-4818-a950-78e41a1d71ab",
        },
        {
            "id": "0481e838-0a59-4eff-9541-a4ca6f149574",
        },
    ],
    "on_success": [
        {
            "id": "03dd86e4-57ef-4818-a950-78e41a1d71ab",
        }
    ],
    "on_failure": [
        {
            "id": "0481e838-0a59-4eff-9541-a4ca6f149574",
        }
    ],
}
TIMEOUT_SECONDS = 86400
SCHEDULE = {
    "quartz_cron_expression": "20 30 * * * ?",
    "timezone_id": "Europe/London",
    "pause_status": "PAUSED",
}
MAX_CONCURRENT_RUNS = 10
GIT_SOURCE = {
    "git_url": "https://github.com/databricks/databricks-cli",
    "git_branch": "main",
    "git_provider": "gitHub",
}
ACCESS_CONTROL_LIST = [
    {
        "user_name": "jsmith@example.com",
        "permission_level": "CAN_MANAGE",
    }
]


def mock_dict(d: dict):
    m = MagicMock()
    m.return_value = d
    return m


def make_run_with_state_mock(
    lifecycle_state: str, result_state: str, state_message: str = "", run_id=1, job_id=JOB_ID
):
    return mock_dict(
        {
            "job_id": job_id,
            "run_id": run_id,
            "state": {
                "life_cycle_state": lifecycle_state,
                "result_state": result_state,
                "state_message": state_message,
            },
        }
    )


class TestDatabricksCreateJobsOperator:
    def test_init_with_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksCreateJobsOperator(
            task_id=TASK_ID,
            name=JOB_NAME,
            tags=TAGS,
            tasks=TASKS,
            job_clusters=JOB_CLUSTERS,
            email_notifications=EMAIL_NOTIFICATIONS,
            webhook_notifications=WEBHOOK_NOTIFICATIONS,
            timeout_seconds=TIMEOUT_SECONDS,
            schedule=SCHEDULE,
            max_concurrent_runs=MAX_CONCURRENT_RUNS,
            git_source=GIT_SOURCE,
            access_control_list=ACCESS_CONTROL_LIST,
        )
        expected = utils.normalise_json_content(
            {
                "name": JOB_NAME,
                "tags": TAGS,
                "tasks": TASKS,
                "job_clusters": JOB_CLUSTERS,
                "email_notifications": EMAIL_NOTIFICATIONS,
                "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                "timeout_seconds": TIMEOUT_SECONDS,
                "schedule": SCHEDULE,
                "max_concurrent_runs": MAX_CONCURRENT_RUNS,
                "git_source": GIT_SOURCE,
                "access_control_list": ACCESS_CONTROL_LIST,
            }
        )

        assert expected == op.json

    def test_init_with_json(self):
        """
        Test the initializer with json data.
        """
        json = {
            "name": JOB_NAME,
            "tags": TAGS,
            "tasks": TASKS,
            "job_clusters": JOB_CLUSTERS,
            "email_notifications": EMAIL_NOTIFICATIONS,
            "webhook_notifications": WEBHOOK_NOTIFICATIONS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "schedule": SCHEDULE,
            "max_concurrent_runs": MAX_CONCURRENT_RUNS,
            "git_source": GIT_SOURCE,
            "access_control_list": ACCESS_CONTROL_LIST,
        }
        op = DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)

        expected = utils.normalise_json_content(
            {
                "name": JOB_NAME,
                "tags": TAGS,
                "tasks": TASKS,
                "job_clusters": JOB_CLUSTERS,
                "email_notifications": EMAIL_NOTIFICATIONS,
                "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                "timeout_seconds": TIMEOUT_SECONDS,
                "schedule": SCHEDULE,
                "max_concurrent_runs": MAX_CONCURRENT_RUNS,
                "git_source": GIT_SOURCE,
                "access_control_list": ACCESS_CONTROL_LIST,
            }
        )

        assert expected == op.json

    def test_init_with_merging(self):
        """
        Test the initializer when json and other named parameters are both
        provided. The named parameters should override top level keys in the
        json dict.
        """
        override_name = "override"
        override_tags = {}
        override_tasks = []
        override_job_clusters = []
        override_email_notifications = {}
        override_webhook_notifications = {}
        override_timeout_seconds = 0
        override_schedule = {}
        override_max_concurrent_runs = 0
        override_git_source = {}
        override_access_control_list = []
        json = {
            "name": JOB_NAME,
            "tags": TAGS,
            "tasks": TASKS,
            "job_clusters": JOB_CLUSTERS,
            "email_notifications": EMAIL_NOTIFICATIONS,
            "webhook_notifications": WEBHOOK_NOTIFICATIONS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "schedule": SCHEDULE,
            "max_concurrent_runs": MAX_CONCURRENT_RUNS,
            "git_source": GIT_SOURCE,
            "access_control_list": ACCESS_CONTROL_LIST,
        }

        op = DatabricksCreateJobsOperator(
            task_id=TASK_ID,
            json=json,
            name=override_name,
            tags=override_tags,
            tasks=override_tasks,
            job_clusters=override_job_clusters,
            email_notifications=override_email_notifications,
            webhook_notifications=override_webhook_notifications,
            timeout_seconds=override_timeout_seconds,
            schedule=override_schedule,
            max_concurrent_runs=override_max_concurrent_runs,
            git_source=override_git_source,
            access_control_list=override_access_control_list,
        )

        expected = utils.normalise_json_content(
            {
                "name": override_name,
                "tags": override_tags,
                "tasks": override_tasks,
                "job_clusters": override_job_clusters,
                "email_notifications": override_email_notifications,
                "webhook_notifications": override_webhook_notifications,
                "timeout_seconds": override_timeout_seconds,
                "schedule": override_schedule,
                "max_concurrent_runs": override_max_concurrent_runs,
                "git_source": override_git_source,
                "access_control_list": override_access_control_list,
            }
        )

        assert expected == op.json

    def test_init_with_templating(self):
        json = {"name": "test-{{ ds }}"}

        dag = DAG("test", start_date=datetime.now())
        op = DatabricksCreateJobsOperator(dag=dag, task_id=TASK_ID, json=json)
        op.render_template_fields(context={"ds": DATE})
        expected = utils.normalise_json_content({"name": f"test-{DATE}"})
        assert expected == op.json

    def test_init_with_bad_type(self):
        json = {"test": datetime.now()}
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = (
            r"Type \<(type|class) \'datetime.datetime\'\> used "
            r"for parameter json\[test\] is not a number or a string"
        )
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_create(self, db_mock_class):
        """
        Test the execute function in case where the job does not exist.
        """
        json = {
            "name": JOB_NAME,
            "tags": TAGS,
            "tasks": TASKS,
            "job_clusters": JOB_CLUSTERS,
            "email_notifications": EMAIL_NOTIFICATIONS,
            "webhook_notifications": WEBHOOK_NOTIFICATIONS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "schedule": SCHEDULE,
            "max_concurrent_runs": MAX_CONCURRENT_RUNS,
            "git_source": GIT_SOURCE,
            "access_control_list": ACCESS_CONTROL_LIST,
        }
        op = DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)
        db_mock = db_mock_class.return_value
        db_mock.create_job.return_value = JOB_ID

        db_mock.find_job_id_by_name.return_value = None

        return_result = op.execute({})

        expected = utils.normalise_json_content(
            {
                "name": JOB_NAME,
                "tags": TAGS,
                "tasks": TASKS,
                "job_clusters": JOB_CLUSTERS,
                "email_notifications": EMAIL_NOTIFICATIONS,
                "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                "timeout_seconds": TIMEOUT_SECONDS,
                "schedule": SCHEDULE,
                "max_concurrent_runs": MAX_CONCURRENT_RUNS,
                "git_source": GIT_SOURCE,
                "access_control_list": ACCESS_CONTROL_LIST,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksCreateJobsOperator",
        )

        db_mock.create_job.assert_called_once_with(expected)
        assert JOB_ID == return_result

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_reset(self, db_mock_class):
        """
        Test the execute function in case where the job already exists.
        """
        json = {
            "name": JOB_NAME,
            "tags": TAGS,
            "tasks": TASKS,
            "job_clusters": JOB_CLUSTERS,
            "email_notifications": EMAIL_NOTIFICATIONS,
            "webhook_notifications": WEBHOOK_NOTIFICATIONS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "schedule": SCHEDULE,
            "max_concurrent_runs": MAX_CONCURRENT_RUNS,
            "git_source": GIT_SOURCE,
            "access_control_list": ACCESS_CONTROL_LIST,
        }
        op = DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)
        db_mock = db_mock_class.return_value
        db_mock.find_job_id_by_name.return_value = JOB_ID

        return_result = op.execute({})

        expected = utils.normalise_json_content(
            {
                "name": JOB_NAME,
                "tags": TAGS,
                "tasks": TASKS,
                "job_clusters": JOB_CLUSTERS,
                "email_notifications": EMAIL_NOTIFICATIONS,
                "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                "timeout_seconds": TIMEOUT_SECONDS,
                "schedule": SCHEDULE,
                "max_concurrent_runs": MAX_CONCURRENT_RUNS,
                "git_source": GIT_SOURCE,
                "access_control_list": ACCESS_CONTROL_LIST,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksCreateJobsOperator",
        )

        db_mock.reset_job.assert_called_once_with(JOB_ID, expected)
        assert JOB_ID == return_result


class TestDatabricksSubmitRunOperator:
    def test_init_with_notebook_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, notebook_task=NOTEBOOK_TASK
        )
        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": TASK_ID}
        )

        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_spark_python_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, spark_python_task=SPARK_PYTHON_TASK
        )
        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "spark_python_task": SPARK_PYTHON_TASK, "run_name": TASK_ID}
        )

        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_pipeline_name_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, pipeline_task=PIPELINE_NAME_TASK)
        expected = utils.normalise_json_content({"pipeline_task": PIPELINE_NAME_TASK, "run_name": TASK_ID})

        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_pipeline_id_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, pipeline_task=PIPELINE_ID_TASK)
        expected = utils.normalise_json_content({"pipeline_task": PIPELINE_ID_TASK, "run_name": TASK_ID})

        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_spark_submit_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, spark_submit_task=SPARK_SUBMIT_TASK
        )
        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "spark_submit_task": SPARK_SUBMIT_TASK, "run_name": TASK_ID}
        )

        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_dbt_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        git_source = {
            "git_url": "https://github.com/dbt-labs/jaffle_shop",
            "git_provider": "github",
            "git_branch": "main",
        }
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, dbt_task=DBT_TASK, git_source=git_source
        )
        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "dbt_task": DBT_TASK, "git_source": git_source, "run_name": TASK_ID}
        )

        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_dbt_task_mixed_parameters(self):
        """
        Test the initializer with mixed parameters.
        """
        git_source = {
            "git_url": "https://github.com/dbt-labs/jaffle_shop",
            "git_provider": "github",
            "git_branch": "main",
        }
        json = {"git_source": git_source}
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, dbt_task=DBT_TASK, json=json
        )
        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "dbt_task": DBT_TASK, "git_source": git_source, "run_name": TASK_ID}
        )

        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_dbt_task_without_git_source_raises_error(self):
        """
        Test the initializer without the necessary git_source for dbt_task raises error.
        """
        exception_message = "git_source is required for dbt_task"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksSubmitRunOperator(task_id=TASK_ID, new_cluster=NEW_CLUSTER, dbt_task=DBT_TASK)

    def test_init_with_dbt_task_json_without_git_source_raises_error(self):
        """
        Test the initializer without the necessary git_source for dbt_task raises error.
        """
        json = {"dbt_task": DBT_TASK, "new_cluster": NEW_CLUSTER}

        exception_message = "git_source is required for dbt_task"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)

    def test_init_with_json(self):
        """
        Test the initializer with json data.
        """
        json = {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": TASK_ID}
        )
        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_tasks(self):
        tasks = [{"task_key": 1, "new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK}]
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, tasks=tasks)
        expected = utils.normalise_json_content({"run_name": TASK_ID, "tasks": tasks})
        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_specified_run_name(self):
        """
        Test the initializer with a specified run_name.
        """
        json = {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": RUN_NAME}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": RUN_NAME}
        )
        assert expected == utils.normalise_json_content(op.json)

    def test_pipeline_task(self):
        """
        Test the initializer with a pipeline task.
        """
        pipeline_task = {"pipeline_id": "test-dlt"}
        json = {"new_cluster": NEW_CLUSTER, "run_name": RUN_NAME, "pipeline_task": pipeline_task}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "pipeline_task": pipeline_task, "run_name": RUN_NAME}
        )
        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_merging(self):
        """
        Test the initializer when json and other named parameters are both
        provided. The named parameters should override top level keys in the
        json dict.
        """
        override_new_cluster = {"workers": 999}
        json = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json, new_cluster=override_new_cluster)
        expected = utils.normalise_json_content(
            {
                "new_cluster": override_new_cluster,
                "notebook_task": NOTEBOOK_TASK,
                "run_name": TASK_ID,
            }
        )
        assert expected == utils.normalise_json_content(op.json)

    @pytest.mark.db_test
    def test_init_with_templating(self):
        json = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": TEMPLATED_NOTEBOOK_TASK,
        }
        dag = DAG("test", start_date=datetime.now())
        op = DatabricksSubmitRunOperator(dag=dag, task_id=TASK_ID, json=json)
        op.render_template_fields(context={"ds": DATE})
        expected = utils.normalise_json_content(
            {
                "new_cluster": NEW_CLUSTER,
                "notebook_task": RENDERED_TEMPLATED_NOTEBOOK_TASK,
                "run_name": TASK_ID,
            }
        )
        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_git_source(self):
        json = {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": RUN_NAME}
        git_source = {
            "git_url": "https://github.com/apache/airflow",
            "git_provider": "github",
            "git_branch": "main",
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, git_source=git_source, json=json)
        expected = utils.normalise_json_content(
            {
                "new_cluster": NEW_CLUSTER,
                "notebook_task": NOTEBOOK_TASK,
                "run_name": RUN_NAME,
                "git_source": git_source,
            }
        )
        assert expected == utils.normalise_json_content(op.json)

    def test_init_with_bad_type(self):
        json = {"test": datetime.now()}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = (
            r"Type \<(type|class) \'datetime.datetime\'\> used "
            r"for parameter json\[test\] is not a number or a string"
        )
        with pytest.raises(AirflowException, match=exception_message):
            utils.normalise_json_content(op.json)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        op.execute(None)

        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": TASK_ID}
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksSubmitRunOperator",
        )

        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_failure(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        with pytest.raises(AirflowException):
            op.execute(None)

        expected = utils.normalise_json_content(
            {
                "new_cluster": NEW_CLUSTER,
                "notebook_task": NOTEBOOK_TASK,
                "run_name": TASK_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksSubmitRunOperator",
        )
        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_on_kill(self, db_mock_class):
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        op.run_id = RUN_ID

        op.on_kill()

        db_mock.cancel_run.assert_called_once_with(RUN_ID)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_wait_for_termination(self, db_mock_class):
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        assert op.wait_for_termination

        op.execute(None)

        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": TASK_ID}
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksSubmitRunOperator",
        )

        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_called_once_with(RUN_ID)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_no_wait_for_termination(self, db_mock_class):
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, wait_for_termination=False, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1

        assert not op.wait_for_termination

        op.execute(None)

        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": TASK_ID}
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksSubmitRunOperator",
        )

        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_not_called()


class TestDatabricksSubmitRunDeferrableOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_execute_task_deferred(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunDeferrableOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        with pytest.raises(TaskDeferred) as exc:
            op.execute(None)
        assert isinstance(exc.value.trigger, DatabricksExecutionTrigger)
        assert exc.value.method_name == "execute_complete"

        expected = utils.normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": TASK_ID}
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksSubmitRunDeferrableOperator",
        )

        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        assert op.run_id == RUN_ID

    def test_execute_complete_success(self):
        """
        Test `execute_complete` function in case the Trigger has returned a successful completion event.
        """
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        event = {
            "run_id": RUN_ID,
            "run_page_url": RUN_PAGE_URL,
            "run_state": RunState("TERMINATED", "SUCCESS", "").to_json(),
        }

        op = DatabricksSubmitRunDeferrableOperator(task_id=TASK_ID, json=run)
        assert op.execute_complete(context=None, event=event) is None

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_execute_complete_failure(self, db_mock_class):
        """
        Test `execute_complete` function in case the Trigger has returned a failure completion event.
        """
        run_state_failed = RunState("TERMINATED", "FAILED", "")
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        event = {
            "run_id": RUN_ID,
            "run_page_url": RUN_PAGE_URL,
            "run_state": run_state_failed.to_json(),
        }

        op = DatabricksSubmitRunDeferrableOperator(task_id=TASK_ID, json=run)
        with pytest.raises(AirflowException):
            op.execute_complete(context=None, event=event)

        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        with pytest.raises(AirflowException, match=f"Job run failed with terminal state: {run_state_failed}"):
            op.execute_complete(context=None, event=event)

    def test_execute_complete_incorrect_event_validation_failure(self):
        event = {"event_id": "no such column"}
        op = DatabricksSubmitRunDeferrableOperator(task_id=TASK_ID)
        with pytest.raises(AirflowException):
            op.execute_complete(context=None, event=event)


class TestDatabricksRunNowOperator:
    def test_init_with_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksRunNowOperator(job_id=JOB_ID, task_id=TASK_ID)
        expected = utils.normalise_json_content({"job_id": 42})

        assert expected == op.json

    def test_init_with_json(self):
        """
        Test the initializer with json data.
        """
        json = {
            "notebook_params": NOTEBOOK_PARAMS,
            "jar_params": JAR_PARAMS,
            "python_params": PYTHON_PARAMS,
            "spark_submit_params": SPARK_SUBMIT_PARAMS,
            "job_id": JOB_ID,
        }
        op = DatabricksRunNowOperator(task_id=TASK_ID, json=json)

        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "jar_params": JAR_PARAMS,
                "python_params": PYTHON_PARAMS,
                "spark_submit_params": SPARK_SUBMIT_PARAMS,
                "job_id": JOB_ID,
            }
        )

        assert expected == op.json

    def test_init_with_merging(self):
        """
        Test the initializer when json and other named parameters are both
        provided. The named parameters should override top level keys in the
        json dict.
        """
        override_notebook_params = {"workers": "999"}
        override_jar_params = ["workers", "998"]
        json = {"notebook_params": NOTEBOOK_PARAMS, "jar_params": JAR_PARAMS}

        op = DatabricksRunNowOperator(
            task_id=TASK_ID,
            json=json,
            job_id=JOB_ID,
            notebook_params=override_notebook_params,
            python_params=PYTHON_PARAMS,
            jar_params=override_jar_params,
            spark_submit_params=SPARK_SUBMIT_PARAMS,
        )

        expected = utils.normalise_json_content(
            {
                "notebook_params": override_notebook_params,
                "jar_params": override_jar_params,
                "python_params": PYTHON_PARAMS,
                "spark_submit_params": SPARK_SUBMIT_PARAMS,
                "job_id": JOB_ID,
            }
        )

        assert expected == op.json

    @pytest.mark.db_test
    def test_init_with_templating(self):
        json = {"notebook_params": NOTEBOOK_PARAMS, "jar_params": TEMPLATED_JAR_PARAMS}

        dag = DAG("test", start_date=datetime.now())
        op = DatabricksRunNowOperator(dag=dag, task_id=TASK_ID, job_id=JOB_ID, json=json)
        op.render_template_fields(context={"ds": DATE})
        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "jar_params": RENDERED_TEMPLATED_JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )
        assert expected == op.json

    def test_init_with_bad_type(self):
        json = {"test": datetime.now()}
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = (
            r"Type \<(type|class) \'datetime.datetime\'\> used "
            r"for parameter json\[test\] is not a number or a string"
        )
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=json)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        op.execute(None)

        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "notebook_task": NOTEBOOK_TASK,
                "jar_params": JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksRunNowOperator",
        )
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_failure(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        with pytest.raises(AirflowException):
            op.execute(None)

        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "notebook_task": NOTEBOOK_TASK,
                "jar_params": JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksRunNowOperator",
        )
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_failure_with_message(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run = mock_dict(
            {
                "job_id": JOB_ID,
                "run_id": 1,
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "FAILED",
                    "state_message": "failed",
                },
                "tasks": [
                    {
                        "run_id": 2,
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "FAILED",
                            "state_message": "failed",
                        },
                    }
                ],
            }
        )
        db_mock.get_run_output = mock_dict({"error": "Exception: Something went wrong..."})

        with pytest.raises(AirflowException) as exc_info:
            op.execute(None)

        assert exc_info.value.args[0].endswith(" Exception: Something went wrong...")

        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "notebook_task": NOTEBOOK_TASK,
                "jar_params": JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksRunNowOperator",
        )
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_on_kill(self, db_mock_class):
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        op.run_id = RUN_ID

        op.on_kill()
        db_mock.cancel_run.assert_called_once_with(RUN_ID)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_wait_for_termination(self, db_mock_class):
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        assert op.wait_for_termination

        op.execute(None)

        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "notebook_task": NOTEBOOK_TASK,
                "jar_params": JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksRunNowOperator",
        )

        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_called_once_with(RUN_ID)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_no_wait_for_termination(self, db_mock_class):
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, wait_for_termination=False, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1

        assert not op.wait_for_termination

        op.execute(None)

        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "notebook_task": NOTEBOOK_TASK,
                "jar_params": JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksRunNowOperator",
        )

        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_not_called()

    def test_init_exception_with_job_name_and_job_id(self):
        exception_message = "Argument 'job_name' is not allowed with argument 'job_id'"

        with pytest.raises(AirflowException, match=exception_message):
            DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, job_name=JOB_NAME)

        with pytest.raises(AirflowException, match=exception_message):
            run = {"job_id": JOB_ID, "job_name": JOB_NAME}
            DatabricksRunNowOperator(task_id=TASK_ID, json=run)

        with pytest.raises(AirflowException, match=exception_message):
            run = {"job_id": JOB_ID}
            DatabricksRunNowOperator(task_id=TASK_ID, json=run, job_name=JOB_NAME)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_with_job_name(self, db_mock_class):
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_name=JOB_NAME, json=run)
        db_mock = db_mock_class.return_value
        db_mock.find_job_id_by_name.return_value = JOB_ID
        db_mock.run_now.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        op.execute(None)

        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "notebook_task": NOTEBOOK_TASK,
                "jar_params": JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksRunNowOperator",
        )
        db_mock.find_job_id_by_name.assert_called_once_with(JOB_NAME)
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_failure_if_job_id_not_found(self, db_mock_class):
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_name=JOB_NAME, json=run)
        db_mock = db_mock_class.return_value
        db_mock.find_job_id_by_name.return_value = None

        exception_message = f"Job ID for job name {JOB_NAME} can not be found"
        with pytest.raises(AirflowException, match=exception_message):
            op.execute(None)

        db_mock.find_job_id_by_name.assert_called_once_with(JOB_NAME)


class TestDatabricksRunNowDeferrableOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_execute_task_deferred(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowDeferrableOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        with pytest.raises(TaskDeferred) as exc:
            op.execute(None)
        assert isinstance(exc.value.trigger, DatabricksExecutionTrigger)
        assert exc.value.method_name == "execute_complete"

        expected = utils.normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "notebook_task": NOTEBOOK_TASK,
                "jar_params": JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksRunNowOperator",
        )

        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        assert op.run_id == RUN_ID

    def test_execute_complete_success(self):
        """
        Test `execute_complete` function in case the Trigger has returned a successful completion event.
        """
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        event = {
            "run_id": RUN_ID,
            "run_page_url": RUN_PAGE_URL,
            "run_state": RunState("TERMINATED", "SUCCESS", "").to_json(),
        }

        op = DatabricksRunNowDeferrableOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        assert op.execute_complete(context=None, event=event) is None

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_execute_complete_failure(self, db_mock_class):
        """
        Test `execute_complete` function in case the Trigger has returned a failure completion event.
        """
        run_state_failed = RunState("TERMINATED", "FAILED", "")
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        event = {
            "run_id": RUN_ID,
            "run_page_url": RUN_PAGE_URL,
            "run_state": run_state_failed.to_json(),
        }

        op = DatabricksRunNowDeferrableOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        with pytest.raises(AirflowException):
            op.execute_complete(context=None, event=event)

        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        with pytest.raises(AirflowException, match=f"Job run failed with terminal state: {run_state_failed}"):
            op.execute_complete(context=None, event=event)

    def test_execute_complete_incorrect_event_validation_failure(self):
        event = {"event_id": "no such column"}
        op = DatabricksRunNowDeferrableOperator(task_id=TASK_ID, job_id=JOB_ID)
        with pytest.raises(AirflowException):
            op.execute_complete(context=None, event=event)
