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

from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.decorators import task
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import RunState
from airflow.providers.databricks.operators.databricks import (
    DatabricksCreateJobsOperator,
    DatabricksNotebookOperator,
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
    DatabricksTaskBaseOperator,
    DatabricksTaskOperator,
)
from airflow.providers.databricks.triggers.databricks import DatabricksExecutionTrigger
from airflow.providers.databricks.utils import databricks as utils
from airflow.utils import timezone

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
JOB_DESCRIPTION = "job-description"
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
NOTIFICATION_SETTINGS = {"no_alert_for_canceled_runs": True, "no_alert_for_skipped_runs": True}
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
    def test_validate_json_with_named_parameters(self):
        """
        Test the _setup_and_validate_json function with the named parameters.
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
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
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

    def test_validate_json_with_json(self):
        """
        Test the _setup_and_validate_json function with json data.
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
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
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

    def test_validate_json_with_merging(self):
        """
        Test the _setup_and_validate_json function when json and other named parameters are both
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
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
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

    def test_validate_json_with_templating(self):
        json = {"name": "test-{{ ds }}"}

        dag = DAG("test", start_date=datetime.now())
        op = DatabricksCreateJobsOperator(dag=dag, task_id=TASK_ID, json=json)
        op.render_template_fields(context={"ds": DATE})
        op._setup_and_validate_json()

        expected = utils._normalise_json_content({"name": f"test-{DATE}"})
        assert expected == op.json

    def test_validate_json_with_bad_type(self):
        json = {"test": datetime.now(), "name": "test"}
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = (
            r"Type \<(type|class) \'datetime.datetime\'\> used "
            r"for parameter json\[test\] is not a number or a string"
        )
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)._setup_and_validate_json()

    def test_validate_json_with_no_name(self):
        json = {}
        exception_message = "Missing required parameter: name"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)._setup_and_validate_json()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_validate_json_with_templated_json(self, db_mock_class, dag_maker):
        json = "{{ ti.xcom_pull(task_ids='push_json') }}"
        with dag_maker("test_templated", render_template_as_native_obj=True):
            push_json = PythonOperator(
                task_id="push_json",
                python_callable=lambda: {
                    "name": JOB_NAME,
                    "description": JOB_DESCRIPTION,
                    "tags": TAGS,
                    "tasks": TASKS,
                    "job_clusters": JOB_CLUSTERS,
                    "email_notifications": EMAIL_NOTIFICATIONS,
                    "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                    "notification_settings": NOTIFICATION_SETTINGS,
                    "timeout_seconds": TIMEOUT_SECONDS,
                    "schedule": SCHEDULE,
                    "max_concurrent_runs": MAX_CONCURRENT_RUNS,
                    "git_source": GIT_SOURCE,
                    "access_control_list": ACCESS_CONTROL_LIST,
                },
            )
            op = DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)
            push_json >> op

        db_mock = db_mock_class.return_value
        db_mock.create_job.return_value = JOB_ID

        db_mock.find_job_id_by_name.return_value = None

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["push_json"].run()
        tis[TASK_ID].run()

        expected = utils._normalise_json_content(
            {
                "name": JOB_NAME,
                "description": JOB_DESCRIPTION,
                "tags": TAGS,
                "tasks": TASKS,
                "job_clusters": JOB_CLUSTERS,
                "email_notifications": EMAIL_NOTIFICATIONS,
                "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                "notification_settings": NOTIFICATION_SETTINGS,
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
        assert JOB_ID == tis[TASK_ID].xcom_pull()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_validate_json_with_xcomarg_json(self, db_mock_class, dag_maker):
        with dag_maker("test_xcomarg", render_template_as_native_obj=True):

            @task
            def push_json() -> dict:
                return {
                    "name": JOB_NAME,
                    "description": JOB_DESCRIPTION,
                    "tags": TAGS,
                    "tasks": TASKS,
                    "job_clusters": JOB_CLUSTERS,
                    "email_notifications": EMAIL_NOTIFICATIONS,
                    "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                    "notification_settings": NOTIFICATION_SETTINGS,
                    "timeout_seconds": TIMEOUT_SECONDS,
                    "schedule": SCHEDULE,
                    "max_concurrent_runs": MAX_CONCURRENT_RUNS,
                    "git_source": GIT_SOURCE,
                    "access_control_list": ACCESS_CONTROL_LIST,
                }

            json = push_json()
            op = DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)

        db_mock = db_mock_class.return_value
        db_mock.create_job.return_value = JOB_ID

        db_mock.find_job_id_by_name.return_value = None

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["push_json"].run()
        tis[TASK_ID].run()

        expected = utils._normalise_json_content(
            {
                "name": JOB_NAME,
                "description": JOB_DESCRIPTION,
                "tags": TAGS,
                "tasks": TASKS,
                "job_clusters": JOB_CLUSTERS,
                "email_notifications": EMAIL_NOTIFICATIONS,
                "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                "notification_settings": NOTIFICATION_SETTINGS,
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
        assert JOB_ID == tis[TASK_ID].xcom_pull()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_create(self, db_mock_class):
        """
        Test the execute function in case where the job does not exist.
        """
        json = {
            "name": JOB_NAME,
            "description": JOB_DESCRIPTION,
            "tags": TAGS,
            "tasks": TASKS,
            "job_clusters": JOB_CLUSTERS,
            "email_notifications": EMAIL_NOTIFICATIONS,
            "webhook_notifications": WEBHOOK_NOTIFICATIONS,
            "notification_settings": NOTIFICATION_SETTINGS,
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

        expected = utils._normalise_json_content(
            {
                "name": JOB_NAME,
                "description": JOB_DESCRIPTION,
                "tags": TAGS,
                "tasks": TASKS,
                "job_clusters": JOB_CLUSTERS,
                "email_notifications": EMAIL_NOTIFICATIONS,
                "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                "notification_settings": NOTIFICATION_SETTINGS,
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
            "description": JOB_DESCRIPTION,
            "tags": TAGS,
            "tasks": TASKS,
            "job_clusters": JOB_CLUSTERS,
            "email_notifications": EMAIL_NOTIFICATIONS,
            "webhook_notifications": WEBHOOK_NOTIFICATIONS,
            "notification_settings": NOTIFICATION_SETTINGS,
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

        expected = utils._normalise_json_content(
            {
                "name": JOB_NAME,
                "description": JOB_DESCRIPTION,
                "tags": TAGS,
                "tasks": TASKS,
                "job_clusters": JOB_CLUSTERS,
                "email_notifications": EMAIL_NOTIFICATIONS,
                "webhook_notifications": WEBHOOK_NOTIFICATIONS,
                "notification_settings": NOTIFICATION_SETTINGS,
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

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_update_job_permission(self, db_mock_class):
        """
        Test job permission update.
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

        op.execute({})

        expected = utils._normalise_json_content({"access_control_list": ACCESS_CONTROL_LIST})

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksCreateJobsOperator",
        )

        db_mock.update_job_permission.assert_called_once_with(JOB_ID, expected)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_update_job_permission_with_empty_acl(self, db_mock_class):
        """
        Test job permission update.
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
        }
        op = DatabricksCreateJobsOperator(task_id=TASK_ID, json=json)
        db_mock = db_mock_class.return_value
        db_mock.find_job_id_by_name.return_value = JOB_ID

        op.execute({})

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksCreateJobsOperator",
        )

        db_mock.update_job_permission.assert_not_called()


class TestDatabricksSubmitRunOperator:
    def test_validate_json_with_notebook_task_named_parameters(self):
        """
        Test the _setup_and_validate_json function with named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, notebook_task=NOTEBOOK_TASK
        )
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": TASK_ID}
        )

        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_spark_python_task_named_parameters(self):
        """
        Test the _setup_and_validate_json function with the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, spark_python_task=SPARK_PYTHON_TASK
        )
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "spark_python_task": SPARK_PYTHON_TASK, "run_name": TASK_ID}
        )

        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_pipeline_name_task_named_parameters(self):
        """
        Test the _setup_and_validate_json function with the named parameters.
        """
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, pipeline_task=PIPELINE_NAME_TASK)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content({"pipeline_task": PIPELINE_NAME_TASK, "run_name": TASK_ID})

        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_pipeline_id_task_named_parameters(self):
        """
        Test the _setup_and_validate_json function with the named parameters.
        """
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, pipeline_task=PIPELINE_ID_TASK)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content({"pipeline_task": PIPELINE_ID_TASK, "run_name": TASK_ID})

        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_spark_submit_task_named_parameters(self):
        """
        Test the _setup_and_validate_json function with the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, spark_submit_task=SPARK_SUBMIT_TASK
        )
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "spark_submit_task": SPARK_SUBMIT_TASK, "run_name": TASK_ID}
        )

        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_dbt_task_named_parameters(self):
        """
        Test the _setup_and_validate_json function with the named parameters.
        """
        git_source = {
            "git_url": "https://github.com/dbt-labs/jaffle_shop",
            "git_provider": "github",
            "git_branch": "main",
        }
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, dbt_task=DBT_TASK, git_source=git_source
        )
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "dbt_task": DBT_TASK, "git_source": git_source, "run_name": TASK_ID}
        )

        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_dbt_task_mixed_parameters(self):
        """
        Test the _setup_and_validate_json function with mixed parameters.
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
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "dbt_task": DBT_TASK, "git_source": git_source, "run_name": TASK_ID}
        )

        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_dbt_task_without_git_source_raises_error(self):
        """
        Test the _setup_and_validate_json function without the necessary git_source for dbt_task raises error.
        """
        exception_message = "git_source is required for dbt_task"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksSubmitRunOperator(
                task_id=TASK_ID, new_cluster=NEW_CLUSTER, dbt_task=DBT_TASK
            )._setup_and_validate_json()

    def test_validate_json_with_dbt_task_json_without_git_source_raises_error(self):
        """
        Test the _setup_and_validate_json function without the necessary git_source for dbt_task raises error.
        """
        json = {"dbt_task": DBT_TASK, "new_cluster": NEW_CLUSTER}

        exception_message = "git_source is required for dbt_task"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)._setup_and_validate_json()

    def test_validate_json_with_json(self):
        """
        Test the _setup_and_validate_json function with json data.
        """
        json = {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": TASK_ID}
        )
        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_tasks(self):
        tasks = [{"task_key": 1, "new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK}]
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, tasks=tasks)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content({"run_name": TASK_ID, "tasks": tasks})
        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_specified_run_name(self):
        """
        Test the _setup_and_validate_json function with a specified run_name.
        """
        json = {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": RUN_NAME}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": RUN_NAME}
        )
        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_pipeline_task(self):
        """
        Test the _setup_and_validate_json function with a pipeline task.
        """
        pipeline_task = {"pipeline_id": "test-dlt"}
        json = {"new_cluster": NEW_CLUSTER, "run_name": RUN_NAME, "pipeline_task": pipeline_task}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {"new_cluster": NEW_CLUSTER, "pipeline_task": pipeline_task, "run_name": RUN_NAME}
        )
        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_merging(self):
        """
        Test the _setup_and_validate_json function when json and other named parameters are both
        provided. The named parameters should override top level keys in the
        json dict.
        """
        override_new_cluster = {"workers": 999}
        json = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json, new_cluster=override_new_cluster)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {
                "new_cluster": override_new_cluster,
                "notebook_task": NOTEBOOK_TASK,
                "run_name": TASK_ID,
            }
        )
        assert expected == utils._normalise_json_content(op.json)

    @pytest.mark.db_test
    def test_validate_json_with_templating(self):
        json = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": TEMPLATED_NOTEBOOK_TASK,
        }
        dag = DAG("test", start_date=datetime.now())
        op = DatabricksSubmitRunOperator(dag=dag, task_id=TASK_ID, json=json)
        op._setup_and_validate_json()

        op.render_template_fields(context={"ds": DATE})
        expected = utils._normalise_json_content(
            {
                "new_cluster": NEW_CLUSTER,
                "notebook_task": RENDERED_TEMPLATED_NOTEBOOK_TASK,
                "run_name": TASK_ID,
            }
        )
        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_git_source(self):
        json = {"new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK, "run_name": RUN_NAME}
        git_source = {
            "git_url": "https://github.com/apache/airflow",
            "git_provider": "github",
            "git_branch": "main",
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, git_source=git_source, json=json)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {
                "new_cluster": NEW_CLUSTER,
                "notebook_task": NOTEBOOK_TASK,
                "run_name": RUN_NAME,
                "git_source": git_source,
            }
        )
        assert expected == utils._normalise_json_content(op.json)

    def test_validate_json_with_bad_type(self):
        json = {"test": datetime.now()}
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = (
            r"Type \<(type|class) \'datetime.datetime\'\> used "
            r"for parameter json\[test\] is not a number or a string"
        )
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)._setup_and_validate_json()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_validate_json_with_templated_json(self, db_mock_class, dag_maker):
        json = "{{ ti.xcom_pull(task_ids='push_json') }}"
        with dag_maker("test_templated", render_template_as_native_obj=True):
            push_json = PythonOperator(
                task_id="push_json",
                python_callable=lambda: {
                    "new_cluster": NEW_CLUSTER,
                    "notebook_task": NOTEBOOK_TASK,
                },
            )
            op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
            push_json >> op

        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run_page_url.return_value = RUN_PAGE_URL
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["push_json"].run()
        tis[TASK_ID].run()

        expected = utils._normalise_json_content(
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
    def test_validate_json_with_xcomarg_json(self, db_mock_class, dag_maker):
        with dag_maker("test_xcomarg", render_template_as_native_obj=True):

            @task
            def push_json() -> dict:
                return {
                    "new_cluster": NEW_CLUSTER,
                    "notebook_task": NOTEBOOK_TASK,
                }

            json = push_json()

            op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run_page_url.return_value = RUN_PAGE_URL
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["push_json"].run()
        tis[TASK_ID].run()

        expected = utils._normalise_json_content(
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
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        op.execute(None)

        expected = utils._normalise_json_content(
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
    def test_exec_pipeline_name(self, db_mock_class):
        """
        Test the execute function when provided a pipeline name.
        """
        run = {"pipeline_task": {"pipeline_name": "This is a test pipeline"}}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.find_pipeline_id_by_name.return_value = PIPELINE_ID_TASK["pipeline_id"]
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        op.execute(None)

        expected = utils._normalise_json_content({"pipeline_task": PIPELINE_ID_TASK, "run_name": TASK_ID})
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksSubmitRunOperator",
        )
        db_mock.find_pipeline_id_by_name.assert_called_once_with("This is a test pipeline")

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
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        with pytest.raises(AirflowException):
            op.execute(None)

        expected = utils._normalise_json_content(
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
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        assert op.wait_for_termination

        op.execute(None)

        expected = utils._normalise_json_content(
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
        db_mock.submit_run.return_value = RUN_ID

        assert not op.wait_for_termination

        op.execute(None)

        expected = utils._normalise_json_content(
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

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_execute_task_deferred(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(deferrable=True, task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("RUNNING", "RUNNING")

        with pytest.raises(TaskDeferred) as exc:
            op.execute(None)
        assert isinstance(exc.value.trigger, DatabricksExecutionTrigger)
        assert exc.value.method_name == "execute_complete"

        expected = utils._normalise_json_content(
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
            "errors": [],
        }

        op = DatabricksSubmitRunOperator(deferrable=True, task_id=TASK_ID, json=run)
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
            "repair_run": False,
            "errors": [],
        }

        op = DatabricksSubmitRunOperator(deferrable=True, task_id=TASK_ID, json=run)
        with pytest.raises(AirflowException):
            op.execute_complete(context=None, event=event)

        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        with pytest.raises(AirflowException, match=f"Job run failed with terminal state: {run_state_failed}"):
            op.execute_complete(context=None, event=event)

    def test_execute_complete_incorrect_event_validation_failure(self):
        event = {"event_id": "no such column"}
        op = DatabricksSubmitRunOperator(deferrable=True, task_id=TASK_ID)
        with pytest.raises(AirflowException):
            op.execute_complete(context=None, event=event)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator.defer")
    def test_databricks_submit_run_deferrable_operator_failed_before_defer(self, mock_defer, db_mock_class):
        """Asserts that a task is not deferred when its failed"""
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(deferrable=True, task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")
        op.execute(None)

        expected = utils._normalise_json_content(
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
        assert op.run_id == RUN_ID
        assert not mock_defer.called

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator.defer")
    def test_databricks_submit_run_deferrable_operator_success_before_defer(self, mock_defer, db_mock_class):
        """Asserts that a task is not deferred when it succeeds"""
        run = {
            "new_cluster": NEW_CLUSTER,
            "notebook_task": NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(deferrable=True, task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")
        op.execute(None)

        expected = utils._normalise_json_content(
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
        assert op.run_id == RUN_ID
        assert not mock_defer.called


class TestDatabricksRunNowOperator:
    def test_validate_json_with_named_parameters(self):
        """
        Test the _setup_and_validate_json function with named parameters.
        """
        op = DatabricksRunNowOperator(job_id=JOB_ID, task_id=TASK_ID)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content({"job_id": 42})

        assert expected == op.json

    def test_validate_json_with_json(self):
        """
        Test the _setup_and_validate_json function with json data.
        """
        json = {
            "notebook_params": NOTEBOOK_PARAMS,
            "jar_params": JAR_PARAMS,
            "python_params": PYTHON_PARAMS,
            "spark_submit_params": SPARK_SUBMIT_PARAMS,
            "job_id": JOB_ID,
            "repair_run": False,
        }
        op = DatabricksRunNowOperator(task_id=TASK_ID, json=json)
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "jar_params": JAR_PARAMS,
                "python_params": PYTHON_PARAMS,
                "spark_submit_params": SPARK_SUBMIT_PARAMS,
                "job_id": JOB_ID,
                "repair_run": False,
            }
        )

        assert expected == op.json

    def test_validate_json_with_merging(self):
        """
        Test the _setup_and_validate_json function when json and other named parameters are both
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
        op._setup_and_validate_json()

        expected = utils._normalise_json_content(
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
    def test_validate_json_with_templating(self):
        json = {"notebook_params": NOTEBOOK_PARAMS, "jar_params": TEMPLATED_JAR_PARAMS}

        dag = DAG("test", start_date=datetime.now())
        op = DatabricksRunNowOperator(dag=dag, task_id=TASK_ID, job_id=JOB_ID, json=json)
        op.render_template_fields(context={"ds": DATE})
        op._setup_and_validate_json()
        expected = utils._normalise_json_content(
            {
                "notebook_params": NOTEBOOK_PARAMS,
                "jar_params": RENDERED_TEMPLATED_JAR_PARAMS,
                "job_id": JOB_ID,
            }
        )
        assert expected == op.json

    def test_validate_json_with_bad_type(self):
        json = {"test": datetime.now()}
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = (
            r"Type \<(type|class) \'datetime.datetime\'\> used "
            r"for parameter json\[test\] is not a number or a string"
        )
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=json)._setup_and_validate_json()

    def test_validate_json_exception_with_job_name_and_job_id(self):
        exception_message = "Argument 'job_name' is not allowed with argument 'job_id'"

        with pytest.raises(AirflowException, match=exception_message):
            DatabricksRunNowOperator(
                task_id=TASK_ID, job_id=JOB_ID, job_name=JOB_NAME
            )._setup_and_validate_json()

        run = {"job_id": JOB_ID, "job_name": JOB_NAME}
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksRunNowOperator(task_id=TASK_ID, json=run)._setup_and_validate_json()

        run = {"job_id": JOB_ID}
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksRunNowOperator(task_id=TASK_ID, json=run, job_name=JOB_NAME)._setup_and_validate_json()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_validate_json_with_templated_json(self, db_mock_class, dag_maker):
        json = "{{ ti.xcom_pull(task_ids='push_json') }}"
        with dag_maker("test_templated", render_template_as_native_obj=True):
            push_json = PythonOperator(
                task_id="push_json",
                python_callable=lambda: {
                    "notebook_params": NOTEBOOK_PARAMS,
                    "notebook_task": NOTEBOOK_TASK,
                    "jar_params": JAR_PARAMS,
                    "job_id": JOB_ID,
                },
            )
            op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=json)
            push_json >> op

        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run_page_url.return_value = RUN_PAGE_URL
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["push_json"].run()
        tis[TASK_ID].run()

        expected = utils._normalise_json_content(
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
    def test_validate_json_with_xcomarg_json(self, db_mock_class, dag_maker):
        with dag_maker("test_xcomarg", render_template_as_native_obj=True):

            @task
            def push_json() -> dict:
                return {
                    "notebook_params": NOTEBOOK_PARAMS,
                    "notebook_task": NOTEBOOK_TASK,
                    "jar_params": JAR_PARAMS,
                    "job_id": JOB_ID,
                }

            json = push_json()

            op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=json)

        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run_page_url.return_value = RUN_PAGE_URL
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        dagrun = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        tis = {ti.task_id: ti for ti in dagrun.task_instances}
        tis["push_json"].run()
        tis[TASK_ID].run()

        expected = utils._normalise_json_content(
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
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        op.execute(None)

        expected = utils._normalise_json_content(
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
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        with pytest.raises(AirflowException):
            op.execute(None)

        expected = utils._normalise_json_content(
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
        db_mock.run_now.return_value = RUN_ID
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
                        "task_key": "first_task",
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

        with pytest.raises(AirflowException, match="Exception: Something went wrong"):
            op.execute(None)

        expected = utils._normalise_json_content(
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
    def test_exec_multiple_failures_with_message(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
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
                        "task_key": "first_task",
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "FAILED",
                            "state_message": "failed",
                        },
                    },
                    {
                        "run_id": 3,
                        "task_key": "second_task",
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "FAILED",
                            "state_message": "failed",
                        },
                    },
                ],
            }
        )
        db_mock.get_run_output = mock_dict({"error": "Exception: Something went wrong..."})

        with pytest.raises(
            AirflowException,
            match="(?=.*Exception: Something went wrong.*)(?=.*Exception: Something went wrong.*)",
        ):
            op.execute(None)

        expected = utils._normalise_json_content(
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
        db_mock.get_run_output.assert_called()
        assert db_mock.get_run_output.call_count == 2
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
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        assert op.wait_for_termination

        op.execute(None)

        expected = utils._normalise_json_content(
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
        db_mock.run_now.return_value = RUN_ID

        assert not op.wait_for_termination

        op.execute(None)

        expected = utils._normalise_json_content(
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

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_exec_with_job_name(self, db_mock_class):
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_name=JOB_NAME, json=run)
        db_mock = db_mock_class.return_value
        db_mock.find_job_id_by_name.return_value = JOB_ID
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        op.execute(None)

        expected = utils._normalise_json_content(
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

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_cancel_previous_runs(self, db_mock_class):
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(
            task_id=TASK_ID, job_id=JOB_ID, cancel_previous_runs=True, wait_for_termination=False, json=run
        )
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID

        assert op.cancel_previous_runs

        op.execute(None)

        expected = utils._normalise_json_content(
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

        db_mock.cancel_all_runs.assert_called_once_with(JOB_ID)
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_not_called()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_no_cancel_previous_runs(self, db_mock_class):
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(
            task_id=TASK_ID, job_id=JOB_ID, cancel_previous_runs=False, wait_for_termination=False, json=run
        )
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID

        assert not op.cancel_previous_runs

        op.execute(None)

        expected = utils._normalise_json_content(
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

        db_mock.cancel_all_runs.assert_not_called()
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run.assert_not_called()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_execute_task_deferred(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(deferrable=True, task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("RUNNING", "RUNNING")

        with pytest.raises(TaskDeferred) as exc:
            op.execute(None)
        assert isinstance(exc.value.trigger, DatabricksExecutionTrigger)
        assert exc.value.method_name == "execute_complete"

        expected = utils._normalise_json_content(
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
            "repair_run": False,
            "errors": [],
        }

        op = DatabricksRunNowOperator(deferrable=True, task_id=TASK_ID, job_id=JOB_ID, json=run)
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
            "repair_run": False,
            "errors": [],
        }

        op = DatabricksRunNowOperator(deferrable=True, task_id=TASK_ID, job_id=JOB_ID, json=run)
        with pytest.raises(AirflowException):
            op.execute_complete(context=None, event=event)

        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        with pytest.raises(AirflowException, match=f"Job run failed with terminal state: {run_state_failed}"):
            op.execute_complete(context=None, event=event)

    @mock.patch(
        "airflow.providers.databricks.operators.databricks._handle_deferrable_databricks_operator_execution"
    )
    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_execute_complete_failure_and_repair_run(
        self, db_mock_class, mock_handle_deferrable_databricks_operator_execution
    ):
        """
        Test `execute_complete` function in case the Trigger has returned a failure event with repair_run=True.
        """
        run_state_failed = RunState("TERMINATED", "FAILED", "")
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        event = {
            "run_id": RUN_ID,
            "run_page_url": RUN_PAGE_URL,
            "run_state": run_state_failed.to_json(),
            "repair_run": True,
            "errors": [],
        }

        op = DatabricksRunNowOperator(deferrable=True, task_id=TASK_ID, job_id=JOB_ID, json=run)
        op.execute_complete(context=None, event=event)

        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")
        db_mock.get_latest_repair_id.assert_called_once()
        db_mock.repair_run.assert_called_once()
        mock_handle_deferrable_databricks_operator_execution.assert_called_once()

    def test_execute_complete_incorrect_event_validation_failure(self):
        event = {"event_id": "no such column"}
        op = DatabricksRunNowOperator(deferrable=True, task_id=TASK_ID, job_id=JOB_ID)
        with pytest.raises(AirflowException):
            op.execute_complete(context=None, event=event)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator.defer")
    def test_databricks_run_now_deferrable_operator_failed_before_defer(self, mock_defer, db_mock_class):
        """Asserts that a task is not deferred when its failed"""
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(deferrable=True, task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "FAILED")

        op.execute(None)
        expected = utils._normalise_json_content(
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
        assert not mock_defer.called

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator.defer")
    def test_databricks_run_now_deferrable_operator_success_before_defer(self, mock_defer, db_mock_class):
        """Asserts that a task is not deferred when its succeeds"""
        run = {"notebook_params": NOTEBOOK_PARAMS, "notebook_task": NOTEBOOK_TASK, "jar_params": JAR_PARAMS}
        op = DatabricksRunNowOperator(deferrable=True, task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = RUN_ID
        db_mock.get_run = make_run_with_state_mock("TERMINATED", "SUCCESS")

        op.execute(None)

        expected = utils._normalise_json_content(
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
        assert not mock_defer.called


class TestDatabricksNotebookOperator:
    def test_is_instance_of_databricks_task_base_operator(self):
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
        )

        assert isinstance(operator, DatabricksTaskBaseOperator)

    def test_execute_with_wait_for_termination(self):
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
        )
        operator._launch_job = MagicMock(return_value=12345)
        operator.monitor_databricks_job = MagicMock()

        operator.execute({})

        assert operator.wait_for_termination is True
        operator._launch_job.assert_called_once()
        operator.monitor_databricks_job.assert_called_once()

    def test_execute_without_wait_for_termination(self):
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
            wait_for_termination=False,
        )
        operator._launch_job = MagicMock(return_value=12345)
        operator.monitor_databricks_job = MagicMock()

        operator.execute({})

        assert operator.wait_for_termination is False
        operator._launch_job.assert_called_once()
        operator.monitor_databricks_job.assert_not_called()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    @mock.patch(
        "airflow.providers.databricks.operators.databricks.DatabricksNotebookOperator._get_current_databricks_task"
    )
    def test_execute_with_deferrable(self, mock_get_current_task, mock_databricks_hook):
        mock_databricks_hook.return_value.get_run.return_value = {
            "state": {"life_cycle_state": "PENDING"},
            "run_page_url": "test_url",
        }
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
            wait_for_termination=True,
            deferrable=True,
        )
        operator.databricks_run_id = 12345

        with pytest.raises(TaskDeferred) as exec_info:
            operator.monitor_databricks_job()
        assert isinstance(
            exec_info.value.trigger, DatabricksExecutionTrigger
        ), "Trigger is not a DatabricksExecutionTrigger"
        assert exec_info.value.method_name == "execute_complete"

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    @mock.patch(
        "airflow.providers.databricks.operators.databricks.DatabricksNotebookOperator._get_current_databricks_task"
    )
    def test_execute_with_deferrable_early_termination(self, mock_get_current_task, mock_databricks_hook):
        mock_databricks_hook.return_value.get_run.return_value = {
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "FAILURE",
            },
            "run_page_url": "test_url",
        }
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
            wait_for_termination=True,
            deferrable=True,
        )
        operator.databricks_run_id = 12345

        with pytest.raises(AirflowException) as exec_info:
            operator.monitor_databricks_job()
        exception_message = "Task failed. Final state FAILED. Reason: FAILURE"
        assert exception_message == str(exec_info.value)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    @mock.patch(
        "airflow.providers.databricks.operators.databricks.DatabricksNotebookOperator._get_current_databricks_task"
    )
    def test_monitor_databricks_job_successful_raises_no_exception(
        self, mock_get_current_task, mock_databricks_hook
    ):
        mock_databricks_hook.return_value.get_run.return_value = {
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            "run_page_url": "test_url",
        }

        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
        )

        operator.databricks_run_id = 12345
        operator.monitor_databricks_job()

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    @mock.patch(
        "airflow.providers.databricks.operators.databricks.DatabricksNotebookOperator._get_current_databricks_task"
    )
    def test_monitor_databricks_job_failed(self, mock_get_current_task, mock_databricks_hook):
        mock_databricks_hook.return_value.get_run.return_value = {
            "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED", "state_message": "FAILURE"},
            "run_page_url": "test_url",
        }

        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
        )

        operator.databricks_run_id = 12345

        with pytest.raises(AirflowException) as exc_info:
            operator.monitor_databricks_job()
        exception_message = "Task failed. Final state FAILED. Reason: FAILURE"
        assert exception_message == str(exc_info.value)

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook")
    def test_launch_notebook_job(self, mock_databricks_hook):
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
            existing_cluster_id="test_cluster_id",
        )
        operator._hook.submit_run.return_value = 12345

        run_id = operator._launch_job()

        assert run_id == 12345

    def test_both_new_and_existing_cluster_set(self):
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            new_cluster={"new_cluster_config_key": "new_cluster_config_value"},
            existing_cluster_id="existing_cluster_id",
            databricks_conn_id="test_conn_id",
        )
        with pytest.raises(ValueError) as exc_info:
            operator._get_run_json()
        exception_message = "Both new_cluster and existing_cluster_id are set. Only one should be set."
        assert str(exc_info.value) == exception_message

    def test_both_new_and_existing_cluster_unset(self):
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
        )
        with pytest.raises(ValueError) as exc_info:
            operator._get_run_json()
        exception_message = "Must specify either existing_cluster_id or new_cluster."
        assert str(exc_info.value) == exception_message

    def test_job_runs_forever_by_default(self):
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
            existing_cluster_id="existing_cluster_id",
        )
        run_json = operator._get_run_json()
        assert operator.execution_timeout is None
        assert run_json["timeout_seconds"] == 0

    def test_zero_execution_timeout_raises_error(self):
        operator = DatabricksNotebookOperator(
            task_id="test_task",
            notebook_path="test_path",
            source="test_source",
            databricks_conn_id="test_conn_id",
            existing_cluster_id="existing_cluster_id",
            execution_timeout=timedelta(seconds=0),
        )
        with pytest.raises(ValueError) as exc_info:
            operator._get_run_json()
        exception_message = (
            "If you've set an `execution_timeout` for the task, ensure it's not `0`. "
            "Set it instead to `None` if you desire the task to run indefinitely."
        )
        assert str(exc_info.value) == exception_message

    def test_extend_workflow_notebook_packages(self):
        """Test that the operator can extend the notebook packages of a Databricks workflow task group."""
        databricks_workflow_task_group = MagicMock()
        databricks_workflow_task_group.notebook_packages = [
            {"pypi": {"package": "numpy"}},
            {"pypi": {"package": "pandas"}},
        ]

        operator = DatabricksNotebookOperator(
            notebook_path="/path/to/notebook",
            source="WORKSPACE",
            task_id="test_task",
            notebook_packages=[
                {"pypi": {"package": "numpy"}},
                {"pypi": {"package": "scipy"}},
            ],
        )

        operator._extend_workflow_notebook_packages(databricks_workflow_task_group)

        assert operator.notebook_packages == [
            {"pypi": {"package": "numpy"}},
            {"pypi": {"package": "scipy"}},
            {"pypi": {"package": "pandas"}},
        ]

    def test_convert_to_databricks_workflow_task(self):
        """Test that the operator can convert itself to a Databricks workflow task."""
        dag = DAG(dag_id="example_dag", start_date=datetime.now())
        operator = DatabricksNotebookOperator(
            notebook_path="/path/to/notebook",
            source="WORKSPACE",
            task_id="test_task",
            notebook_packages=[
                {"pypi": {"package": "numpy"}},
                {"pypi": {"package": "scipy"}},
            ],
            dag=dag,
        )

        databricks_workflow_task_group = MagicMock()
        databricks_workflow_task_group.notebook_packages = [
            {"pypi": {"package": "numpy"}},
        ]
        databricks_workflow_task_group.notebook_params = {"param1": "value1"}

        operator.notebook_packages = [{"pypi": {"package": "pandas"}}]
        operator.notebook_params = {"param2": "value2"}
        operator.task_group = databricks_workflow_task_group
        operator.task_id = "test_task"
        operator.upstream_task_ids = ["upstream_task"]
        relevant_upstreams = [MagicMock(task_id="upstream_task")]

        task_json = operator._convert_to_databricks_workflow_task(relevant_upstreams)

        expected_json = {
            "task_key": "example_dag__test_task",
            "depends_on": [],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notebook_task": {
                "notebook_path": "/path/to/notebook",
                "source": "WORKSPACE",
                "base_parameters": {
                    "param2": "value2",
                    "param1": "value1",
                },
            },
            "libraries": [
                {"pypi": {"package": "pandas"}},
                {"pypi": {"package": "numpy"}},
            ],
        }

        assert task_json == expected_json

    def test_convert_to_databricks_workflow_task_no_task_group(self):
        """Test that an error is raised if the operator is not in a TaskGroup."""
        operator = DatabricksNotebookOperator(
            notebook_path="/path/to/notebook",
            source="WORKSPACE",
            task_id="test_task",
            notebook_packages=[
                {"pypi": {"package": "numpy"}},
                {"pypi": {"package": "scipy"}},
            ],
        )
        operator.task_group = None
        relevant_upstreams = [MagicMock(task_id="upstream_task")]

        with pytest.raises(
            AirflowException,
            match="Calling `_convert_to_databricks_workflow_task` without a parent TaskGroup.",
        ):
            operator._convert_to_databricks_workflow_task(relevant_upstreams)

    def test_convert_to_databricks_workflow_task_cluster_conflict(self):
        """Test that an error is raised if both `existing_cluster_id` and `job_cluster_key` are set."""
        operator = DatabricksNotebookOperator(
            notebook_path="/path/to/notebook",
            source="WORKSPACE",
            task_id="test_task",
            notebook_packages=[
                {"pypi": {"package": "numpy"}},
                {"pypi": {"package": "scipy"}},
            ],
        )
        databricks_workflow_task_group = MagicMock()
        operator.existing_cluster_id = "existing-cluster-id"
        operator.job_cluster_key = "job-cluster-key"
        operator.task_group = databricks_workflow_task_group
        relevant_upstreams = [MagicMock(task_id="upstream_task")]

        with pytest.raises(
            ValueError,
            match="Both existing_cluster_id and job_cluster_key are set. Only one can be set per task.",
        ):
            operator._convert_to_databricks_workflow_task(relevant_upstreams)


class TestDatabricksTaskOperator:
    def test_is_instance_of_databricks_task_base_operator(self):
        task_config = {
            "sql_task": {
                "query": {
                    "query_id": "c9cf6468-babe-41a6-abc3-10ac358c71ee",
                },
                "warehouse_id": "cf414a2206dfb397",
            }
        }
        operator = DatabricksTaskOperator(
            task_id="test_task",
            databricks_conn_id="test_conn_id",
            task_config=task_config,
        )

        assert isinstance(operator, DatabricksTaskBaseOperator)

    def test_get_task_base_json(self):
        task_config = {
            "sql_task": {
                "query": {
                    "query_id": "c9cf646-8babe-41a6-abc3-10ac358c71ee",
                },
                "warehouse_id": "cf414a2206dfb397",
            }
        }
        operator = DatabricksTaskOperator(
            task_id="test_task",
            databricks_conn_id="test_conn_id",
            task_config=task_config,
        )
        task_base_json = operator._get_task_base_json()

        assert operator.task_config == task_config
        assert task_base_json == task_config
