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

import json
import os
import warnings
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.links.athena import AthenaQueryResultsLink
from airflow.providers.amazon.aws.operators.athena import (
    _DURABLE_UNSET,
    AthenaOperator,
    _warn_and_disable_durable_pre_3_3,
)
from airflow.providers.amazon.aws.triggers.athena import AthenaTrigger
from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    ExternalQueryRunFacet,
    Identifier,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SQLJobFacet,
    SymlinksDatasetFacet,
)
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import timezone
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.taskinstance import create_task_instance, get_template_context
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_3_PLUS
from unit.amazon.aws.utils.test_template_fields import validate_template_fields

TEST_DAG_ID = "unit_tests"
DEFAULT_DATE = timezone.datetime(2018, 1, 1)
ATHENA_QUERY_ID = "eac29bf8-daa1-4ffc-b19a-0db31dc3b784"

MOCK_DATA = {
    "task_id": "test_athena_operator",
    "query": "SELECT * FROM TEST_TABLE",
    "database": "TEST_DATABASE",
    "catalog": "AwsDataCatalog",
    "outputLocation": "s3://test_s3_bucket/",
    "client_request_token": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
    "workgroup": "primary",
}

query_context = {"Database": MOCK_DATA["database"], "Catalog": MOCK_DATA["catalog"]}
result_configuration = {"OutputLocation": MOCK_DATA["outputLocation"]}


@mock_aws
class TestAthenaOperator:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        args = {
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        }

        self.dag = DAG(TEST_DAG_ID, default_args=args, schedule="@once")

        self.default_op_kwargs = dict(
            task_id="test_athena_operator",
            query="SELECT * FROM TEST_TABLE",
            database="TEST_DATABASE",
            client_request_token="eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
            sleep_time=0,
            max_polling_attempts=3,
        )
        self.athena = AthenaOperator(
            **self.default_op_kwargs, output_location="s3://test_s3_bucket/", aws_conn_id=None, dag=self.dag
        )

        with mock.patch("airflow.providers.amazon.aws.links.athena.AthenaQueryResultsLink.persist") as m:
            self.mocked_athena_result_link = m
            yield

    def test_base_aws_op_attributes(self):
        op = AthenaOperator(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None
        assert op.hook.log_query is True

        op = AthenaOperator(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
            log_query=False,
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42
        assert op.hook.log_query is False

    def test_init(self):
        assert self.athena.task_id == MOCK_DATA["task_id"]
        assert self.athena.query == MOCK_DATA["query"]
        assert self.athena.database == MOCK_DATA["database"]
        assert self.athena.catalog == MOCK_DATA["catalog"]
        assert self.athena.client_request_token == MOCK_DATA["client_request_token"]
        assert self.athena.sleep_time == 0

    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_override_catalog(self, mock_conn, mock_run_query, mock_check_query_status):
        query_context_catalog = {"Database": MOCK_DATA["database"], "Catalog": "MyCatalog"}
        self.athena.catalog = "MyCatalog"
        self.athena.execute({})
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context_catalog,
            result_configuration=result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )
        assert mock_check_query_status.call_count == 1

    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_without_database(self, mock_conn, mock_run_query, mock_check_query_status):
        op_kwargs = self.default_op_kwargs.copy()
        op_kwargs["task_id"] = "test_athena_operator_without_database"
        op_kwargs.pop("database")
        op = AthenaOperator(
            **op_kwargs, output_location="s3://test_s3_bucket/", aws_conn_id=None, dag=self.dag
        )
        op.execute({})
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context={"Catalog": MOCK_DATA["catalog"]},
            result_configuration=result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )
        assert mock_check_query_status.call_count == 1

    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_small_success_query(self, mock_conn, mock_run_query, mock_check_query_status):
        self.athena.execute({})
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context,
            result_configuration=result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )
        assert mock_check_query_status.call_count == 1

        # Validate call persist Athena Query result link
        self.mocked_athena_result_link.assert_called_once_with(
            aws_partition=mock.ANY,
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            query_execution_id=ATHENA_QUERY_ID,
        )

    @mock.patch.object(
        AthenaHook,
        "check_query_status",
        side_effect="SUCCEEDED",
    )
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_big_success_query(self, mock_conn, mock_run_query, mock_check_query_status):
        self.athena.execute({})
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context,
            result_configuration=result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )

    @mock.patch.object(AthenaHook, "get_state_change_reason")
    @mock.patch.object(AthenaHook, "check_query_status", return_value="FAILED")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_failure_query(
        self,
        mock_conn,
        mock_run_query,
        mock_check_query_status,
        mock_get_state_change_reason,
    ):
        with pytest.raises(AirflowException):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context,
            result_configuration=result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )
        assert mock_get_state_change_reason.call_count == 1

    @mock.patch.object(AthenaHook, "check_query_status", return_value="CANCELLED")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_cancelled_query(self, mock_conn, mock_run_query, mock_check_query_status):
        with pytest.raises(AirflowException):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context,
            result_configuration=result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )

    @mock.patch.object(AthenaHook, "check_query_status", return_value="RUNNING")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_failed_query_with_max_tries(self, mock_conn, mock_run_query, mock_check_query_status):
        with pytest.raises(AirflowException):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context,
            result_configuration=result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )

    @pytest.mark.db_test
    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_return_value(
        self,
        mock_conn,
        mock_run_query,
        mock_check_query_status,
        session,
        clean_dags_dagruns_and_dagbundles,
        testing_dag_bundle,
    ):
        """Test we return the right value -- that will get put in to XCom by the execution engine"""
        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion

            sync_dag_to_db(self.dag)
            dag_version = DagVersion.get_latest_version(self.dag.dag_id)
            ti = create_task_instance(task=self.athena, dag_version_id=dag_version.id)
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                logical_date=timezone.utcnow(),
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
            )
        else:
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                execution_date=timezone.utcnow(),
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
            )
            ti = TaskInstance(task=self.athena)
        ti.dag_run = dag_run
        session.add(ti)
        session.commit()
        self.athena.durable = False
        assert self.athena.execute(get_template_context(ti, self.athena)) == ATHENA_QUERY_ID

    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_optional_output_location(self, mock_conn, mock_run_query, mock_check_query_status):
        op = AthenaOperator(**self.default_op_kwargs, aws_conn_id=None)

        op.execute({})
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context,
            result_configuration={},  # Should be an empty dict since we do not provide output_location
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )

    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    def test_is_deferred(self, mock_run_query):
        self.athena.deferrable = True

        with pytest.raises(TaskDeferred) as deferred:
            self.athena.execute(None)

        assert isinstance(deferred.value.trigger, AthenaTrigger)

        # Validate call persist Athena Query result link
        self.mocked_athena_result_link.assert_called_once_with(
            aws_partition=mock.ANY,
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            query_execution_id=ATHENA_QUERY_ID,
        )

    def test_execute_complete_reassigns_query_execution_id_after_deferring(self):
        """Assert that we use query_execution_id from event after deferral."""

        operator = AthenaOperator(
            task_id="test_athena_operator",
            query="SELECT * FROM TEST_TABLE",
            database="TEST_DATABASE",
            deferrable=True,
        )
        assert operator.query_execution_id is None

        query_execution_id = "123456"
        operator.execute_complete(
            context=None,
            event={"status": "success", "value": query_execution_id},
        )
        assert operator.query_execution_id == query_execution_id

    @mock.patch.object(AthenaOperator, "get_openlineage_dataset")
    def test_openlineage_uses_database_from_query_execution_context(self, mock_get_dataset):
        op = AthenaOperator(
            task_id="test_athena_openlineage",
            query="INSERT INTO TEST_TABLE SELECT CUSTOMER_EMAIL FROM DISCOUNTS",
            database=None,
            query_execution_context={"Database": "TEST_DATABASE"},
            dag=self.dag,
        )

        op.get_openlineage_facets_on_complete(None)

        mock_get_dataset.assert_any_call("TEST_DATABASE", "DISCOUNTS")
        mock_get_dataset.assert_any_call("TEST_DATABASE", "TEST_TABLE")

    @mock.patch.object(AthenaHook, "region_name", new_callable=mock.PropertyMock)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_operator_openlineage_data(self, mock_conn, mock_region_name):
        mock_region_name.return_value = "eu-west-1"

        def mock_get_table_metadata(CatalogName, DatabaseName, TableName):
            with open(os.path.dirname(__file__) + "/athena_metadata.json") as f:
                return json.load(f)[TableName]

        mock_conn.return_value.get_table_metadata = mock_get_table_metadata

        op = AthenaOperator(
            task_id="test_athena_openlineage",
            query="INSERT INTO TEST_TABLE SELECT CUSTOMER_EMAIL FROM DISCOUNTS",
            database="TEST_DATABASE",
            output_location="s3://test_s3_bucket",
            client_request_token="eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
            sleep_time=0,
            max_polling_attempts=3,
            dag=self.dag,
        )
        op.query_execution_id = "12345"  # Mocking what will be available after execution

        expected_lineage = OperatorLineage(
            inputs=[
                Dataset(
                    namespace="awsathena://athena.eu-west-1.amazonaws.com",
                    name="AwsDataCatalog.TEST_DATABASE.DISCOUNTS",
                    facets={
                        "symlinks": SymlinksDatasetFacet(
                            identifiers=[
                                Identifier(
                                    namespace="s3://bucket",
                                    name="/discount/data/path/",
                                    type="TABLE",
                                )
                            ],
                        ),
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaDatasetFacetFields(
                                    name="ID",
                                    type="int",
                                    description="from deserializer",
                                ),
                                SchemaDatasetFacetFields(
                                    name="AMOUNT_OFF",
                                    type="int",
                                    description="from deserializer",
                                ),
                                SchemaDatasetFacetFields(
                                    name="CUSTOMER_EMAIL",
                                    type="varchar",
                                    description="from deserializer",
                                ),
                                SchemaDatasetFacetFields(
                                    name="STARTS_ON",
                                    type="timestamp",
                                    description="from deserializer",
                                ),
                                SchemaDatasetFacetFields(
                                    name="ENDS_ON",
                                    type="timestamp",
                                    description=None,
                                ),
                            ],
                        ),
                    },
                )
            ],
            outputs=[
                Dataset(
                    namespace="awsathena://athena.eu-west-1.amazonaws.com",
                    name="AwsDataCatalog.TEST_DATABASE.TEST_TABLE",
                    facets={
                        "symlinks": SymlinksDatasetFacet(
                            identifiers=[
                                Identifier(
                                    namespace="s3://bucket",
                                    name="/data/test_table/data/path",
                                    type="TABLE",
                                )
                            ],
                        ),
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaDatasetFacetFields(
                                    name="column",
                                    type="string",
                                    description="from deserializer",
                                )
                            ],
                        ),
                    },
                ),
                Dataset(namespace="s3://test_s3_bucket", name="/"),
            ],
            job_facets={
                "sql": SQLJobFacet(
                    query="INSERT INTO TEST_TABLE SELECT CUSTOMER_EMAIL FROM DISCOUNTS",
                )
            },
            run_facets={"externalQuery": ExternalQueryRunFacet(externalQueryId="12345", source="awsathena")},
        )
        assert op.get_openlineage_facets_on_complete(None) == expected_lineage

    def test_template_fields(self):
        validate_template_fields(self.athena)


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS, reason="task_state_store (durable execution) requires Airflow 3.3+"
)
class TestAthenaOperatorDurableExecution:
    @staticmethod
    def _invalid_request_error(message: str) -> ClientError:
        return ClientError(
            error_response={
                "Error": {"Code": "InvalidRequestException", "Message": message},
                "ResponseMetadata": {"HTTPStatusCode": 400},
            },
            operation_name="GetQueryExecution",
        )

    @staticmethod
    def _context(task_state_store):
        return {
            "ti": mock.MagicMock(spec_set=["stats_tags"], stats_tags={}),
            "task_state_store": task_state_store,
        }

    @staticmethod
    def _make_operator(**kwargs):
        return AthenaOperator(
            task_id="test_athena_durable",
            query=MOCK_DATA["query"],
            database=MOCK_DATA["database"],
            output_location=MOCK_DATA["outputLocation"],
            client_request_token=MOCK_DATA["client_request_token"],
            sleep_time=0,
            max_polling_attempts=3,
            aws_conn_id=None,
            **kwargs,
        )

    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "poll_query_status", return_value="SUCCEEDED")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    def test_fresh_submission_persists_before_polling(self, mock_run_query, mock_poll, mock_persist):
        operator = self._make_operator()
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = None
        persisted_before_poll = []
        mock_poll.side_effect = lambda *args, **kwargs: (
            persisted_before_poll.append(task_state_store.set.call_args.args) or "SUCCEEDED"
        )

        result = operator.execute(self._context(task_state_store))

        assert result == ATHENA_QUERY_ID
        task_state_store.set.assert_called_once_with("athena_query_execution_id", ATHENA_QUERY_ID)
        assert persisted_before_poll == [("athena_query_execution_id", ATHENA_QUERY_ID)]
        mock_run_query.assert_called_once()
        mock_persist.assert_called_once()

    @pytest.mark.parametrize("status", AthenaHook.INTERMEDIATE_STATES)
    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "poll_query_status", return_value="SUCCEEDED")
    @mock.patch.object(AthenaHook, "check_query_status")
    @mock.patch.object(AthenaHook, "run_query")
    def test_active_query_reconnects_without_resubmission(
        self, mock_run_query, mock_check_status, mock_poll, mock_persist, status
    ):
        operator = self._make_operator()
        mock_check_status.return_value = status
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = ATHENA_QUERY_ID

        result = operator.execute(self._context(task_state_store))

        assert result == ATHENA_QUERY_ID
        assert operator.query_execution_id == ATHENA_QUERY_ID
        mock_run_query.assert_not_called()
        task_state_store.set.assert_not_called()
        mock_poll.assert_called_once_with(
            query_execution_id=ATHENA_QUERY_ID,
            max_polling_attempts=3,
            sleep_time=0,
        )
        mock_persist.assert_called_once_with(
            context=mock.ANY,
            operator=operator,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            query_execution_id=ATHENA_QUERY_ID,
        )

    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "poll_query_status")
    @mock.patch.object(AthenaHook, "check_query_status", return_value="SUCCEEDED")
    @mock.patch.object(AthenaHook, "run_query")
    def test_succeeded_query_recovers_without_polling_or_resubmission(
        self, mock_run_query, mock_check_status, mock_poll, mock_persist
    ):
        operator = self._make_operator()
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = ATHENA_QUERY_ID

        result = operator.execute(self._context(task_state_store))

        assert result == ATHENA_QUERY_ID
        assert operator.query_execution_id == ATHENA_QUERY_ID
        mock_run_query.assert_not_called()
        mock_poll.assert_not_called()
        task_state_store.set.assert_not_called()
        mock_persist.assert_called_once()

    @pytest.mark.parametrize("status", AthenaHook.FAILURE_STATES)
    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "poll_query_status", return_value="SUCCEEDED")
    @mock.patch.object(AthenaHook, "check_query_status")
    @mock.patch.object(AthenaHook, "run_query", return_value="new-query-id")
    def test_terminal_query_resubmits_with_unchanged_client_token(
        self, mock_run_query, mock_check_status, mock_poll, mock_persist, status
    ):
        operator = self._make_operator()
        mock_check_status.return_value = status
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = ATHENA_QUERY_ID

        result = operator.execute(self._context(task_state_store))

        assert result == "new-query-id"
        assert operator.query_execution_id == "new-query-id"
        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context,
            result_configuration=result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
            workgroup=MOCK_DATA["workgroup"],
        )
        task_state_store.set.assert_called_once_with("athena_query_execution_id", "new-query-id")

    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "poll_query_status", return_value="SUCCEEDED")
    @mock.patch.object(AthenaHook, "check_query_status")
    @mock.patch.object(AthenaHook, "run_query", return_value="new-query-id")
    def test_missing_query_resubmits(self, mock_run_query, mock_check_status, mock_poll, mock_persist):
        operator = self._make_operator()
        mock_check_status.side_effect = self._invalid_request_error(
            f"QueryExecution {ATHENA_QUERY_ID} was not found"
        )
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = ATHENA_QUERY_ID

        assert operator.execute(self._context(task_state_store)) == "new-query-id"

        mock_run_query.assert_called_once()
        task_state_store.set.assert_called_once_with("athena_query_execution_id", "new-query-id")

    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "check_query_status")
    @mock.patch.object(AthenaHook, "run_query")
    @pytest.mark.parametrize(
        "message",
        [
            "QueryExecutionId is malformed",
            "QueryExecution 0f8db785-a48f-42c2-84c5-d749b034234a was not found",
        ],
    )
    def test_other_invalid_request_is_raised(self, mock_run_query, mock_check_status, mock_persist, message):
        operator = self._make_operator()
        error = self._invalid_request_error(message)
        mock_check_status.side_effect = error
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = ATHENA_QUERY_ID

        with pytest.raises(ClientError) as caught:
            operator.execute(self._context(task_state_store))

        assert caught.value is error
        mock_run_query.assert_not_called()

    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "poll_query_status", return_value="SUCCEEDED")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    def test_durable_false_submits_without_reading_or_writing_store(
        self, mock_run_query, mock_poll, mock_persist
    ):
        operator = self._make_operator(durable=False)
        task_state_store = mock.MagicMock(spec_set=["get", "set"])

        assert operator.execute(self._context(task_state_store)) == ATHENA_QUERY_ID

        task_state_store.get.assert_not_called()
        task_state_store.set.assert_not_called()
        mock_run_query.assert_called_once()

    @mock.patch.object(AthenaOperator, "defer")
    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    def test_deferrable_execution_does_not_use_task_state_store(
        self, mock_run_query, mock_persist, mock_defer
    ):
        operator = self._make_operator(deferrable=True)
        task_state_store = mock.MagicMock(spec_set=["get", "set"])

        operator.execute(self._context(task_state_store))

        mock_defer.assert_called_once()
        task_state_store.get.assert_not_called()
        task_state_store.set.assert_not_called()
        mock_run_query.assert_called_once()

    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "poll_query_status", side_effect=RuntimeError("worker stopped"))
    @mock.patch.object(AthenaHook, "check_query_status", return_value="RUNNING")
    @mock.patch.object(AthenaHook, "run_query")
    @mock.patch.object(AthenaHook, "stop_query", return_value={"ResponseMetadata": {"HTTPStatusCode": 200}})
    def test_on_kill_cancels_reconnected_query(
        self, mock_stop_query, mock_run_query, mock_check_status, mock_poll, mock_persist
    ):
        operator = self._make_operator()
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = ATHENA_QUERY_ID

        with pytest.raises(RuntimeError, match="worker stopped"):
            operator.execute(self._context(task_state_store))

        mock_poll.side_effect = None
        mock_poll.return_value = "CANCELLED"
        operator.on_kill()

        mock_stop_query.assert_called_once_with(ATHENA_QUERY_ID)
        mock_run_query.assert_not_called()

    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "poll_query_status", return_value="SUCCEEDED")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    def test_unset_client_request_token_remains_unset(self, mock_run_query, mock_poll, mock_persist):
        operator = AthenaOperator(
            task_id="test_athena_durable_without_token",
            query=MOCK_DATA["query"],
            database=MOCK_DATA["database"],
            output_location=MOCK_DATA["outputLocation"],
            sleep_time=0,
            max_polling_attempts=3,
            aws_conn_id=None,
        )
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = None

        assert operator.execute(self._context(task_state_store)) == ATHENA_QUERY_ID

        mock_run_query.assert_called_once_with(
            query=MOCK_DATA["query"],
            query_context=query_context,
            result_configuration=result_configuration,
            client_request_token=None,
            workgroup=MOCK_DATA["workgroup"],
        )

    @pytest.mark.parametrize("status", [None, "UNKNOWN"])
    @mock.patch.object(AthenaQueryResultsLink, "persist")
    @mock.patch.object(AthenaHook, "check_query_status")
    @mock.patch.object(AthenaHook, "run_query")
    def test_unknown_recovered_status_does_not_resubmit(
        self, mock_run_query, mock_check_status, mock_persist, status
    ):
        operator = self._make_operator()
        mock_check_status.return_value = status
        task_state_store = mock.MagicMock(spec_set=["get", "set"])
        task_state_store.get.return_value = ATHENA_QUERY_ID

        with pytest.raises(ValueError, match="Unexpected Athena query status"):
            operator.execute(self._context(task_state_store))

        mock_run_query.assert_not_called()

    def test_default_args_durable_reaches_operator(self):
        with DAG(
            dag_id="test_athena_durable_default_args",
            schedule=None,
            start_date=DEFAULT_DATE,
            default_args={"durable": False},
        ):
            operator = self._make_operator()
        assert operator.durable is False


class TestWarnAndDisableDurableAirflowPre3_3:
    def test_no_warning_when_unset(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _warn_and_disable_durable_pre_3_3(_DURABLE_UNSET)
        assert result is False
        assert caught == []

    @pytest.mark.parametrize("value", [True, False])
    def test_warns_and_disables_when_explicitly_set(self, value):
        with pytest.warns(UserWarning, match="durable.*no effect"):
            result = _warn_and_disable_durable_pre_3_3(value)
        assert result is False
