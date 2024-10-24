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


class ForbiddenWarningsPlugin:
    """Internal plugin for restricting warnings during the tests run."""

    node_key: str = "forbidden_warnings_node"

    def __init__(self, config: pytest.Config, forbidden_warnings: tuple[str, ...]):
        excluded_cases = {
            # Skip: Integration and System Tests
            "tests/integration/",
            "tests/system/",
            "providers/tests/integration/",
            "providers/tests/system/",
            # Skip: DAGs for tests
            "tests/dags/",
            "tests/dags_corrupted/",
            "tests/dags_with_system_exit/",
            # CLI
            # https://github.com/apache/airflow/issues/39199
            "tests/cli/commands/test_kubernetes_command.py::TestGenerateDagYamlCommand::test_generate_dag_yaml",
            # Core
            "tests/models/test_dagbag.py::TestDagBag::test_load_subdags",
            "tests/models/test_mappedoperator.py::test_expand_mapped_task_instance_with_named_index",
            "tests/models/test_xcom.py::TestXCom::test_set_serialize_call_old_signature",
            # WWW
            "tests/www/api/experimental/test_dag_runs_endpoint.py::TestDagRunsEndpoint::test_get_dag_runs_success",
            "tests/www/api/experimental/test_dag_runs_endpoint.py::TestDagRunsEndpoint::test_get_dag_runs_success_with_capital_state_parameter",
            "tests/www/api/experimental/test_dag_runs_endpoint.py::TestDagRunsEndpoint::test_get_dag_runs_success_with_state_no_result",
            "tests/www/api/experimental/test_dag_runs_endpoint.py::TestDagRunsEndpoint::test_get_dag_runs_success_with_state_parameter",
            "tests/www/api/experimental/test_endpoints.py::TestApiExperimental::test_dagrun_status",
            "tests/www/api/experimental/test_endpoints.py::TestApiExperimental::test_get_dag_code",
            "tests/www/api/experimental/test_endpoints.py::TestApiExperimental::test_info",
            "tests/www/api/experimental/test_endpoints.py::TestApiExperimental::test_task_info",
            "tests/www/api/experimental/test_endpoints.py::TestApiExperimental::test_task_instance_info",
            "tests/www/api/experimental/test_endpoints.py::TestApiExperimental::test_trigger_dag",
            "tests/www/api/experimental/test_endpoints.py::TestApiExperimental::test_trigger_dag_for_date",
            "tests/www/api/experimental/test_endpoints.py::TestPoolApiExperimental::test_create_pool",
            "tests/www/api/experimental/test_endpoints.py::TestPoolApiExperimental::test_create_pool_with_bad_name",
            "tests/www/api/experimental/test_endpoints.py::TestPoolApiExperimental::test_delete_default_pool",
            "tests/www/api/experimental/test_endpoints.py::TestPoolApiExperimental::test_delete_pool",
            "tests/www/api/experimental/test_endpoints.py::TestPoolApiExperimental::test_delete_pool_non_existing",
            "tests/www/api/experimental/test_endpoints.py::TestPoolApiExperimental::test_get_pool",
            "tests/www/api/experimental/test_endpoints.py::TestPoolApiExperimental::test_get_pool_non_existing",
            "tests/www/api/experimental/test_endpoints.py::TestPoolApiExperimental::test_get_pools",
            "tests/www/views/test_views_acl.py::test_success",
            "tests/www/views/test_views_rendered.py::test_rendered_task_detail_env_secret",
            "tests/www/views/test_views_tasks.py::test_rendered_task_view",
            "tests/www/views/test_views_tasks.py::test_views_get",
            # Providers
            "providers/tests/amazon/aws/deferrable/hooks/test_base_aws.py::TestAwsBaseAsyncHook::test_get_client_async",
            "providers/tests/amazon/aws/deferrable/hooks/test_redshift_cluster.py::TestRedshiftAsyncHook::test_cluster_status",
            "providers/tests/amazon/aws/deferrable/hooks/test_redshift_cluster.py::TestRedshiftAsyncHook::test_get_cluster_status"
            "providers/tests/amazon/aws/deferrable/hooks/test_redshift_cluster.py::TestRedshiftAsyncHook::test_get_cluster_status_exception",
            "providers/tests/amazon/aws/deferrable/hooks/test_redshift_cluster.py::TestRedshiftAsyncHook::test_pause_cluster",
            "providers/tests/amazon/aws/deferrable/hooks/test_redshift_cluster.py::TestRedshiftAsyncHook::test_resume_cluster",
            "providers/tests/amazon/aws/deferrable/hooks/test_redshift_cluster.py::TestRedshiftAsyncHook::test_resume_cluster_exception",
            "providers/tests/amazon/aws/triggers/test_redshift_cluster.py::TestRedshiftClusterTrigger::test_redshift_cluster_sensor_trigger_exception",
            "providers/tests/amazon/aws/triggers/test_redshift_cluster.py::TestRedshiftClusterTrigger::test_redshift_cluster_sensor_trigger_resuming_status",
            "providers/tests/amazon/aws/triggers/test_redshift_cluster.py::TestRedshiftClusterTrigger::test_redshift_cluster_sensor_trigger_success",
            "providers/tests/google/common/auth_backend/test_google_openid.py::TestGoogleOpenID::test_success",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_api_resource_configs",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_api_resource_configs_duplication_warning",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_cancel_queries",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_cancel_query_cancel_completed",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_cancel_query_cancel_timeout",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_cancel_query_jobs_to_cancel",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_get_dataset_tables_list",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_invalid_schema_update_and_write_disposition",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_invalid_schema_update_options",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_invalid_source_format",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_extract",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_load_with_non_csv_as_src_fmt",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_query_schema_update_options",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_query_schema_update_options_incorrect",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_query_sql_dialect",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_query_sql_dialect_default",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_query_sql_dialect_legacy_with_query_params",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_query_sql_dialect_legacy_with_query_params_fails",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_query_with_arg",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_query_without_sql_fails",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookMethods::test_run_table_delete",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryHookRunWithConfiguration::test_run_with_configuration_location",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryWithKMS::test_create_external_table_with_kms",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryWithKMS::test_run_copy_with_kms",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryWithKMS::test_run_load_with_kms",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryWithKMS::test_run_query_with_kms",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryWithLabelsAndDescription::test_create_external_table_description",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryWithLabelsAndDescription::test_create_external_table_labels",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryWithLabelsAndDescription::test_run_load_description",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestBigQueryWithLabelsAndDescription::test_run_load_labels",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestClusteringInRunJob::test_run_load_default",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestClusteringInRunJob::test_run_load_with_arg",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestClusteringInRunJob::test_run_query_default",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestClusteringInRunJob::test_run_query_with_arg",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestDatasetsOperations::test_patch_dataset",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestTableOperations::test_patch_table",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestTimePartitioningInRunJob::test_run_load_default",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestTimePartitioningInRunJob::test_run_load_with_arg",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestTimePartitioningInRunJob::test_run_query_with_arg",
            "providers/tests/google/cloud/hooks/test_bigquery.py::TestTimePartitioningInRunJob::test_run_with_auto_detect",
            "providers/tests/google/cloud/hooks/test_gcs.py::TestGCSHook::test_list__error_match_glob_and_invalid_delimiter",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithDefaultProjectIdFromConnection::test_error_operation",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithDefaultProjectIdFromConnection::test_life_science_client_creation",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithDefaultProjectIdFromConnection::test_run_pipeline_immediately_complete",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithDefaultProjectIdFromConnection::test_waiting_operation",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithPassedProjectId::test_delegate_to_runtime_error",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithPassedProjectId::test_error_operation",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithPassedProjectId::test_life_science_client_creation",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithPassedProjectId::test_location_path",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithPassedProjectId::test_run_pipeline_immediately_complete",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithPassedProjectId::test_waiting_operation",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithoutProjectId::test_life_science_client_creation",
            "providers/tests/google/cloud/hooks/test_life_sciences.py::TestLifeSciencesHookWithoutProjectId::test_run_pipeline",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithDefaultProjectIdHook::test_cancel_pipeline_job",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithDefaultProjectIdHook::test_create_pipeline_job",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithDefaultProjectIdHook::test_delete_pipeline_job",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithDefaultProjectIdHook::test_get_pipeline_job",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithDefaultProjectIdHook::test_list_pipeline_jobs",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithoutDefaultProjectIdHook::test_cancel_pipeline_job",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithoutDefaultProjectIdHook::test_create_pipeline_job",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithoutDefaultProjectIdHook::test_delete_pipeline_job",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithoutDefaultProjectIdHook::test_get_pipeline_job",
            "providers/tests/google/cloud/hooks/vertex_ai/test_custom_job.py::TestCustomJobWithoutDefaultProjectIdHook::test_list_pipeline_jobs",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcHadoopOperator::test_execute",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcHiveOperator::test_builder",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcHiveOperator::test_execute",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcPigOperator::test_builder",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcPigOperator::test_execute",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcPySparkOperator::test_execute",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcSparkOperator::test_execute",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcSparkSqlOperator::test_builder",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcSparkSqlOperator::test_execute",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataProcSparkSqlOperator::test_execute_override_project_id",
            "providers/tests/google/cloud/operators/test_dataproc.py::TestDataprocClusterScaleOperator::test_execute",
            "providers/tests/google/cloud/operators/test_dataproc.py::test_create_cluster_operator_extra_links",
            "providers/tests/google/cloud/operators/test_dataproc.py::test_scale_cluster_operator_extra_links",
            "providers/tests/google/cloud/operators/test_dataproc.py::test_submit_spark_job_operator_extra_links",
            "providers/tests/google/cloud/operators/test_gcs.py::TestGoogleCloudStorageListOperator::test_execute__delimiter",
            "providers/tests/google/cloud/operators/test_kubernetes_engine.py::TestGoogleCloudPlatformContainerOperator::test_create_execute_error_body",
            "providers/tests/google/cloud/operators/test_life_sciences.py::TestLifeSciencesRunPipelineOperator::test_executes",
            "providers/tests/google/cloud/operators/test_life_sciences.py::TestLifeSciencesRunPipelineOperator::test_executes_without_project_id",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_copy_files_into_a_folder",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_last_modified_time",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_more_than_1_wildcard",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_no_prefix",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_no_suffix",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_prefix_and_suffix",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_wildcard_empty_destination_object",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_wildcard_with_destination_object",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_wildcard_with_destination_object_retained_prefix",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_wildcard_with_replace_flag_false",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_wildcard_with_replace_flag_false_with_destination_object",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_execute_wildcard_without_destination_object",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_executes_with_a_delimiter",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_executes_with_delimiter_and_destination_object",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_executes_with_different_delimiter_and_destination_object",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_wc_with_last_modified_time_with_all_true_cond",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_wc_with_last_modified_time_with_one_true_cond",
            "providers/tests/google/cloud/transfers/test_gcs_to_gcs.py::TestGoogleCloudStorageToCloudStorageOperator::test_wc_with_no_last_modified_time",
        }

        self.config = config
        self.forbidden_warnings = forbidden_warnings
        self.is_worker_node = hasattr(config, "workerinput")
        self.detected_cases: set[str] = set()
        self.excluded_cases: tuple[str, ...] = tuple(sorted(excluded_cases))

    @staticmethod
    def prune_params_node_id(node_id: str) -> str:
        """Remove parametrized parts from node id."""
        return node_id.partition("[")[0]

    def pytest_itemcollected(self, item: pytest.Item):
        if item.nodeid.startswith(self.excluded_cases):
            return
        for fw in self.forbidden_warnings:
            # Add marker at the beginning of the markers list. In this case, it does not conflict with
            # filterwarnings markers, which are set explicitly in the test suite.
            item.add_marker(pytest.mark.filterwarnings(f"error::{fw}"), append=False)
        item.add_marker(
            pytest.mark.filterwarnings(
                "ignore:Timer and timing metrics publish in seconds were deprecated. It is enabled by default from Airflow 3 onwards. Enable metrics consistency to publish all the timer and timing metrics in milliseconds.:DeprecationWarning"
            )
        )

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int):
        """Save set of test node ids in the session finish on xdist worker node."""
        yield
        if self.is_worker_node and self.detected_cases and hasattr(self.config, "workeroutput"):
            self.config.workeroutput[self.node_key] = frozenset(self.detected_cases)

    @pytest.hookimpl(optionalhook=True)
    def pytest_testnodedown(self, node, error):
        """Get a set of test node ids from the xdist worker node."""
        if not (workeroutput := getattr(node, "workeroutput", {})):
            return

        node_detected_cases: tuple[tuple[str, int]] = workeroutput.get(self.node_key)
        if not node_detected_cases:
            return

        self.detected_cases |= node_detected_cases

    def pytest_exception_interact(self, node: pytest.Item, call: pytest.CallInfo, report: pytest.TestReport):
        if not call.excinfo or call.when not in ["setup", "call", "teardown"]:
            # Skip analyze exception if there is no exception exists
            # or exception happens outside of tests or fixtures
            return

        exc = call.excinfo.type
        exception_qualname = exc.__name__
        if (exception_module := exc.__module__) != "builtins":
            exception_qualname = f"{exception_module}.{exception_qualname}"
        if exception_qualname in self.forbidden_warnings:
            self.detected_cases.add(node.nodeid)

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_terminal_summary(self, terminalreporter, exitstatus: int, config: pytest.Config):
        yield
        if not self.detected_cases or self.is_worker_node:  # No need to print report on worker node
            return

        total_cases = len(self.detected_cases)
        uniq_tests_cases = len(set(map(self.prune_params_node_id, self.detected_cases)))
        terminalreporter.section(f"{total_cases:,} prohibited warning(s) detected", red=True, bold=True)

        report_message = "By default selected warnings are prohibited during tests runs:\n * "
        report_message += "\n * ".join(self.forbidden_warnings)
        report_message += "\n\n"
        report_message += (
            "Please make sure that you follow Airflow Unit test developer guidelines:\n"
            "https://github.com/apache/airflow/blob/main/contributing-docs/testing/unit_tests.rst#handling-warnings"
        )
        if total_cases <= 20:
            # Print tests node ids only if there is a small amount of it,
            # otherwise it could turn into a mess in the terminal
            report_message += "\n\nWarnings detected in test case(s):\n - "
            report_message += "\n - ".join(sorted(self.detected_cases))
        if uniq_tests_cases >= 15:
            # If there are a lot of unique tests where this happens,
            # we might suggest adding it into the exclusion list
            report_message += (
                "\n\nIf this is significant change in code base you might add tests cases ids into the "
                "forbidden_warnings.py file, please make sure that you also create "
                "follow up Issue/Task in https://github.com/apache/airflow/issues"
            )
        terminalreporter.write_line(report_message.rstrip(), red=True)
        terminalreporter.write_line(
            "You could disable this check by provide the `--disable-forbidden-warnings` flag, "
            "however this check always turned on in the Airflow's CI.",
            white=True,
        )
