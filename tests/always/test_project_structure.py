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

import ast
import glob
import itertools
import mmap
import os

import pytest

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


class TestProjectStructure:
    def test_reference_to_providers_from_core(self):
        for filename in glob.glob(f"{ROOT_FOLDER}/example_dags/**/*.py", recursive=True):
            self.assert_file_not_contains(filename, "providers")

    def test_deprecated_packages(self):
        path_pattern = f"{ROOT_FOLDER}/airflow/contrib/**/*.py"

        for filename in glob.glob(path_pattern, recursive=True):
            if filename.endswith("/__init__.py"):
                self.assert_file_contains(filename, "This package is deprecated.")
            else:
                self.assert_file_contains(filename, "This module is deprecated.")

    def assert_file_not_contains(self, filename: str, pattern: str):
        with open(filename, "rb", 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, "utf-8")) != -1:
                pytest.fail(f"File {filename} not contains pattern - {pattern}")

    def assert_file_contains(self, filename: str, pattern: str):
        with open(filename, "rb", 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, "utf-8")) == -1:
                pytest.fail(f"File {filename} contains illegal pattern - {pattern}")

    def test_providers_modules_should_have_tests(self):
        """
        Assert every module in /airflow/providers has a corresponding test_ file in tests/airflow/providers.
        """
        # The test below had a but for quite a while and we missed a lot of modules to have tess
        # We should make sure that one goes to 0
        OVERLOOKED_TESTS = [
            "tests/providers/amazon/aws/executors/ecs/test_boto_schema.py",
            "tests/providers/amazon/aws/executors/ecs/test_ecs_executor_config.py",
            "tests/providers/amazon/aws/executors/ecs/test_utils.py",
            "tests/providers/amazon/aws/operators/test_emr.py",
            "tests/providers/amazon/aws/operators/test_sagemaker.py",
            "tests/providers/amazon/aws/sensors/test_emr.py",
            "tests/providers/amazon/aws/sensors/test_sagemaker.py",
            "tests/providers/amazon/aws/test_exceptions.py",
            "tests/providers/amazon/aws/triggers/test_athena.py",
            "tests/providers/amazon/aws/triggers/test_batch.py",
            "tests/providers/amazon/aws/triggers/test_eks.py",
            "tests/providers/amazon/aws/triggers/test_emr.py",
            "tests/providers/amazon/aws/triggers/test_glue_crawler.py",
            "tests/providers/amazon/aws/triggers/test_lambda_function.py",
            "tests/providers/amazon/aws/triggers/test_rds.py",
            "tests/providers/amazon/aws/triggers/test_redshift_cluster.py",
            "tests/providers/amazon/aws/triggers/test_step_function.py",
            "tests/providers/amazon/aws/utils/test_rds.py",
            "tests/providers/amazon/aws/utils/test_sagemaker.py",
            "tests/providers/amazon/aws/utils/test_sqs.py",
            "tests/providers/amazon/aws/utils/test_tags.py",
            "tests/providers/amazon/aws/waiters/test_base_waiter.py",
            "tests/providers/apache/cassandra/hooks/test_cassandra.py",
            "tests/providers/apache/druid/operators/test_druid_check.py",
            "tests/providers/apache/hdfs/hooks/test_hdfs.py",
            "tests/providers/apache/hdfs/log/test_hdfs_task_handler.py",
            "tests/providers/apache/hdfs/sensors/test_hdfs.py",
            "tests/providers/apache/hive/plugins/test_hive.py",
            "tests/providers/apache/kafka/hooks/test_base.py",
            "tests/providers/celery/executors/test_celery_executor_utils.py",
            "tests/providers/celery/executors/test_default_celery.py",
            "tests/providers/cncf/kubernetes/backcompat/test_backwards_compat_converters.py",
            "tests/providers/cncf/kubernetes/executors/test_kubernetes_executor_types.py",
            "tests/providers/cncf/kubernetes/executors/test_kubernetes_executor_utils.py",
            "tests/providers/cncf/kubernetes/operators/test_kubernetes_pod.py",
            "tests/providers/cncf/kubernetes/test_k8s_model.py",
            "tests/providers/cncf/kubernetes/test_kube_client.py",
            "tests/providers/cncf/kubernetes/test_kube_config.py",
            "tests/providers/cncf/kubernetes/test_pod_generator_deprecated.py",
            "tests/providers/cncf/kubernetes/test_pod_launcher_deprecated.py",
            "tests/providers/cncf/kubernetes/test_python_kubernetes_script.py",
            "tests/providers/cncf/kubernetes/test_secret.py",
            "tests/providers/cncf/kubernetes/triggers/test_kubernetes_pod.py",
            "tests/providers/cncf/kubernetes/utils/test_delete_from.py",
            "tests/providers/cncf/kubernetes/utils/test_k8s_hashlib_wrapper.py",
            "tests/providers/cncf/kubernetes/utils/test_xcom_sidecar.py",
            "tests/providers/daskexecutor/executors/test_dask_executor.py",
            "tests/providers/databricks/hooks/test_databricks_base.py",
            "tests/providers/docker/test_exceptions.py",
            "tests/providers/elasticsearch/log/test_es_json_formatter.py",
            "tests/providers/elasticsearch/log/test_es_response.py",
            "tests/providers/google/cloud/fs/test_gcs.py",
            "tests/providers/google/cloud/links/test_automl.py",
            "tests/providers/google/cloud/links/test_base.py",
            "tests/providers/google/cloud/links/test_bigquery.py",
            "tests/providers/google/cloud/links/test_bigquery_dts.py",
            "tests/providers/google/cloud/links/test_bigtable.py",
            "tests/providers/google/cloud/links/test_cloud_build.py",
            "tests/providers/google/cloud/links/test_cloud_functions.py",
            "tests/providers/google/cloud/links/test_cloud_memorystore.py",
            "tests/providers/google/cloud/links/test_cloud_sql.py",
            "tests/providers/google/cloud/links/test_cloud_storage_transfer.py",
            "tests/providers/google/cloud/links/test_cloud_tasks.py",
            "tests/providers/google/cloud/links/test_compute.py",
            "tests/providers/google/cloud/links/test_data_loss_prevention.py",
            "tests/providers/google/cloud/links/test_datacatalog.py",
            "tests/providers/google/cloud/links/test_dataflow.py",
            "tests/providers/google/cloud/links/test_dataform.py",
            "tests/providers/google/cloud/links/test_datafusion.py",
            "tests/providers/google/cloud/links/test_dataplex.py",
            "tests/providers/google/cloud/links/test_dataprep.py",
            "tests/providers/google/cloud/links/test_dataproc.py",
            "tests/providers/google/cloud/links/test_datastore.py",
            "tests/providers/google/cloud/links/test_kubernetes_engine.py",
            "tests/providers/google/cloud/links/test_life_sciences.py",
            "tests/providers/google/cloud/links/test_mlengine.py",
            "tests/providers/google/cloud/links/test_pubsub.py",
            "tests/providers/google/cloud/links/test_spanner.py",
            "tests/providers/google/cloud/links/test_stackdriver.py",
            "tests/providers/google/cloud/links/test_vertex_ai.py",
            "tests/providers/google/cloud/links/test_workflows.py",
            "tests/providers/google/cloud/operators/vertex_ai/test_auto_ml.py",
            "tests/providers/google/cloud/operators/vertex_ai/test_batch_prediction_job.py",
            "tests/providers/google/cloud/operators/vertex_ai/test_custom_job.py",
            "tests/providers/google/cloud/operators/vertex_ai/test_dataset.py",
            "tests/providers/google/cloud/operators/vertex_ai/test_endpoint_service.py",
            "tests/providers/google/cloud/operators/vertex_ai/test_hyperparameter_tuning_job.py",
            "tests/providers/google/cloud/operators/vertex_ai/test_model_service.py",
            "tests/providers/google/cloud/operators/vertex_ai/test_pipeline_job.py",
            "tests/providers/google/cloud/sensors/test_dataform.py",
            "tests/providers/google/cloud/transfers/test_bigquery_to_sql.py",
            "tests/providers/google/cloud/transfers/test_presto_to_gcs.py",
            "tests/providers/google/cloud/transfers/test_trino_to_gcs.py",
            "tests/providers/google/cloud/triggers/test_cloud_composer.py",
            "tests/providers/google/cloud/utils/test_bigquery.py",
            "tests/providers/google/cloud/utils/test_bigquery_get_data.py",
            "tests/providers/google/cloud/utils/test_dataform.py",
            "tests/providers/google/common/links/test_storage.py",
            "tests/providers/google/common/test_consts.py",
            "tests/providers/google/test_go_module_utils.py",
            "tests/providers/microsoft/azure/fs/test_adls.py",
            "tests/providers/microsoft/azure/operators/test_adls.py",
            "tests/providers/microsoft/azure/transfers/test_azure_blob_to_gcs.py",
            "tests/providers/microsoft/azure/triggers/test_wasb.py",
            "tests/providers/mongo/sensors/test_mongo.py",
            "tests/providers/openlineage/extractors/test_manager.py",
            "tests/providers/openlineage/plugins/test_adapter.py",
            "tests/providers/openlineage/plugins/test_facets.py",
            "tests/providers/openlineage/plugins/test_macros.py",
            "tests/providers/openlineage/test_sqlparser.py",
            "tests/providers/redis/operators/test_redis_publish.py",
            "tests/providers/redis/sensors/test_redis_key.py",
            "tests/providers/slack/notifications/test_slack_notifier.py",
            "tests/providers/snowflake/triggers/test_snowflake_trigger.py",
        ]

        # TODO: Should we extend this test to cover other directories?
        modules_files = list(glob.glob(f"{ROOT_FOLDER}/airflow/providers/**/*.py", recursive=True))

        # Make path relative
        modules_files = list(os.path.relpath(f, ROOT_FOLDER) for f in modules_files)
        # Exclude example_dags
        modules_files = list(f for f in modules_files if "/example_dags/" not in f)
        # Exclude __init__.py
        modules_files = list(f for f in modules_files if not f.endswith("__init__.py"))
        # Change airflow/ to tests/
        expected_test_files = list(
            f'tests/{f.partition("/")[2]}' for f in modules_files if not f.endswith("__init__.py")
        )
        # Add test_ prefix to filename
        expected_test_files = list(
            f'{f.rpartition("/")[0]}/test_{f.rpartition("/")[2]}'
            for f in expected_test_files
            if not f.endswith("__init__.py")
        )

        current_test_files = glob.glob(f"{ROOT_FOLDER}/tests/providers/**/*.py", recursive=True)
        # Make path relative
        current_test_files = (os.path.relpath(f, ROOT_FOLDER) for f in current_test_files)
        # Exclude __init__.py
        current_test_files = (f for f in current_test_files if not f.endswith("__init__.py"))

        modules_files = set(modules_files)
        expected_test_files = set(expected_test_files) - set(OVERLOOKED_TESTS)
        current_test_files = set(current_test_files)

        missing_tests_files = expected_test_files - expected_test_files.intersection(current_test_files)

        assert set() == missing_tests_files, "Detect missing tests in providers module - please add tests"

        added_test_files = current_test_files.intersection(OVERLOOKED_TESTS)
        assert set() == added_test_files, (
            "Detect added tests in providers module - please remove the tests "
            "from OVERLOOKED_TESTS list above"
        )


def get_imports_from_file(filepath: str):
    with open(filepath) as py_file:
        content = py_file.read()
    doc_node = ast.parse(content, filepath)
    import_names: set[str] = set()
    for current_node in ast.walk(doc_node):
        if not isinstance(current_node, (ast.Import, ast.ImportFrom)):
            continue
        for alias in current_node.names:
            name = alias.name
            fullname = f"{current_node.module}.{name}" if isinstance(current_node, ast.ImportFrom) else name
            import_names.add(fullname)
    return import_names


def filepath_to_module(filepath: str):
    filepath = os.path.relpath(os.path.abspath(filepath), ROOT_FOLDER)
    return filepath.replace("/", ".")[: -(len(".py"))]


def print_sorted(container: set, indent: str = "    ") -> None:
    sorted_container = sorted(container)
    print(f"{indent}" + f"\n{indent}".join(sorted_container))


class ProjectStructureTest:
    PROVIDER = "blank"
    CLASS_DIRS = {"operators", "sensors", "transfers"}
    CLASS_SUFFIXES = ["Operator", "Sensor"]

    def class_paths(self):
        """Override this method if your classes are located under different paths"""
        for resource_type in self.CLASS_DIRS:
            python_files = glob.glob(
                f"{ROOT_FOLDER}/airflow/providers/{self.PROVIDER}/**/{resource_type}/**.py", recursive=True
            )
            # Make path relative
            resource_files = (os.path.relpath(f, ROOT_FOLDER) for f in python_files)
            resource_files = (f for f in resource_files if not f.endswith("__init__.py"))
            yield from resource_files

    def list_of_classes(self):
        classes = {}
        for operator_file in self.class_paths():
            operators_paths = self.get_classes_from_file(f"{ROOT_FOLDER}/{operator_file}")
            classes.update(operators_paths)
        return classes

    def get_classes_from_file(self, filepath: str):
        with open(filepath) as py_file:
            content = py_file.read()
        doc_node = ast.parse(content, filepath)
        module = filepath_to_module(filepath)
        results: dict = {}
        for current_node in ast.walk(doc_node):
            if isinstance(current_node, ast.ClassDef) and current_node.name.endswith(
                tuple(self.CLASS_SUFFIXES)
            ):
                results[f"{module}.{current_node.name}"] = current_node
        return results


class ExampleCoverageTest(ProjectStructureTest):
    """Checks that every operator is covered by example"""

    # Those operators are deprecated, so we do not need examples for them
    DEPRECATED_CLASSES: set = set()

    # Those operators should not have examples as they are never used standalone (they are abstract)
    BASE_CLASSES: set = set()

    # Please add the examples to those operators at the earliest convenience :)
    MISSING_EXAMPLES_FOR_CLASSES: set = set()

    def example_paths(self):
        """Override this method if your example dags are located elsewhere"""
        # old_design:
        yield from glob.glob(
            f"{ROOT_FOLDER}/airflow/providers/{self.PROVIDER}/**/example_dags/example_*.py", recursive=True
        )
        # new_design:
        yield from glob.glob(
            f"{ROOT_FOLDER}/tests/system/providers/{self.PROVIDER}/**/example_*.py", recursive=True
        )

    def test_missing_examples(self):
        """
        Assert that all operators defined under operators, sensors and transfers directories
        are used in any of the example dags
        """
        classes = self.list_of_classes()
        assert 0 != len(classes), "Failed to retrieve operators, override class_paths if needed"
        classes = set(classes.keys())
        for example in self.example_paths():
            classes -= get_imports_from_file(example)

        covered_but_omitted = self.MISSING_EXAMPLES_FOR_CLASSES - classes
        classes -= self.MISSING_EXAMPLES_FOR_CLASSES
        classes -= self.DEPRECATED_CLASSES
        classes -= self.BASE_CLASSES
        if set() != classes:
            print("Classes with missing examples:")
            print_sorted(classes)
            pytest.fail(
                "Not all classes are covered with example dags. Update self.MISSING_EXAMPLES_FOR_CLASSES "
                "if you want to skip this error"
            )
        if set() != covered_but_omitted:
            print("Covered classes that are listed as missing:")
            print_sorted(covered_but_omitted)
            pytest.fail("Operator listed in missing examples but is used in example dag")


class AssetsCoverageTest(ProjectStructureTest):
    """Checks that every operator have operator_extra_links attribute"""

    # These operators should not have assets
    ASSETS_NOT_REQUIRED: set = set()

    # Please add assets to following classes
    MISSING_ASSETS_FOR_CLASSES: set = set()

    def test_missing_assets(self):
        classes = self.list_of_classes()
        assets, no_assets = set(), set()
        for name, operator in classes.items():
            for attr in operator.body:
                if (
                    isinstance(attr, ast.Assign)
                    and attr.targets
                    and getattr(attr.targets[0], "id", "") == "operator_extra_links"
                ):
                    assets.add(name)
                    break
            else:
                no_assets.add(name)

        asset_should_be_missing = self.ASSETS_NOT_REQUIRED - no_assets
        no_assets -= self.ASSETS_NOT_REQUIRED
        no_assets -= self.MISSING_ASSETS_FOR_CLASSES
        if set() != no_assets:
            print("Classes with missing assets:")
            print_sorted(no_assets)
            pytest.fail("Some classes are missing assets")
        if set() != asset_should_be_missing:
            print("Classes that should not have assets:")
            print_sorted(asset_should_be_missing)
            pytest.fail("Class should not have assets")


class TestGoogleProviderProjectStructure(ExampleCoverageTest, AssetsCoverageTest):
    PROVIDER = "google"
    CLASS_DIRS = ProjectStructureTest.CLASS_DIRS | {"operators/vertex_ai"}

    DEPRECATED_CLASSES = {
        "airflow.providers.google.cloud.operators.cloud_storage_transfer_service"
        ".CloudDataTransferServiceS3ToGCSOperator",
        "airflow.providers.google.cloud.operators.cloud_storage_transfer_service"
        ".CloudDataTransferServiceGCSToGCSOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineManageModelOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineManageVersionOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineCreateModelOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineCreateVersionOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteModelOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteVersionOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineGetModelOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineListVersionsOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineSetDefaultVersionOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator",
        "airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryPatchDatasetOperator",
        "airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360CreateQueryOperator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360RunQueryOperator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360DownloadReportV2Operator",
        "airflow.providers.google.marketing_platform.sensors.GoogleDisplayVideo360RunQuerySensor",
    }

    BASE_CLASSES = {
        "airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator",
        "airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryToSqlBaseOperator",
        "airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.custom_job.CustomTrainingJobBaseOperator",
        "airflow.providers.google.cloud.operators.cloud_base.GoogleCloudBaseOperator",
    }

    MISSING_EXAMPLES_FOR_CLASSES = {
        "airflow.providers.google.cloud.operators.mlengine.MLEngineTrainingCancelJobOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator",
        "airflow.providers.google.cloud.transfers.cassandra_to_gcs.CassandraToGCSOperator",
        "airflow.providers.google.cloud.transfers.adls_to_gcs.ADLSToGCSOperator",
        "airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.GetEndpointOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.auto_ml.AutoMLTrainingJobBaseOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.UpdateEndpointOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job."
        "GetBatchPredictionJobOperator",
    }

    ASSETS_NOT_REQUIRED = {
        "airflow.providers.google.cloud.operators.automl.AutoMLDeleteDatasetOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLDeleteModelOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator",
        "airflow.providers.google.cloud.operators.bigquery_dts.BigQueryDeleteDataTransferConfigOperator",
        "airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator",
        "airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator",
        "airflow.providers.google.cloud.operators.cloud_build.CloudBuildDeleteBuildTriggerOperator",
        "airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreDeleteInstanceOperator",
        "airflow.providers.google.cloud.operators.cloud_memorystore."
        "CloudMemorystoreMemcachedDeleteInstanceOperator",
        "airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator",
        "airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceDatabaseOperator",
        "airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceOperator",
        "airflow.providers.google.cloud.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceDeleteJobOperator",
        "airflow.providers.google.cloud.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceGetOperationOperator",
        "airflow.providers.google.cloud.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceListOperationsOperator",
        "airflow.providers.google.cloud.operators.cloud_storage_transfer_service."
        "CloudDataTransferServicePauseOperationOperator",
        "airflow.providers.google.cloud.operators.cloud_storage_transfer_service."
        "CloudDataTransferServiceResumeOperationOperator",
        "airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator",
        "airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeleteInstanceOperator",
        "airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeletePipelineOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocDeleteBatchOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator",
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreDeleteBackupOperator",
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreDeleteServiceOperator",
        "airflow.providers.google.cloud.operators.datastore.CloudDatastoreBeginTransactionOperator",
        "airflow.providers.google.cloud.operators.datastore.CloudDatastoreDeleteOperationOperator",
        "airflow.providers.google.cloud.operators.datastore.CloudDatastoreGetOperationOperator",
        "airflow.providers.google.cloud.operators.datastore.CloudDatastoreRollbackOperator",
        "airflow.providers.google.cloud.operators.datastore.CloudDatastoreRunQueryOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPDeidentifyContentOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteInspectTemplateOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteJobTriggerOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPInspectContentOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator",
        "airflow.providers.google.cloud.operators.dlp.CloudDLPReidentifyContentOperator",
        "airflow.providers.google.cloud.operators.functions.CloudFunctionDeleteFunctionOperator",
        "airflow.providers.google.cloud.operators.gcs.GCSDeleteBucketOperator",
        "airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator",
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteModelOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteVersionOperator",
        "airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator",
        "airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator",
        "airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator",
        "airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator",
        "airflow.providers.google.cloud.operators.stackdriver.StackdriverDeleteAlertOperator",
        "airflow.providers.google.cloud.operators.stackdriver.StackdriverDeleteNotificationChannelOperator",
        "airflow.providers.google.cloud.operators.tasks.CloudTasksQueueDeleteOperator",
        "airflow.providers.google.cloud.operators.tasks.CloudTasksTaskDeleteOperator",
        "airflow.providers.google.cloud.operators.translate.CloudTranslateTextOperator",
        "airflow.providers.google.cloud.operators.translate_speech.CloudTranslateSpeechOperator",
        "airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator",
        "airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator",
        "airflow.providers.google.cloud.operators.vision.CloudVisionDeleteReferenceImageOperator",
        "airflow.providers.google.cloud.operators.workflows.WorkflowsDeleteWorkflowOperator",
        "airflow.providers.google.marketing_platform.sensors.campaign_manager."
        "GoogleCampaignManagerReportSensor",
        "airflow.providers.google.marketing_platform.sensors.display_video."
        "GoogleDisplayVideo360GetSDFDownloadOperationSensor",
        "airflow.providers.google.marketing_platform.sensors.display_video."
        "GoogleDisplayVideo360ReportSensor",
        "airflow.providers.google.marketing_platform.sensors.search_ads.GoogleSearchAdsReportSensor",
    }

    @pytest.mark.xfail(reason="We did not reach full coverage yet")
    def test_missing_assets(self):
        super().test_missing_assets()


class TestAmazonProviderProjectStructure(ExampleCoverageTest):
    PROVIDER = "amazon"
    CLASS_DIRS = ProjectStructureTest.CLASS_DIRS

    BASE_CLASSES = {
        "airflow.providers.amazon.aws.operators.base_aws.AwsBaseOperator",
        "airflow.providers.amazon.aws.operators.rds.RdsBaseOperator",
        "airflow.providers.amazon.aws.operators.sagemaker.SageMakerBaseOperator",
        "airflow.providers.amazon.aws.sensors.base_aws.AwsBaseSensor",
        "airflow.providers.amazon.aws.sensors.dms.DmsTaskBaseSensor",
        "airflow.providers.amazon.aws.sensors.emr.EmrBaseSensor",
        "airflow.providers.amazon.aws.sensors.rds.RdsBaseSensor",
        "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerBaseSensor",
        "airflow.providers.amazon.aws.operators.appflow.AppflowBaseOperator",
        "airflow.providers.amazon.aws.operators.ecs.EcsBaseOperator",
        "airflow.providers.amazon.aws.sensors.ecs.EcsBaseSensor",
        "airflow.providers.amazon.aws.sensors.eks.EksBaseSensor",
        "airflow.providers.amazon.aws.transfers.base.AwsToAwsBaseOperator",
    }

    MISSING_EXAMPLES_FOR_CLASSES = {
        # S3 Exasol transfer difficult to test, see: https://github.com/apache/airflow/issues/22632
        "airflow.providers.amazon.aws.transfers.exasol_to_s3.ExasolToS3Operator",
        # Glue Catalog sensor difficult to test
        "airflow.providers.amazon.aws.sensors.glue_catalog_partition.GlueCatalogPartitionSensor",
    }

    DEPRECATED_CLASSES = {
        "airflow.providers.amazon.aws.operators.lambda_function.AwsLambdaInvokeFunctionOperator",
    }


class TestElasticsearchProviderProjectStructure(ExampleCoverageTest):
    PROVIDER = "elasticsearch"
    CLASS_DIRS = {"hooks"}
    CLASS_SUFFIXES = ["Hook"]
    DEPRECATED_CLASSES = {
        "airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchHook",
    }


class TestCncfProviderProjectStructure(ExampleCoverageTest):
    PROVIDER = "cncf"
    CLASS_DIRS = ProjectStructureTest.CLASS_DIRS
    DEPRECATED_CLASSES = {
        "airflow.providers.cncf.kubernetes.operators.kubernetes_pod",
        "airflow.providers.cncf.kubernetes.triggers.kubernetes_pod",
    }
    BASE_CLASSES = {"airflow.providers.cncf.kubernetes.operators.resource.KubernetesResourceBaseOperator"}


class TestSlackProviderProjectStructure(ExampleCoverageTest):
    PROVIDER = "slack"
    CLASS_DIRS = ProjectStructureTest.CLASS_DIRS
    BASE_CLASSES = {
        "airflow.providers.slack.transfers.base_sql_to_slack.BaseSqlToSlackOperator",
        "airflow.providers.slack.operators.slack.SlackAPIOperator",
    }
    MISSING_EXAMPLES_FOR_CLASSES = set()
    DEPRECATED_CLASSES = {
        "airflow.providers.slack.notifications.slack_notifier.py.",
        "airflow.providers.slack.transfers.sql_to_slack.SqlToSlackOperator",
    }


class TestDockerProviderProjectStructure(ExampleCoverageTest):
    PROVIDER = "docker"


class TestOperatorsHooks:
    def test_no_illegal_suffixes(self):
        illegal_suffixes = ["_operator.py", "_hook.py", "_sensor.py"]
        files = itertools.chain.from_iterable(
            glob.glob(f"{ROOT_FOLDER}/{part}/providers/**/{resource_type}/*.py", recursive=True)
            for resource_type in ["operators", "hooks", "sensors", "example_dags"]
            for part in ["airflow", "tests"]
        )

        invalid_files = [f for f in files if f.endswith(tuple(illegal_suffixes))]

        assert [] == invalid_files
