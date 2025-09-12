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
import pathlib

import pytest

from tests_common.test_utils.paths import (
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_PROVIDERS_ROOT_PATH,
    AIRFLOW_ROOT_PATH,
)


class TestProjectStructure:
    def test_reference_to_providers_from_core(self):
        for filename in AIRFLOW_CORE_SOURCES_PATH.glob("example_dags/**/*.py"):
            self.assert_file_not_contains(filename, "providers")

    def test_deprecated_packages(self):
        for filename in AIRFLOW_CORE_SOURCES_PATH.glob("airflow/contrib/**/*.py"):
            if filename.name == "__init__.py":
                self.assert_file_contains(filename, "This package is deprecated.")
            else:
                self.assert_file_contains(filename, "This module is deprecated.")

    def assert_file_not_contains(self, filename: pathlib.Path, pattern: str):
        with open(filename, "rb", 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, "utf-8")) != -1:
                pytest.fail(f"File {filename} not contains pattern - {pattern}")

    def assert_file_contains(self, filename: pathlib.Path, pattern: str):
        with open(filename, "rb", 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, "utf-8")) == -1:
                pytest.fail(f"File {filename} contains illegal pattern - {pattern}")

    def test_providers_modules_should_have_tests(self):
        """
        Assert every module in /providers/ has a corresponding test_ file in providers/providers.
        """
        # The test below had a but for quite a while and we missed a lot of modules to have tess
        # We should make sure that one goes to 0
        # TODO(potiuk) - check if that test actually tests something
        OVERLOOKED_TESTS = [
            "providers/amazon/tests/unit/amazon/aws/auth_manager/datamodels/test_login.py",
            "providers/amazon/tests/unit/amazon/aws/auth_manager/security_manager/test_aws_security_manager_override.py",
            "providers/amazon/tests/unit/amazon/aws/executors/batch/test_batch_executor_config.py",
            "providers/amazon/tests/unit/amazon/aws/executors/batch/test_boto_schema.py",
            "providers/amazon/tests/unit/amazon/aws/executors/ecs/test_ecs_executor_config.py",
            "providers/amazon/tests/unit/amazon/aws/executors/ecs/test_utils.py",
            "providers/amazon/tests/unit/amazon/aws/executors/aws_lambda/test_utils.py",
            "providers/amazon/tests/unit/amazon/aws/executors/aws_lambda/docker/test_app.py",
            "providers/amazon/tests/unit/amazon/aws/executors/utils/test_base_config_keys.py",
            "providers/amazon/tests/unit/amazon/aws/operators/test_emr.py",
            "providers/amazon/tests/unit/amazon/aws/operators/test_sagemaker.py",
            "providers/amazon/tests/unit/amazon/aws/sensors/test_emr.py",
            "providers/amazon/tests/unit/amazon/aws/sensors/test_sagemaker.py",
            "providers/amazon/tests/unit/amazon/aws/test_exceptions.py",
            "providers/amazon/tests/unit/amazon/aws/triggers/test_sagemaker_unified_studio.py",
            "providers/amazon/tests/unit/amazon/aws/triggers/test_step_function.py",
            "providers/amazon/tests/unit/amazon/aws/utils/test_rds.py",
            "providers/amazon/tests/unit/amazon/aws/utils/test_sagemaker.py",
            "providers/amazon/tests/unit/amazon/aws/waiters/test_base_waiter.py",
            "providers/apache/hdfs/tests/unit/apache/hdfs/hooks/test_hdfs.py",
            "providers/apache/hdfs/tests/unit/apache/hdfs/log/test_hdfs_task_handler.py",
            "providers/apache/hdfs/tests/unit/apache/hdfs/sensors/test_hdfs.py",
            "providers/apache/hive/tests/unit/apache/hive/plugins/test_hive.py",
            "providers/celery/tests/unit/celery/executors/test_celery_executor_utils.py",
            "providers/celery/tests/unit/celery/executors/test_default_celery.py",
            "providers/cloudant/tests/unit/cloudant/test_cloudant_fake.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/executors/test_kubernetes_executor_types.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/executors/test_kubernetes_executor_utils.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/operators/test_kubernetes_pod.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/test_exceptions.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/test_k8s_model.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/test_kube_client.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/test_kube_config.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/test_python_kubernetes_script.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/test_secret.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/triggers/test_kubernetes_pod.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/utils/test_delete_from.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/utils/test_k8s_hashlib_wrapper.py",
            "providers/cncf/kubernetes/tests/unit/cncf/kubernetes/utils/test_xcom_sidecar.py",
            "providers/common/compat/tests/unit/common/compat/lineage/test_entities.py",
            "providers/common/compat/tests/unit/common/compat/standard/test_operators.py",
            "providers/common/compat/tests/unit/common/compat/standard/test_triggers.py",
            "providers/common/compat/tests/unit/common/compat/standard/test_utils.py",
            "providers/common/messaging/tests/unit/common/messaging/providers/test_base_provider.py",
            "providers/common/messaging/tests/unit/common/messaging/providers/test_sqs.py",
            "providers/edge3/tests/unit/edge3/models/test_edge_job.py",
            "providers/edge3/tests/unit/edge3/models/test_edge_logs.py",
            "providers/edge3/tests/unit/edge3/models/test_edge_worker.py",
            "providers/edge3/tests/unit/edge3/worker_api/routes/test__v2_compat.py",
            "providers/edge3/tests/unit/edge3/worker_api/routes/test__v2_routes.py",
            "providers/edge3/tests/unit/edge3/worker_api/routes/test_jobs.py",
            "providers/edge3/tests/unit/edge3/worker_api/test_app.py",
            "providers/edge3/tests/unit/edge3/worker_api/test_auth.py",
            "providers/edge3/tests/unit/edge3/worker_api/test_datamodels.py",
            "providers/edge3/tests/unit/edge3/worker_api/test_datamodels_ui.py",
            "providers/fab/tests/unit/fab/auth_manager/api_fastapi/datamodels/test_login.py",
            "providers/fab/tests/unit/fab/migrations/test_env.py",
            "providers/fab/tests/unit/fab/www/api_connexion/test_exceptions.py",
            "providers/fab/tests/unit/fab/www/api_connexion/test_parameters.py",
            "providers/fab/tests/unit/fab/www/api_connexion/test_security.py",
            "providers/fab/tests/unit/fab/www/api_connexion/test_types.py",
            "providers/fab/tests/unit/fab/www/extensions/test_init_appbuilder.py",
            "providers/fab/tests/unit/fab/www/extensions/test_init_jinja_globals.py",
            "providers/fab/tests/unit/fab/www/extensions/test_init_manifest_files.py",
            "providers/fab/tests/unit/fab/www/extensions/test_init_security.py",
            "providers/fab/tests/unit/fab/www/extensions/test_init_session.py",
            "providers/fab/tests/unit/fab/www/extensions/test_init_views.py",
            "providers/fab/tests/unit/fab/www/extensions/test_init_wsgi_middlewares.py",
            "providers/fab/tests/unit/fab/www/security/test_permissions.py",
            "providers/fab/tests/unit/fab/www/test_airflow_flask_app.py",
            "providers/fab/tests/unit/fab/www/test_app.py",
            "providers/fab/tests/unit/fab/www/test_constants.py",
            "providers/fab/tests/unit/fab/www/test_security_appless.py",
            "providers/fab/tests/unit/fab/www/test_security_manager.py",
            "providers/fab/tests/unit/fab/www/test_session.py",
            "providers/fab/tests/unit/fab/www/test_utils.py",
            "providers/fab/tests/unit/fab/www/test_views.py",
            "providers/google/tests/unit/google/cloud/fs/test_gcs.py",
            "providers/google/tests/unit/google/cloud/links/test_automl.py",
            "providers/google/tests/unit/google/cloud/links/test_base.py",
            "providers/google/tests/unit/google/cloud/links/test_bigquery.py",
            "providers/google/tests/unit/google/cloud/links/test_bigquery_dts.py",
            "providers/google/tests/unit/google/cloud/links/test_bigtable.py",
            "providers/google/tests/unit/google/cloud/links/test_cloud_build.py",
            "providers/google/tests/unit/google/cloud/links/test_cloud_functions.py",
            "providers/google/tests/unit/google/cloud/links/test_cloud_memorystore.py",
            "providers/google/tests/unit/google/cloud/links/test_cloud_sql.py",
            "providers/google/tests/unit/google/cloud/links/test_cloud_storage_transfer.py",
            "providers/google/tests/unit/google/cloud/links/test_cloud_tasks.py",
            "providers/google/tests/unit/google/cloud/links/test_compute.py",
            "providers/google/tests/unit/google/cloud/links/test_data_loss_prevention.py",
            "providers/google/tests/unit/google/cloud/links/test_datacatalog.py",
            "providers/google/tests/unit/google/cloud/links/test_dataflow.py",
            "providers/google/tests/unit/google/cloud/links/test_dataform.py",
            "providers/google/tests/unit/google/cloud/links/test_datafusion.py",
            "providers/google/tests/unit/google/cloud/links/test_dataprep.py",
            "providers/google/tests/unit/google/cloud/links/test_dataproc.py",
            "providers/google/tests/unit/google/cloud/links/test_datastore.py",
            "providers/google/tests/unit/google/cloud/links/test_kubernetes_engine.py",
            "providers/google/tests/unit/google/cloud/links/test_mlengine.py",
            "providers/google/tests/unit/google/cloud/links/test_pubsub.py",
            "providers/google/tests/unit/google/cloud/links/test_spanner.py",
            "providers/google/tests/unit/google/cloud/links/test_stackdriver.py",
            "providers/google/tests/unit/google/cloud/links/test_vertex_ai.py",
            "providers/google/tests/unit/google/cloud/links/test_workflows.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_auto_ml.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_batch_prediction_job.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_custom_job.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_dataset.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_endpoint_service.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_hyperparameter_tuning_job.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_model_service.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_pipeline_job.py",
            "providers/google/tests/unit/google/cloud/operators/vertex_ai/test_ray.py",
            "providers/google/tests/unit/google/cloud/sensors/vertex_ai/test_feature_store.py",
            "providers/google/tests/unit/google/cloud/transfers/test_bigquery_to_sql.py",
            "providers/google/tests/unit/google/cloud/transfers/test_presto_to_gcs.py",
            "providers/google/tests/unit/google/cloud/utils/test_bigquery.py",
            "providers/google/tests/unit/google/cloud/utils/test_bigquery_get_data.py",
            "providers/google/tests/unit/google/cloud/utils/test_dataform.py",
            "providers/google/tests/unit/google/common/links/test_storage.py",
            "providers/google/tests/unit/google/common/test_consts.py",
            "providers/google/tests/unit/google/common/hooks/test_operation_helpers.py",
            "providers/google/tests/unit/google/test_go_module_utils.py",
            "providers/http/tests/unit/http/test_exceptions.py",
            "providers/keycloak/tests/unit/keycloak/auth_manager/datamodels/test_token.py",
            "providers/microsoft/azure/tests/unit/microsoft/azure/operators/test_adls.py",
            "providers/snowflake/tests/unit/snowflake/triggers/test_snowflake_trigger.py",
            "providers/standard/tests/unit/standard/operators/test_branch.py",
            "providers/standard/tests/unit/standard/operators/test_empty.py",
            "providers/standard/tests/unit/standard/operators/test_latest_only.py",
            "providers/standard/tests/unit/standard/operators/test_trigger_dagrun.py",
            "providers/standard/tests/unit/standard/sensors/test_external_task.py",
            "providers/standard/tests/unit/standard/sensors/test_filesystem.py",
            "providers/standard/tests/unit/standard/utils/test_sensor_helper.py",
            "providers/sftp/tests/unit/sftp/test_exceptions.py",
        ]
        modules_files: list[pathlib.Path] = list(
            AIRFLOW_PROVIDERS_ROOT_PATH.glob("**/src/airflow/providers/**/*.py")
        )
        # Exclude .build files
        modules_files = (f for f in modules_files if ".build" not in f.parts)
        # Exclude .git files
        modules_files = (f for f in modules_files if ".git" not in f.parts)
        # Exclude .venv files
        modules_files = (f for f in modules_files if ".venv" not in f.parts)
        # Exclude node_modules
        modules_files = (f for f in modules_files if "node_modules" not in f.parts)
        # Exclude __init__.py
        modules_files = filter(lambda f: f.name != "__init__.py", modules_files)
        # Exclude example_dags
        modules_files = (f for f in modules_files if "example_dags" not in f.parts)
        # Exclude _vendor
        modules_files = (f for f in modules_files if "_vendor" not in f.parts)
        # Exclude versions file
        modules_files = (f for f in modules_files if "versions" not in f.parts)
        # Exclude get_provider_info files
        modules_files = (f for f in modules_files if "get_provider_info.py" not in f.parts)
        # Make path relative
        modules_files = list(f.relative_to(AIRFLOW_ROOT_PATH) for f in modules_files)
        current_test_files = list(AIRFLOW_PROVIDERS_ROOT_PATH.rglob("**/tests/**/*.py"))
        # Make path relative
        current_test_files = list(f.relative_to(AIRFLOW_ROOT_PATH) for f in current_test_files)
        # Exclude __init__.py
        current_test_files = set(f for f in current_test_files if not f.name == "__init__.py")
        # Exclude node_modules
        current_test_files = set(f for f in current_test_files if "node_modules" not in f.parts)
        # Exclude version_compat.py
        modules_files = filter(lambda f: f.name != "version_compat.py", modules_files)

        modules_files_set = set(modules_files)
        expected_test_files = set(
            [
                pathlib.Path(
                    f.with_name("test_" + f.name)
                    .as_posix()
                    .replace("/src/airflow/providers/", "/tests/unit/")
                )
                for f in modules_files_set
            ]
        )
        expected_test_files = set(expected_test_files) - set(
            [pathlib.Path(test_file) for test_file in OVERLOOKED_TESTS]
        )

        missing_tests_files = [
            file.as_posix()
            for file in sorted(expected_test_files - expected_test_files.intersection(current_test_files))
        ]

        assert missing_tests_files == [], "Detect missing tests in providers module - please add tests"

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


def filepath_to_module(path: pathlib.Path, src_folder: pathlib.Path):
    path = path.relative_to(src_folder)
    return path.as_posix().replace("/", ".")[: -(len(".py"))]


def print_sorted(container: set, indent: str = "    ") -> None:
    sorted_container = sorted(container)
    print(f"{indent}" + f"\n{indent}".join(sorted_container))


class ProjectStructureTest:
    PROVIDER = "blank"
    CLASS_DIRS = {"operators", "sensors", "transfers"}
    CLASS_SUFFIXES = ["Operator", "Sensor"]

    def new_class_paths(self):
        for resource_type in self.CLASS_DIRS:
            python_files = AIRFLOW_PROVIDERS_ROOT_PATH.glob(
                f"{self.PROVIDER}/**/{resource_type}/**/*.py",
            )
            # Make path relative
            resource_files = filter(lambda f: f.name != "__init__.py", python_files)
            yield from resource_files

    def list_of_classes(self):
        classes = {}
        for file in self.new_class_paths():
            operators_paths = self.get_classes_from_file(file, AIRFLOW_PROVIDERS_ROOT_PATH)
            classes.update(operators_paths)
        return classes

    def get_classes_from_file(self, filepath: pathlib.Path, src_folder: pathlib.Path):
        with open(filepath) as py_file:
            content = py_file.read()
        doc_node = ast.parse(content, filepath)
        module = filepath_to_module(filepath, src_folder)
        results: dict = {}
        for current_node in ast.walk(doc_node):
            if isinstance(current_node, ast.ClassDef) and current_node.name.endswith(
                tuple(self.CLASS_SUFFIXES)
            ):
                if "unit" in module:
                    continue
                if "integration" in module:
                    continue
                if "system" in module:
                    continue
                module_path = module[module.find("airflow.providers") :]
                results[f"{module_path}.{current_node.name}"] = current_node
        print(f"{results}")
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
        yield from glob.glob(
            f"{AIRFLOW_ROOT_PATH}/providers/{self.PROVIDER}/tests/system/{self.PROVIDER}/**/example_*.py",
            recursive=True,
        )

        yield from glob.glob(
            f"{AIRFLOW_ROOT_PATH}/providers/{self.PROVIDER}/src/airflow/providers/{self.PROVIDER}/**/example_*.py",
            recursive=True,
        )

    def test_missing_examples(self):
        """
        Assert that all operators defined under operators, sensors and transfers directories
        are used in any of the example dags
        """
        classes = self.list_of_classes()
        assert len(classes) != 0, "Failed to retrieve operators, override class_paths if needed"
        classes = set(classes.keys())
        for example in self.example_paths():
            classes -= get_imports_from_file(example)
        covered_but_omitted = self.MISSING_EXAMPLES_FOR_CLASSES - classes
        classes -= self.MISSING_EXAMPLES_FOR_CLASSES
        classes -= self.DEPRECATED_CLASSES
        classes -= self.BASE_CLASSES
        classes = set(class_name for class_name in classes if not class_name.startswith("Test"))
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
        "airflow.providers.google.cloud.operators.automl.AutoMLTablesListColumnSpecsOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLTablesListTableSpecsOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLTablesUpdateDatasetOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLDeployModelOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLTrainModelOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLPredictOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLCreateDatasetOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLImportDataOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLGetModelOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLDeleteModelOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLListDatasetOperator",
        "airflow.providers.google.cloud.operators.automl.AutoMLDeleteDatasetOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator",
        "airflow.providers.google.cloud.operators.bigquery.BigQueryCreateExternalTableOperator",
        "airflow.providers.google.cloud.operators.datapipeline.CreateDataPipelineOperator",
        "airflow.providers.google.cloud.operators.datapipeline.RunDataPipelineOperator",
        "airflow.providers.google.cloud.operators.mlengine.MLEngineCreateModelOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.generative_model.TextGenerationModelPredictOperator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360CreateQueryOperator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360DeleteReportOperator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360RunQueryOperator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360DownloadReportV2Operator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360UploadLineItemsOperator",
        "airflow.providers.google.marketing_platform.operators.GoogleDisplayVideo360DownloadLineItemsOperator",
        "airflow.providers.google.marketing_platform.sensors.GoogleDisplayVideo360RunQuerySensor",
        "airflow.providers.google.cloud.hooks.datacatalog.CloudDataCatalogHook",
        "airflow.providers.google.cloud.links.datacatalog.DataCatalogEntryGroupLink",
        "airflow.providers.google.cloud.links.datacatalog.DataCatalogEntryLink",
        "airflow.providers.google.cloud.links.datacatalog.DataCatalogTagTemplateLink",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetTagTemplateOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogRenameTagTemplateFieldOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateEntryOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateFieldOperator",
    }

    BASE_CLASSES = {
        "airflow.providers.google.cloud.operators.alloy_db.AlloyDBBaseOperator",
        "airflow.providers.google.cloud.operators.alloy_db.AlloyDBWriteBaseOperator",
        "airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator",
        "airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryToSqlBaseOperator",
        "airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator",
        "airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator",
        "airflow.providers.google.cloud.operators.dataproc._DataprocStartStopClusterBaseOperator",
        "airflow.providers.google.cloud.operators.dataplex.DataplexCatalogBaseOperator",
        "airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaBaseOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.custom_job.CustomTrainingJobBaseOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.ray.RayBaseOperator",
        "airflow.providers.google.cloud.operators.cloud_base.GoogleCloudBaseOperator",
        "airflow.providers.google.marketing_platform.operators.search_ads._GoogleSearchAdsBaseOperator",
    }

    MISSING_EXAMPLES_FOR_CLASSES = {
        "airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator",
        "airflow.providers.google.cloud.transfers.cassandra_to_gcs.CassandraToGCSOperator",
        "airflow.providers.google.cloud.transfers.adls_to_gcs.ADLSToGCSOperator",
        "airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.GetEndpointOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.auto_ml.AutoMLTrainingJobBaseOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.UpdateEndpointOperator",
        "airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.GetBatchPredictionJobOperator",
        "airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator",
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360CreateQueryOperator",
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360RunQueryOperator",
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DeleteReportOperator",
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DownloadReportV2Operator",
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DownloadLineItemsOperator",
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360UploadLineItemsOperator",
        "airflow.providers.google.marketing_platform.sensors.display_video.GoogleDisplayVideo360RunQuerySensor",
        "airflow.providers.google.cloud.operators.vertex_ai.generative_model.DeleteExperimentRunOperator",
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
        "airflow.providers.google.marketing_platform.sensors.display_video.GoogleDisplayVideo360ReportSensor",
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
        "airflow.providers.amazon.aws.sensors.bedrock.BedrockBaseSensor",
        "airflow.providers.amazon.aws.sensors.dms.DmsTaskBaseSensor",
        "airflow.providers.amazon.aws.sensors.emr.EmrBaseSensor",
        "airflow.providers.amazon.aws.sensors.rds.RdsBaseSensor",
        "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerBaseSensor",
        "airflow.providers.amazon.aws.operators.appflow.AppflowBaseOperator",
        "airflow.providers.amazon.aws.operators.ecs.EcsBaseOperator",
        "airflow.providers.amazon.aws.sensors.ecs.EcsBaseSensor",
        "airflow.providers.amazon.aws.sensors.eks.EksBaseSensor",
        "airflow.providers.amazon.aws.transfers.base.AwsToAwsBaseOperator",
        "airflow.providers.amazon.aws.operators.comprehend.ComprehendBaseOperator",
        "airflow.providers.amazon.aws.sensors.comprehend.ComprehendBaseSensor",
        "airflow.providers.amazon.aws.sensors.kinesis_analytics.KinesisAnalyticsV2BaseSensor",
    }

    MISSING_EXAMPLES_FOR_CLASSES = {
        # S3 Exasol transfer difficult to test, see: https://github.com/apache/airflow/issues/22632
        "airflow.providers.amazon.aws.transfers.exasol_to_s3.ExasolToS3Operator",
        # These operations take a lot of time, there are commented out in the system tests for this reason
        "airflow.providers.amazon.aws.operators.dms.DmsStartReplicationOperator",
        "airflow.providers.amazon.aws.operators.dms.DmsStopReplicationOperator",
        # These modules are used in the SageMakerNotebookOperator and therefore don't have their own examples
        "airflow.providers.amazon.aws.sensors.sagemaker_unified_studio.SageMakerNotebookSensor",
    }

    DEPRECATED_CLASSES = {
        "airflow.providers.amazon.aws.operators.lambda_function.AwsLambdaInvokeFunctionOperator",
    }


class TestElasticsearchProviderProjectStructure(ExampleCoverageTest):
    PROVIDER = "elasticsearch"
    CLASS_DIRS = {"hooks"}
    CLASS_SUFFIXES = ["Hook"]


class TestCncfProviderProjectStructure(ExampleCoverageTest):
    PROVIDER = "cncf/kubernetes"
    CLASS_DIRS = ProjectStructureTest.CLASS_DIRS
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
            glob.glob(f"{AIRFLOW_ROOT_PATH}/{part}/providers/**/{resource_type}/*.py", recursive=True)
            for resource_type in ["operators", "hooks", "sensors", "example_dags"]
            for part in ["airflow", "tests"]
        )

        invalid_files = [f for f in files if f.endswith(tuple(illegal_suffixes))]

        assert invalid_files == []
