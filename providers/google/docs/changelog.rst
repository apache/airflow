 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.


``apache-airflow-providers-google``


Changelog
---------

.. warning::
  Deprecated classes, parameters and features have been removed from the Google provider package.
  The following breaking changes were introduced:

* Operators

  * ``Remove AutoMLTrainModelOperator use airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLTabularTrainingJobOperator, airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLVideoTrainingJobOperator, airflow.providers.google.cloud.operators.vertex_ai.auto_ml.CreateAutoMLImageTrainingJobOperator, airflow.providers.google.cloud.operators.vertex_ai.generative_model.SupervisedFineTuningTrainOperator, airflow.providers.google.cloud.operators.translate.TranslateCreateModelOperator instead``
  * ``Remove AutoMLPredictOperator use airflow.providers.google.cloud.operators.translate.TranslateTextOperator instead``
  * ``Remove AutoMLCreateDatasetOperator use airflow.providers.google.cloud.operators.vertex_ai.dataset.CreateDatasetOperator, airflow.providers.google.cloud.operators.translate.TranslateCreateDatasetOperator instead``
  * ``Remove AutoMLImportDataOperator use airflow.providers.google.cloud.operators.vertex_ai.dataset.ImportDataOperator, airflow.providers.google.cloud.operators.translate.TranslateImportDataOperator instead``
  * ``Remove AutoMLTablesListColumnSpecsOperator because of the shutdown of legacy version of AutoML Tables``
  * ``Remove AutoMLTablesUpdateDatasetOperator use airflow.providers.google.cloud.operators.vertex_ai.dataset.UpdateDatasetOperator instead``
  * ``Remove AutoMLGetModelOperator use airflow.providers.google.cloud.operators.vertex_ai.model_service.GetModelOperator instead``
  * ``Remove AutoMLDeleteModelOperator use airflow.providers.google.cloud.operators.vertex_ai.model_service.DeleteModelOperator, airflow.providers.google.cloud.operators.translate.TranslateDeleteModelOperator instead``
  * ``Remove AutoMLDeployModelOperator use airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.DeployModelOperator instead``
  * ``Remove AutoMLTablesListTableSpecsOperator because of the shutdown of legacy version of AutoML Tables``
  * ``Remove AutoMLListDatasetOperator use airflow.providers.google.cloud.operators.vertex_ai.dataset.ListDatasetsOperator, airflow.providers.google.cloud.operators.translate.TranslateDatasetsListOperator instead``
  * ``Remove AutoMLDeleteDatasetOperator use airflow.providers.google.cloud.operators.vertex_ai.dataset.ListDatasetsOperator, airflow.providers.google.cloud.operators.translate.TranslateDatasetsListOperator instead``
  * ``Remove MLEngineCreateModelOperator use appropriate VertexAI operator instead``

* Hooks

  * ``Remove CloudAutoMLHook use airflow.providers.google.cloud.hooks.vertex_ai.auto_ml.AutoMLHook, airflow.providers.google.cloud.hooks.translate.TranslateHook instead``

19.2.0
......

Features
~~~~~~~~

* ``feat(google): add enable_open_telemetry_tracing to PubSubHook and PubSubPublishMessageOperator (#58766)``

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix composer sys test (#59200)``
   * ``Remove global from openlineage provider (#58868)``

19.1.0
......

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Create CloudComposerExternalTaskSensor for Cloud Composer service (#57971)``
* ``Add gcp_conn_id as template field (#58298)``

Bug Fixes
~~~~~~~~~

* ``Fix generate_if for BQ if the run_type=scheduled (#58035)``
* ``Fix incorrect default docstring for gke_finish_action in GKEStartPodOperator (#58349)``
* ``Fix Google provider to handle False boolean values in connection extras (#58348)``
* ``Fix KuberetesPodTriggerer use correct parameter name to read pod logs. (#58489)``
* ``Fix poll_sleep is ignored in DataflowStartFlexTemplateOperator definition (#58846)``
* ``Fix race condition in test_execute_wildcard for LocalFilesystemToGCSOperator (#58613)``

Misc
~~~~

* ``Move out some exceptions to TaskSDK (#54505)``
* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``
* ``Remove SDK reference for NOTSET in Airflow Core (#58258)``
* ``Fix lower bound dependency to common-compat provider (#58833)``
* ``Fix mypy errors for RayHook (#58843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``
   * ``Update create_airflow_connection helper method for making able to create Connection in Composer environment (#57804)``
   * ``Ensure blank line before bullet lists (#58760)``
   * ``Prepare release for 2025-11-27 wave of providers (#58697)``

19.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

* ``Remove deprecated in google provider scheduled for October 2025 (#56935)``

Features
~~~~~~~~

* ``issue-58236: Adding gcp_conn_id to template_fields for BigQuery and Dataproc Operators (#58250)``

Bug Fixes
~~~~~~~~~

* ``fix: add required arguments when creating an external table (#58199)``
* ``Fix MSSQLToGCSOperator MSSQL BIT data type conversion to Parquet boolean (#57514)``
* ``Small fix for feature_store system test + add output for operator (#57605)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Migrate google provider to 'common.compat' (#57130)``
* ``Move subprocess utility closer to usage in google beam (#57187)``

Doc-only
~~~~~~~~

* ``Remove tasks which use DirectRunner from Dataflow system tests (#57803)``
* ``[Doc] Fixing some typos and spelling errors (#57225)``
* ``Add google provider dataflow pipeline streaming system test (#56265)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Enable PT006 rule to google Provider test (triggers) (#57953)``
   * ``Enable PT006 rule to google Provider test (triggers) (#57950)``
   * ``Enable PT006 rule to google Provider test (log,openlineage,common,utils) (#57947)``
   * ``Enable PT006 rule to google Provider test (operators part2)  (#57944)``
   * ``KubernetesPodTriggerer reads pod logs instead of KubernetesPodOperator (#57531)``
   * ``modify test_variables (#57945)``
   * ``Enable PT006 rule to google Provider test (operators part1) (#57943)``
   * ``Enable PT006 rule to google Provider test （hooks） (#57915)``
   * ``Add gcp_conn_id parameter to DataprocDiagnoseClusterOperator examples (#57904)``
   * ``Replace dag.log.info with log.info in system tests in google provider (#57550)``
   * ``Change the runtime for Google Cloud Function system test (#57623)``
   * ``Enable ruff PLW1510 rule (#57660)``
   * ``Enable ruff PLW1508 rule (#57653)``
   * ``Fix code formatting via ruff preview (#57641)``
   * ``Enable ruff PLW0129 rule (#57516)``
   * ``Enable ruff PLW0120 rule (#57456)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``
   * ``Fixing some typos and spelling errors (#57186)``

18.1.0
......

Features
~~~~~~~~

* ``Deprecate old vertex ai generative operators and introduce new gen ai (#56950)``
* ``Add support of deferable mode for CloudDataTransferServiceRunJobOperator (#56711)``
* ``Add fail_on_file_not_exist to SFTPToGCSOperator (#56528)``
* ``Add GCS Dag bundle (unversioned) (#55919)``
* ``Add use_rest_api parameter for CloudComposerDAGRunSensor for pulling dag_runs using the Airflow REST API (#56138)``
* ``feature: Add Open Lineage support for CloudDataFusionStartPipelineOperator (#56365)``
* ``feature: Add OpenLineage support for SpannerQueryDatabaseInstanceOperator (#56348)``
* ``feature: Add OpenLineage support for CloudDataTransferServiceS3ToGCSOperator (#55946)``

Bug Fixes
~~~~~~~~~

* ``let PubsubPullTrigger exceptions propagate to triggerer framework (#56400)``
* ``Fix AsyncToSync and serialization error (#56363)``
* ``FIX: incorrect access of logical_date in google bigquery operator and google workflow operator (#55110)``
* ``Prefer BigQueryInsertJobOperator's project_id over hook's project_id for openlineage (#55948)``
* ``Fix ValueError in def operators in google provider (#55821)``

Misc
~~~~

* ``Common.Compat: Extract reusable compat utilities and rename to sdk (#56884)``
* ``Simplify version-specific imports in the Google provider (#56793)``
* ``Deprecate CreateAutoMLVideoTrainingJobOperator and removed system tests for video tracking and video training. Update generative_model_tuning system test. Update documentation for vertex ai. (#56282)``
* ``Refactor CloudSQLDatabaseHook.create_connection method to align with new Connection from airflow.sdk. (#56323)``
* ``adding 3 logging statements to display that a job is still waiting in the Dataproc wait_for_job method (#56523)``
* ``Support google ads v21 in google provider (#56012)``

Doc-only
~~~~~~~~

* ``refactor vertex_ai image training system test and docs (#56036)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix managed kafka system tests to use correct network name (#56654)``
   * ``Revert "AIP-82: implement Google Pub/Sub message queue provider (#54494)" (#56423)``
   * ``AIP-82: implement Google Pub/Sub message queue provider (#54494)``
   * ``Fix dataflow java system test + link (#56286)``
   * ``Enable PT011 rule to prvoider tests (#56237)``
   * ``Remove timeout parameter from the dataproc_metastore sys test (#56247)``
   * ``Update system tests in google provider. Add constraints to prevent from failing and modify example_cloud_logging_sink (#55939)``
   * ``Upgrade FAB to FAB 5 (#50960)``
   * ``Enable PT011 rule to provider tests (#56131)``
   * ``Enable PT011 rule to prvoider tests (#56087)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``
   * ``Add missing extra link definition to google provider (#56136)``

18.0.0
......


Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Deprecated classes, parameters and features have been removed from the Google provider package.
  The following breaking changes were introduced:

* Operators

  * ``Remove GoogleDisplayVideo360CreateQueryOperator use airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360CreateSDFDownloadTaskOperator instead``
  * ``Remove GoogleDisplayVideo360DeleteReportOperator. Reports were replaced with SDF export task in v4 of API``
  * ``Remove GoogleDisplayVideo360DownloadReportV2Operator use airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360SDFtoGCSOperator instead``
  * ``Remove GoogleDisplayVideo360RunQueryOperator use airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360CreateSDFDownloadTaskOperator instead``
  * ``Remove GoogleDisplayVideo360DownloadLineItemsOperator use airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360CreateSDFDownloadTaskOperator instead``
  * ``Remove GoogleDisplayVideo360UploadLineItemsOperator use airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360SDFtoGCSOperator instead``

* Hooks

  * ``Remove GoogleDisplayVideo360Hook.get_conn use airflow.providers.google.marketing_platform.hooks.display_video.get_conn_to_display_video instead``
  * ``Remove GoogleDisplayVideo360Hook.create_query use airflow.providers.google.marketing_platform.hooks.display_video.create_sdf_download_operation instead``
  * ``Remove GoogleDisplayVideo360Hook.delete_query``
  * ``Remove GoogleDisplayVideo360Hook.get_query use airflow.providers.google.marketing_platform.hooks.display_video.get_sdf_download_operation instead``
  * ``Remove GoogleDisplayVideo360Hook.list_queries``
  * ``Remove GoogleDisplayVideo360Hook.run_query use airflow.providers.google.marketing_platform.hooks.display_video.create_sdf_download_operation instead``
  * ``Remove GoogleDisplayVideo360Hook.get_report``
  * ``Remove GoogleDisplayVideo360Hook.upload_line_items use airflow.providers.google.marketing_platform.hooks.display_video.create_sdf_download_operation instead``
  * ``Remove GoogleDisplayVideo360Hook.download_line_items use airflow.providers.google.marketing_platform.hooks.display_video.download_media instead``
  * ``Remove AutoMLHook.create_auto_ml_text_training_job. AutoMLText API was deprecated.``

* Sensors

  * ``Remove GoogleDisplayVideo360RunQuerySensor. Reports were replaced with SDF export task in v4 of API.``


* ``Remove deprecated from google provider scheduled for September 2025 except 30 September 2025. (#55683)``




Features
~~~~~~~~

* ``Add OpenLineage support for BigQueryToPostgresOperator (#55392)``
* ``Add OpenLineage support for BigQueryToMySqlOperator (#55219)``
* ``Add range partitioning option to bigquery operators (#55247)``
* ``Add CloudComposerTriggerDAGRunOperator for Cloud Composer service (#55256)``

Bug Fixes
~~~~~~~~~

* ``[OSSTaskHandler, CloudwatchTaskHandler, S3TaskHandler, HdfsTaskHandler, ElasticsearchTaskHandler, GCSTaskHandler, OpensearchTaskHandler, RedisTaskHandler, WasbTaskHandler] supports log file size handling (#55455)``
* ``Add conversion to timedelta type for self.timeout value for CloudComposerDAGRunSensor (#55570)``
* ``Fix Cloud Run Async hooks (#55321)``
* ``Fix CloudComposerAsyncHook to work correctly with Airflow 3 (#54976)``
* ``Fix: Use GoogleBaseAsyncHook (#55316)``

Misc
~~~~

* ``Restrict google-ads 28.0.0.post2 (#55640)``
* ``Switch all airflow logging to structlog (#52651)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump mypy to 1.18.1 (#55596)``
   * ``Fix dataflow java streaming infinite run (#55209)``

17.2.0
......


Features
~~~~~~~~

* ``Added handler for cli command exit code in CloudComposerRunAirflowCLICommandOperator. (#54497)``
* ``Add 'create_intermediate_dirs' option to 'GCSToSFTPOperator' (#54916)``
* ``feature: Add OpenLineage support for PubSubPullOperator (#54821)``
* ``feature: Add OpenLineage support for pubsub create subscription operator - include inputs into OL (#54785)``
* ``feature: Add OpenLineage support for PubSubPublishMessageOperator (#54764)``
* ``feature: Add OpenLineage support for pubsub create subscription operator (#54707)``
* ``feature: Add OpenLineage support for OracleToGCSOperator (#54953)``
* ``feature: Add OpenLineage support for BigQueryToMsSqlOperator (#55168)``
* ``Add GSheetsHook API endpoint support (Part 1) (#54560)``
* ``Add cluster_tier flag support to Dataproc ClusterGenerator (#54105)``

Bug Fixes
~~~~~~~~~

* ``Fix ''BigQueryInsertJobOperator'' failure with asset-triggered DAGs (#55092)``
* ``Fix serialization in google provider (#55014)``
* ``Fix CloudComposerDAGRunSensor functionality (#54351)``
* ``Skip provider_session usage in AF3 for BigQuery and Dataproc triggers (#54405)``
* ``update logic of BQ to be able to use WRITE_TRUNCATE_DATA' (#54417)``
* ``Remove MappedOperator inheritance (#53696)``
* ``Fix mypy no-redef errors for timeout imports in providers (#54471)``
* ``Validate Dataproc batch labels max 63 characters (#55196)``

Misc
~~~~

* ``Add rudimentary support for psycopg3 (#52976)``
* ``Add get_secret function to the common/utils (#54213)``
* ``Update usage of timeout contextmanager from SDK where possible (#54183)``
* ``Add return row count to SpannerQueryDatabaseInstanceOperator (#55127)``
* ``Add composer_dag_run_id as optional parameter to CloudComposerDAGRunSensor (#54977)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``
* ``Fix Airflow 2 reference in README/index of providers (#55240)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unneeded isort skip comments (#54979)``
   * ``Improve google provider example_http_to_gcs system test (#54647)``
   * ``Move trigger_rule utils from 'airflow/utils'  to 'airflow.task'and integrate with Execution API spec (#53389)``
   * ``Replace API server's direct Connection access workaround in BaseHook (#54083)``
   * ``Switch pre-commit to prek (#54258)``
   * ``Remove unused test file (#54408)``
   * ``Add CI support for SQLAlchemy 2.0 (#52233)``
   * ``Add missing schedule parameter for the system tests of Google provider (#54270)``
   * ``Update Vertex AI dataset system test (#54220)``
   * ``Bump pytest-asyncio to 1.1.0 (#54123)``
   * ``Update google-provider system tests beam sdk to the latest version (#55210)``

17.1.0
......

Features
~~~~~~~~

* ``Add Private Service Connect interface support to VertexAI operators (#54170)``

Bug Fixes
~~~~~~~~~

* ``Fix the google provider vertex_ai ImportDataOperator (#53865)``
* ``Fix the google cloud provider RayHook protobuf compatibility (#54014)``
* ``Fix the PostgresHook ignoring custom adapters registered (#53707)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix location and timeout for Workflows and Dataproc Metastore system tests (#54167)``
   * ``Fix ray to be imported conditionally (tests is skipped on Python 3.13) (#54031)``

17.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Deprecated classes, parameters and features have been removed from the Google provider package.
  The following breaking changes were introduced:

* Operators

  * ``Remove operator LifeSciencesRunPipelineOperator use Google Cloud Batch operators instead``
  * ``Remove operator BigQueryCreateEmptyTableOperator use BigQueryCreateTableOperator instead``
  * ``Remove operator BigQueryCreateExternalTableOperator use BigQueryCreateTableOperator instead``

* Hooks

  * ``Remove LifeSciencesHook use Google Cloud Batch hooks instead``
  * ``Remove BigQueryHook.create_empty_table method use BigQueryHook.create_table instead``
  * ``Remove DataflowHook.start_sql_job method use DataflowHook.launch_beam_yaml_job instead``

* Links

  * ``Remove LifeSciencesLink as support of the Life Sciences API will be discontinued on July 8, 2025``

* ``Remove deprecated from google provider scheduled for June and July 2025 (#53197)``

Features
~~~~~~~~

* ``Handle multiple pods to prevent '''KubernetesJobOperator''' falls with parallelism option (#49899)``
* ``Adjust ray operators in google provider. Add ray package as a dependency. (#53182)``
* ``Add new operators for Experiment and Experiment run entities for Vertex AI (#52964)``
* ``feature: Added support for Google cloud log sink management via google provider (#52001)``

Bug Fixes
~~~~~~~~~

* ``fix: DataprocClusterTrigger by passing objects in correct types so that they can be serialized (#53485)``
* ``fix: Change DataprocAsyncHook parent class to GoogleBaseAsyncHook (#52981)``
* ``fix: GCSToBigQueryOperator: avoid pushing non-serializable task_instance to XCom (#53094)``
* ``fix: StackdriverHook channel and policy assignments when creating and updating notifications and policys (#53131)``
* ``fix: return DataflowJobMessagesSensor and DataflowJobAutoScalingEventsSensor result with xcom_value (#53115)``

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Refactor google.cloud imports to prevent conflicts with airflow google provider namespace and resolve mypy errors (#53338)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Clean up mypy ignores in google provider (#53249)``
* ``Use standard library ''typing'' imports for Python 3.10+ (#53158)``
* ``Make google provider compatible with mypy 1.16.1 (#53148)``
* ``Deprecate old DV360 operator and modify system test (#53027)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Remove 'MAX_XCOM_SIZE' hardcoded constant from Airflow core  (#52978)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Replace BaseHook to Task SDK for google (#52809)``

Doc-only
~~~~~~~~

* ``Remove extra slash from endpoint URL (#53755)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Deprecate decorators from Core (#53629)``
   * ``delete: async_get_operation in test_dataproc (#53531)``
   * ``add unit tests for google/common (#52965)``
   * ``Resolve OOM When Reading Large Logs in Webserver (#49470)``
   * ``Clean up in google provider (#53090)``
   * ``Make dag_version_id in TI non-nullable (#50825)``
   * ``Removed unnecessary shebangs from test files (#52992)``


16.1.0
......

Features
~~~~~~~~

* ``Add regional support for google secret manager hook (#52124)``

Bug Fixes
~~~~~~~~~

* ``Add Google Cloud VertexAI and Translate datasets import data verification (#51364)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Bump pyarrow to 16.1.0 minimum version for several providers (#52635)``
* ``Upgrade ruff to latest version (0.12.1) (#52562)``
* ``Replace 'models.BaseOperator' to Task SDK one for Google Provider (#52366)``
* ``Bump google-cloud-bigquery>=3.24.0 (#52337)``
* ``Drop support for Python 3.9 (#52072)``
* ``Replace 'models.BaseOperator' to Task SDK one for Standard Provider (#52292)``
* ``Attempt2: Fix mypy in gcp generative_model (#52331)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``
* ``Fix mypy errors in GCP 'generative_model' (#52321)``
* ``Exclude ray in google provider for Python 3.13 (#51994)``
* ``Change the Link implementation in Google Provider to make it cleand and compatible with Airflow 3 (#51576)``
* ``Bump min version of google-cloud-spanner to 3.50 (#51837)``
* ``Update Google Ads API version to v20 (#51683)``

Doc-only
~~~~~~~~

* ``docstring update for gcp dataplex operator and hook (#52387)``
* ``Add additional VertexAI Feature Store operators (#51958)``
* ``docs: update vertex AI generative model documentation (#51983)``
* ``Add new operator for Vertex AI (#51965)``
* ``Create DAG for showing how to enable Ray on GKE cluster (#51107)``
* ``Deprecate Data Catalog operators, hook, links because service will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. (#51647)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor the google cloud DataprocCreateBatchOperator tests (#52573)``
   * ``Make sure all test version imports come from test_common (#52425)``
   * ``Bring back providers compatibility checks (#52398)``
   * ``Fix xdist compatibility for test_local_to_gcs test (#52244)``
   * ``Separate out creation of default Connections for tests and non-tests (#52129)``
   * ``tests: remove pytest db markers from google provider (#52039)``
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``
   * ``Switch the Supervisor/task process from line-based to length-prefixed (#51699)``
   * ``Fix system tests for vertex ai generative operators (#51582)``

16.0.0
......

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

.. warning::
  Deprecated classes, parameters and features have been removed from the Google provider package.
  The following breaking changes were introduced:

* Operators

  * ``Remove operator TextGenerationModelPredictOperator use GenerativeModelGenerateContentOperator instead``
  * ``Remove name attribute from GKEDeleteClusterOperator operator please use cluster_name instead``
  * ``Remove regional attribute from GKEStartPodOperator because parameter is not in actual use``
  * ``Remove is_delete_operator_pod attribute from GKEStartPodOperator please use on_finish_action attribute instead``

* Hooks

  * ``Remove GenerativeModelHook.get_text_generation_model use GenerativeModelHook.get_generative_model instead``
  * ``Remove GenerativeModelHook.text_generation_model_predict use GenerativeModelHook.generative_model_generate_content instead``
  * ``Remove split_tablename function from airflow.providers.google.cloud.hooks.bigquery``
    ``use airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.split_tablename instead``
  * ``Remove get_auto_ml_text_training_job method from AutoMLHook because of deprecation of AutoMLText API``

* Links

  * ``Remove AutoMLDatasetLink use TranslationLegacyDatasetLink instead``
  * ``Remove AutoMLDatasetListLink use TranslationDatasetListLink instead``
  * ``Remove AutoMLModelLink use TranslationLegacyModelLink instead``
  * ``Remove AutoMLModelTrainLink use TranslationLegacyModelTrainLink instead``
  * ``Remove AutoMLModelPredictLink use TranslationLegacyModelPredictLink``


Breaking changes
~~~~~~~~~~~~~~~~

* ``Removal of deprecated items scheduled for May and June (#51646)``
* ``Remove deprecated for December 2024 and April 2025 in providers/google. (#50021)``

Features
~~~~~~~~

* ``Allow creation of empty files on GCS (#51669)``
* ``Add HttpToGCSOperator for transferring data from HTTP to GCS (#49625)``
* ``Add project_id to the BigQueryCheckOperator, BigQueryValueCheckOperator, BigQueryIntervalCheckOperator, BigQueryColumnCheckOperator, BigQueryTableCheckOperator, BigQueryToBigQueryOperator (#51006)``
* ``refactor: allow to edit the file listings before copy in S3ToGCSOperator (#49766)``
* ``feat: implementation of BaseOperatorLink for cloud run jobs to expose the GCP cloud logging url (#46911)``
* ``feat: add columnNameCharacterMap option to 'GCSToBigQueryOperator' (#49458)``
* ``Add DataflowJobStateCompleteTrigger (#47993)``
* ``Create operators for working with Ray for VertexAI (#47340)``

Bug Fixes
~~~~~~~~~

* ``Remove direct access to DB for safe_to_cancel() method for Dataproc and BigQuery triggers (#49711)``
* ``fix(providers/google): update the default value of PubSubPullOperator.deferrable to  conf.getboolean("operators", "default_deferrable", fallback=False) (#50935)``
* ``fix: CloudRunExecutJobOperator job_name param description (#50628)``
* ``Make gcs remote log args optional when using connection id (#50590)``
* ``FacebookAdsReportToGcsOperator: Replace 'converted_rows[0].keys()' with 'fields' (#50442)``
* ``Fix gcp remote log module import in airflow local settings (#49788)``

Misc
~~~~

* ``Add 'polling_period_seconds' and 'timeout_seconds' to the template fields of 'CloudRunExecuteJobOperator' (#49704)``
* ``Add extra fields and hide unnecessary fields for LevelDB connection UI page (#51343)``
* ``Remove sqlalchemy-spanner exclusion after it's been yanked. (#51433)``
* ``Exclude 'sqlalchemy-spanner' 1.12.0 (#51379)``
* ``Preemptively add proper Apache license for wordcount.go (#50968)``
* ``Add GoogleAdsHook UI connection form (#50755)``
* ``Add more template_fields to RunPipelineJobOperator (#50220)``
* ``Add match_glob field to templated fields of GCSListObjectsOperator (#50393)``
* ``Migrate 'BigQueryHook' to use 'get_df' (#50341)``
* ``Remove deprecated 'automl' link (#50456)``
* ``Ignore typing error that might occur for internal import in ray (#50253)``
* ``add root parent information to OpenLineage events (#49237)``
* ``Fix  error on pip 25.1 (#50098)``
* ``Add lower-binding for types-protobuf (#50075)``
* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``Adding dependency on google-cloud-bigquery-storage temporarily for google (#49807)``
* ``Remove ray extra from google aiplatform for Python 3.12 (#49797)``
* ``Temporarily install ray explicitly for python 3.12 (#49759)``
* ``Remove on_finish_action from template fields (#49637)``
* ``Bump some provider dependencies for faster resolution (#51727)``

Doc-only
~~~~~~~~

* ``Add Looker connection types and LevelDB documentation (#51058)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update xcom_pull usage for google system tests (#51527)``
   * ``fix exmaple_cloud_composer system test (#51108)``
   * ``Fix translate gcp native model system test (#51238)``
   * ``Fixes issue: https://github.com/apache/airflow/issues/40212 (#47479)``
   * ``Replace ORM calls in system tests when creating a connection (#50643)``
   * ``Flattening the 'requirements' input for python venv operators (#50521)``
   * ``Update system tests for Translate and Dataproc services (#49785)``
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Make xcom compatible with af2 in system tests in google provider (#49362)``
   * ``Avoid committing history for providers (#49907)``
   * ``Update system tests with GCE instance adding auto_delete parameter (#49232)``

15.1.0
......

Features
~~~~~~~~

* ``Add ORC support to GCSToBigQueryOperator and test for external tables (#49188)``

Bug Fixes
~~~~~~~~~

* ``Fix default value wait_for_operation_result in OperationHelper (#49121)``
* ``Fix get_confluent_token callback for Managed Kafka service (#48926)``
* ``Fix: BigQuery connection UI defaults 'Use Legacy SQL' to true on edit (#49038)``

Misc
~~~~

* ``remove superfluous else block (#49199)``
* ``Deprecate start_sql_job in DataflowHook (#48663)``
* ``type: annotate miss type in 'provider' (#49130)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use contextlib.suppress(exception) instead of try-except-pass and add SIM105 ruff rule (#49251)``
   * ``Fix xcom for system tests (#49337)``

15.0.1
......

Misc
~~~~

* ``Deprecate Life Science service (#48862)``

Doc-only
~~~~~~~~

* ``Add missing license comment in google docs (#48985)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix false friends in implicit string concatenation (#48871)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Small fixies for system tests in google-provider (#48861)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``

15.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Deprecated classes, parameters and features have been removed from the Google provider package.
  The following breaking changes were introduced:

  * Operators

    * Removed ``MLEngineStartBatchPredictionJobOperator`` use ``CreateBatchPredictionJobOperator`` instead
    * Removed ``MLEngineManageModelOperator`` use ``MLEngineCreateModelOperator`` or ``GetModelOperator`` from Vertex AI
      instead
    * Removed ``MLEngineDeleteModelOperator`` use ``DeleteModelOperator`` from Vertex AI instead
    * Removed ``MLEngineManageVersionOperator`` use appropriate operator from Vertex AI instead,
      e.g ``AddVersionAliasesOnModelOperator``
    * Removed ``MLEngineCreateVersionOperator`` use ``parent_model`` parameter for VertexAI operators
    * Removed ``MLEngineSetDefaultVersionOperator`` use ``SetDefaultVersionOnModelOperator`` from Vertex AI
      instead
    * Removed ``MLEngineListVersionsOperator`` use ``ListModelVersionsOperator`` from Vertex AI instead
    * Removed ``MLEngineDeleteVersionOperator`` use ``DeleteModelVersionOperator`` from Vertex AI instead
    * Removed ``MLEngineStartTrainingJobOperator`` use ``CreateCustomPythonPackageTrainingJobOperator`` from
      Vertex AI instead
    * Removed ``MLEngineTrainingCancelJobOperator`` use ``CancelCustomTrainingJobOperator`` from Vertex AI
      instead
    * Removed ``DataProcScaleClusterOperator`` use ``DataprocUpdateClusterOperator`` instead
    * Removed ``page_size`` parameter from ``GoogleAdsToGcsOperator``
    * Removed ``DeleteAutoMLTrainingJobOperator.training_pipeline`` method use ``training_pipeline_id`` parameter instead
    * Removed ``DeleteCustomTrainingJobOperator.training_pipeline`` method use ``training_pipeline_id`` parameter instead
    * Removed ``DeleteCustomTrainingJobOperator.custom_job`` method use ``custom_job_id`` parameter instead

  * Links

    * Removed ``DataFusionPipelineLinkHelper`` use ``airflow.providers.google.cloud.utils.helpers.resource_path_to_dict``
      instead

  * Hooks

    * Removed ``page_size`` parameter from ``GoogleAdsHook.search``, ``GoogleAdsHook.search_proto_plus``,
      ``GoogleAdsHook._search`` methods
    * Removed ``CloudBuildHook.create_build`` method use ``CloudBuildHook.create_build_without_waiting_for_result`` method
    * Removed ``DataflowHook.start_java_dataflow`` method please use ``airflow.providers.apache.beam.hooks.beam.start.start_java_pipeline``
      method to start pipeline and ``providers.google.cloud.hooks.dataflow.DataflowHook.wait_for_done`` method
      to wait for the required pipeline state instead
    * Removed ``DataflowHook.start_python_dataflow`` method please use ``airflow.providers.apache.beam.hooks.beam.start.start_python_pipeline``
      method to start pipeline and ``providers.google.cloud.hooks.dataflow.DataflowHook.wait_for_done`` method
      to wait for the required pipeline state instead
    * Removed ``CustomJobHook.cancel_pipeline_job`` method use ``PipelineJobHook.cancel_pipeline_job`` method instead
    * Removed ``CustomJobHook.create_pipeline_job`` method use ``PipelineJobHook.create_pipeline_job`` method instead
    * Removed ``CustomJobHook.get_pipeline_job`` method use ``PipelineJobHook.get_pipeline_job`` method instead
    * Removed ``CustomJobHook.list_pipeline_jobs`` method use ``PipelineJobHook.list_pipeline_jobs``  method instead
    * Removed ``CustomJobHook.delete_pipeline_job`` method use ``PipelineJobHook.delete_pipeline_job`` method instead
    * Removed ``GoogleBaseHook.get_client_info`` method use ``airflow.providers.google.common.consts.CLIENT_INFO`` instead
    * ``LifeSciencesHook`` life period extended to July 08, 2025

  * Utils

    * Removed ``create_evaluate_ops`` method

* ``Remove deprecated for February and March 2025 in providers/google. #48018``

Features
~~~~~~~~

* ``SFTPToGCSOperator: Added option to stream data directly to GCS without saving to disk (#48107)``

Bug Fixes
~~~~~~~~~

* ``fix: remove extra slash character from file URI formation in 'MetastoreHivePartitionSensor.poke' method. (#48731)``
* ``Disallowing types-protobuf 5.29.1.20250402 for google to fix mypy (#48666)``

Misc
~~~~

* ``Add 'ti' to the RemoteLogIO read and upload methods (#48804)``
* ``Rework remote task log handling for the structlog era. (#48491)``
* ``Move bases classes to 'airflow.sdk.bases' (#48487)``
* ``Move 'BaseSensorOperator' to TaskSDK definitions (#48244)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``

14.1.0
......

Features
~~~~~~~~

* ``Add DNS endpoint support for GKE Hooks and Operators (#48075)``
* ``feat: allow to set task/dag labels for 'DataprocCreateBatchOperator' (#46781)``
* ``Create operators for working with Consumer Groups for GCP Apache Kafka (#47056)``

Bug Fixes
~~~~~~~~~

* ``FIX: BigQuery traceability labels missing in TaskGroup (#47583)``

Misc
~~~~

* ``Vendor-in json-merge-patch and add '--no-dev' to direct uv tool install (#48210)``
* ``serialize http transports contained in composite transport (#47444)``
* ``AIP-72: Handle Custom XCom Backend on Task SDK (#47339)``
* ``misc: update API version of Google ADS (#47772)``
* ``Introduce operation helper class and refactor (#45354)``
* ``Remove auth backends from core Airflow (#47399)``
* ``Mark Google Oauth2 backend as Airflow 2 only (#47622)``
* ``Add Dataflow Apache Beam Java streaming system test (#47209)``
* ``Add legacy namespace packages to airflow.providers (#47064)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``
   * ``Prepare docs for Mar 1st wave of providers (#47545)``
   * ``AIP-72: Moving BaseOperatorLink to task sdk (#47008)``
   * ``Bugfix typing after google-ads release 26.0.0 release (#47471)``
   * ``Remove extra string interpolation from fstrings (#47436)``
   * ``Base AWS classes - S3 (#47321)``
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Remove unused methods from auth managers (#47316)``
   * ``Remove 'airflow.www' module (#47318)``
   * ``doc: Add GCSListObjectsOperator to operators listed in documentation in Google Provider for GCS  #39290 (#47002)``
   * ``Deprecating email, email_on_retry, email_on_failure in BaseOperator (#47146)``
   * ``Implement stale dag bundle cleanup (#46503)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``doc: fix the typo for glob regex  #47116 (#47134)``
   * ``Render structured logs in the new UI rather than showing raw JSON (#46827)``
   * ``Remove old UI and webserver (#46942)``
   * ``Remove example_dags folder (#47052)``
   * ``Update documentation for Dataflow operators (#46954)``
   * ``Remove extra whitespace in provider readme template (#46975)``
   * ``Fix 'exists' method to support using Requester Pays (#46759)``
   * ``Bump min. 'google-cloud-pubsub' ver. to v2.21.3 (#46984)``
   * ``Upgrade flit to 3.11.0 (#46938)``

14.0.0
......

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.

13.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Deprecated classes, parameters and features have been removed from the Google provider package.
  The following breaking changes were introduced:

  * Operators

    * Removed ``AutoMLBatchPredictOperator``. Please use the operators from ``airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job`` instead
    * Removed ``DataflowStartSqlJobOperator``. Please ``DataflowStartYamlJobOperator`` instead
    * Removed ``PromptLanguageModelOperator``. Please ``TextGenerationModelPredictOperator`` instead
    * Removed ``GenerateTextEmbeddingsOperator``. Please ``TextEmbeddingModelGetEmbeddingsOperator`` instead
    * Removed ``PromptMultimodalModelOperator``. Please ``GenerativeModelGenerateContentOperator`` instead
    * Removed ``PromptMultimodalModelWithMediaOperator``. Please ``GenerativeModelGenerateContentOperator`` instead

  * Hooks

    * Removed ``GenerativeModelHook.prompt_multimodal_model_with_media()``. Please use ``GenerativeModelHook.generative_model_generate_content()`` instead
    * Removed ``GenerativeModelHook.prompt_multimodal_model()``. Please use ``GenerativeModelHook.generative_model_generate_content()`` instead
    * Removed ``GenerativeModelHook.get_generative_model_part()``. Please use ``GenerativeModelHook.generative_model_generate_content()`` instead
    * Removed ``GenerativeModelHook.prompt_language_model()``. Please use ``GenerativeModelHook.text_generation_model_predict()`` instead
    * Removed ``GenerativeModelHook.generate_text_embeddings()``. Please use ``GenerativeModelHook.text_generation_model_predict()`` instead


* ``Remove deprecated functionality from google provider (#46235)``

Features
~~~~~~~~

* ``Create operators for working with Clusters for GCP Apache Kafka (#45795)``
* ``Deprecate old bigquery operators and introduce CreateTableOperator (#46673)``
* ``Add operators for Entry resource (#46647)``
* ``Add deferrable mode to the PubSubPullOperator (#45835)``
* ``Add deferrable mode to google cloud storage transfer sensor and operators (#45754)``
* ``Add operators for AspectType resource (#46240)``
* ``feat: Add OpenLineage support for BigQueryDataTransferServiceStartTransferRunsOperator (#45801)``
* ``feat: Add OpenLineage support for CloudSQLExecuteQueryOperator (#45182)``
* ``Add new operators for Entry Type (#45799)``
* ``Add Dataplex Catalog Entry Group operators (#45751)``
* ``feat: Add Hook Level Lineage support for BigQueryHook (#45706)``
* ``feat: Add OpenLineage support for MsSqlHook and MSSQLToGCSOperator (#45637)``
* ``Adding holiday_region parameter  to create_auto_ml_forecasting_training_job in AutoMl hook (#45465)``
* ``feat: automatically inject OL transport info into spark jobs (#45326)``
* ``feat: Add OpenLineage support for transfer operators between GCS and SFTP (#45485)``
* ``Implement AlloyDB create/update/delete user and backups operators (#45378)``
* ``feat: Add OpenLineage support for some SQL to GCS operators (#45242)``
* ``feat: Add OpenLineage support for some BQ operators (#45422)``
* ``feat: Add OpenLineage support for non-query jobs in BigQueryInsertJobOperator (#44996)``
* ``feat: automatically inject OL info into spark job in DataprocInstantiateInlineWorkflowTemplateOperator (#44697)``
* ``feat: Add CLL to OpenLineage in BigQueryInsertJobOperator (#44872)``
* ``feat: automatically inject OL info into spark job in DataprocCreateBatchOperator (#44612)``
* ``Implement AlloyDB create/update/delete instance operators (#45241)``
* ``Add BigQuery job link (#45020) (#45222)``
* ``Introduce gcp translation(V3) glossaries providers (#45085)``
* ``Add dependencies section to gcp cloud build instance response (#46947)``
* ``Create operators for working with Topics for GCP Apache Kafka (#46865)``

Bug Fixes
~~~~~~~~~

* ``fix return job_id when async finish before deffed in BigQueryInsertJobOperator (#46753)``
* ``fix: Dataproc operators fail to import without OpenLineage (#46561)``
* ``Add backward compatibility for old Airflow version for CloudComposerDAGRunSensor (#46011)``
* ``Improve google credentials error message (#45553)``
* ``Add case of aarch64 environments in CloudSqlProxyRunner (#45488)``
* ``fix GCSToGCSOperator bug when copy single object with replace to False (#45181)``

Misc
~~~~

* ``Rework the TriggererJobRunner to run triggers in a process without DB access (#46677)``
* ``AIP-72: Improving Operator Links Interface to Prevent User Code Execution in Webserver (#46613)``
* ``Remove Airflow 3 Deprecation Warning in Google Provider, Move to Private Warning (#46599)``
* ``Add evaluation extra to google-cloud-aiplatform (#46270)``
* ``Limit 'google-cloud-aiplatform' to fix issues in CI (#46242)``
* ``Improve grammar in cloud_composer.rst (#45708)``
* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``
* ``Deprecate page_size parameter for Google Ads API v17 and later versions (#45239)``
* ``Remove obsolete pandas specfication for pre-python 3.9 (#45399)``
* ``Deprecation policy for apache-airflow-providers-google package (#44985)``
* ``Add lower bound for alloydb (#45180)``
* ``Deprecate gcp AutoML module (#44875)``
* ``Refactor deferrable mode for BeamRunPythonPipelineOperator and BeamRunJavaPipelineOperator (#46678)``
* ``Bump cloud build to 3.31.0 (#46953)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove tests from example_dags folder (#46819)``
   * ``Speedup test collection for 'tests/www' (#46812)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Move Google provider to new provider structure (#46344)``
   * ``Reducing dedent to fix doc building issues in CI (#46350)``

12.0.0
......

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Deprecated classes, parameters and features have been removed from the Google provider package.
  The following breaking changes were introduced:

  * Operators

    * Removed ``CreateDataPipelineOperator``. Please use the ``DataflowCreatePipelineOperator`` instead
    * Removed ``RunDataPipelineOperator``. Please use the ``DataflowRunPipelineOperator`` instead
    * Removed ``CreateDataPipelineOperator``. Please use the ``DataflowCreatePipelineOperator`` instead
    * Removed ``RunDataPipelineOperator``. Please use the ``DataflowRunPipelineOperator`` instead

  * Hooks

    * Removed ``DataPipelineHook``. Please use the ``DataflowHook`` instead
    * Removed ``DataPipelineHook``. Please use the DataflowHook instead

* ``Google provider delete deprecated reaching removal date (December 2024) (#45084)``

Features
~~~~~~~~

* ``Add Google Vertex AI Feature Store - Feature View Sync Operators, Sensor (#44891)``
* ``Introduce gcp translation(V3), translate document providers (#44971)``
* ``Introduce the translation API v3 (advanced) models operators. (#44627)``
* ``Support multiple SQL queries in Dataproc SQL job (#44890)``
* ``feat: add OpenLineage support for BigQuery Create Table operators (#44783)``
* ``feat: add OpenLineage support for S3ToGCSOperator (#44426)``
* ``feat: automatically inject OL info into spark job in DataprocSubmitJobOperator (#44477)``
* ``Implement AlloyDB operators: create/update/delete clusters (#45027)``

Bug Fixes
~~~~~~~~~

* ``Fix MetastoreHivePartitionSensor failing due to duplicate aliases (#45001)``
* ``Fix failing OpenLineage emition for InsertBigQueryOperator  (#44650)``
* ``BigQueryInsertJobOperator: log transient error and check job state before marking task as success (#44279)``
* ``Make 'CloudBatchSubmitJobOperator' fail when job fails (#44425)``

Misc
~~~~

* ``Remove references to AIRFLOW_V_2_9_PLUS (#44987)``
* ``Consistent way of checking Airflow version in providers (#44686)``
* ``Fix yoda-conditions (#44466)``
* ``feat: add OpenLineage support for transfer operators between gcs and local (#44417)``
* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``chore: remove deprecated bigquery facets from OpenLineage utils (#44838)``
* ``Implement KubernetesInstallKueueOperator (#44568)``
* ``Fix docs for dataform operators (#44729)``
* ``Deprecate VertexAI PaLM text generative model (#44719)``
* ``Show prominent warning for deprecations in docs (#44479)``
* ``chore: unify handling of gcs paths (#44410)``

11.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  Deprecated classes, parameters and features have been removed from the Google provider package.
  The following breaking changes were introduced:

  * Operators

    * Removed ``CreateAutoMLTextTrainingJobOperator``. Use ``SupervisedFineTuningTrainOperator`` instead
    * Removed ``BigQueryExecuteQueryOperator``. Use ``BigQueryInsertJobOperator`` instead
    * Removed ``BigQueryPatchDatasetOperator``. Use ``BigQueryUpdateDatasetOperator`` instead
    * Removed ``DataflowCreateJavaJobOperator``. Use ``BeamRunJavaPipelineOperator`` instead
    * Removed ``DataflowCreatePythonJobOperator``. Use ``BeamRunPythonPipelineOperator`` instead
    * Removed ``DataprocSubmitPigJobOperator``. Use ``DataprocSubmitJobOperator`` instead
    * Removed ``DataprocSubmitHiveJobOperator``. Use ``DataprocSubmitJobOperator`` instead
    * Removed ``DataprocSubmitSparkSqlJobOperator``. Use ``DataprocSubmitJobOperator`` instead
    * Removed ``DataprocSubmitSparkJobOperator``. Use ``DataprocSubmitJobOperator`` instead
    * Removed ``DataprocSubmitHadoopJobOperator``. Use ``DataprocSubmitJobOperator`` instead
    * Removed ``DataprocSubmitPySparkJobOperator``. Use ``DataprocSubmitJobOperator`` instead
    * Removed ``GoogleAnalyticsListAccountsOperator``. Use ``GoogleAnalyticsAdminListAccountsOperator`` instead
    * Removed ``GoogleAnalyticsGetAdsLinkOperator``. Use ``GoogleAnalyticsAdminGetGoogleAdsLinkOperator`` instead
    * Removed ``GoogleAnalyticsRetrieveAdsLinksListOperator``. Use ``GoogleAnalyticsAdminListGoogleAdsLinksOperator`` instead
    * Removed ``GoogleAnalyticsDataImportUploadOperator``. Use ``GoogleAnalyticsAdminCreateDataStreamOperator`` instead
    * Removed ``GoogleAnalyticsDeletePreviousDataUploadsOperator``. Use ``GoogleAnalyticsAdminDeleteDataStreamOperator`` instead
    * Removed ``GoogleAnalyticsModifyFileHeadersDataImportOperator``. The class is no longer in actual use due to
      Google Analytics API v3 has reached sunset, and thus the covered use case is no longer relevant
    * Removed ``GoogleCampaignManagerDeleteReportOperator.delegate_to``. Use ``GoogleCampaignManagerDeleteReportOperator.impersonation_chain`` instead
    * Removed ``GoogleCampaignManagerDownloadReportOperator.delegate_to``. Use ``GoogleCampaignManagerDownloadReportOperator.impersonation_chain`` instead
    * Removed ``GoogleCampaignManagerInsertReportOperator.delegate_to``. Use ``GoogleCampaignManagerInsertReportOperator.impersonation_chain`` instead
    * Removed ``GoogleCampaignManagerRunReportOperator.delegate_to``. Use ``GoogleCampaignManagerRunReportOperator.impersonation_chain`` instead
    * Removed ``GoogleCampaignManagerBatchInsertConversionsOperator.delegate_to``. Use ``GoogleCampaignManagerBatchInsertConversionsOperator.impersonation_chain`` instead
    * Removed ``GoogleCampaignManagerBatchUpdateConversionsOperator.delegate_to``. Use ``GoogleCampaignManagerBatchUpdateConversionsOperator.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360CreateQueryOperator.delegate_to``. Use ``GoogleDisplayVideo360CreateQueryOperator.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360DeleteReportOperator.delegate_to``. Use ``GoogleDisplayVideo360DeleteReportOperator.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360DownloadReportV2Operator.delegate_to``. Use ``GoogleDisplayVideo360DownloadReportV2Operator.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360RunQueryOperator.delegate_to``. Use ``GoogleDisplayVideo360RunQueryOperator.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360DownloadLineItemsOperator.delegate_to``. Use ``GoogleDisplayVideo360DownloadLineItemsOperator.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360UploadLineItemsOperator.delegate_to``. Use ``GoogleDisplayVideo360UploadLineItemsOperator.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360CreateSDFDownloadTaskOperator.delegate_to``. Use ``GoogleDisplayVideo360CreateSDFDownloadTaskOperator.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360SDFtoGCSOperator.delegate_to``. Use ``GoogleDisplayVideo360SDFtoGCSOperator.impersonation_chain`` instead
    * Removed ``GoogleSheetsCreateSpreadsheetOperator.delegate_to``. Use ``GoogleSheetsCreateSpreadsheetOperator.impersonation_chain`` instead
    * Removed ``GCSToGoogleDriveOperator.delegate_to``. Use ``GCSToGoogleDriveOperator.impersonation_chain`` instead
    * Removed ``GCSToGoogleSheetsOperator.delegate_to``. Use ``GCSToGoogleSheetsOperator.impersonation_chain`` instead
    * Removed ``LocalFilesystemToGoogleDriveOperator.delegate_to``. Use ``LocalFilesystemToGoogleDriveOperator.impersonation_chain`` instead
    * Removed ``SQLToGoogleSheetsOperator.delegate_to``. Use ``SQLToGoogleSheetsOperator.impersonation_chain`` instead
    * Removed ``CreateBatchPredictionJobOperator.sync``. This parameter is not in actual use
    * Removed ``CreateHyperparameterTuningJobOperator.sync``. This parameter is not in actual use
    * Removed ``CustomTrainingJobBaseOperator.sync``. This parameter is not in actual use
    * Removed ``GKEStartPodOperator.get_gke_config_file()``. Please use ``GKEStartPodOperator.fetch_cluster_info()`` instead

  * Triggers

    * Removed support of ``delegate_to`` field in the ``GCSCheckBlobUpdateTimeTrigger.hook_params`` parameter

  * Sensors

    * Removed ``BigQueryTableExistenceAsyncSensor``. Use ``BigQueryTableExistenceSensor``  and set deferrable attribute
      to True instead
    * Removed ``BigQueryTableExistencePartitionAsyncSensor``. Use ``BigQueryTablePartitionExistenceSensor`` and set
      deferrable attribute to True instead
    * Removed ``CloudComposerEnvironmentSensor``. Use ``CloudComposerCreateEnvironmentOperator``,
      ``CloudComposerUpdateEnvironmentOperator``, or ``CloudComposerDeleteEnvironmentOperator`` instead
    * Removed ``GCSObjectExistenceAsyncSensor``. Use ``GCSObjectExistenceSensor``  and set deferrable attribute
      to True instead
    * Removed ``GoogleCampaignManagerReportSensor.delegate_to``. Use ``GoogleCampaignManagerReportSensor.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360GetSDFDownloadOperationSensor.delegate_to``. Use ``GoogleDisplayVideo360GetSDFDownloadOperationSensor.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360RunQuerySensor.delegate_to``. Use ``GoogleDisplayVideo360RunQuerySensor.impersonation_chain`` instead
    * Removed ``GoogleDriveFileExistenceSensor.delegate_to``. Use ``GoogleDriveFileExistenceSensor.impersonation_chain`` instead

  * Links

    * Removed ``BigQueryConsoleIndexableLink``. This property is no longer in actual use
    * Removed ``BigQueryConsoleLink``. This property is no longer in actual use

  * Hooks

    * Removed ``GKEDeploymentHook``. Use ``GKEKubernetesHook`` instead
    * Removed ``GKECustomResourceHook``. Use ``GKEKubernetesHook`` instead
    * Removed ``GKEPodHook``. Use ``GKEKubernetesHook`` instead
    * Removed ``GKEJobHook``. Use ``GKEKubernetesHook`` instead
    * Removed ``GKEPodAsyncHook``. Use ``GKEKubernetesAsyncHook`` instead
    * Removed ``SecretsManagerHook``. Use ``GoogleCloudSecretManagerHook`` instead
    * Removed ``GoogleAnalyticsHook``. The class is no longer in actual use due to Google Analytics API v3 has reached
      sunset
    * Removed ``GoogleBaseHook.delegate_to``. Please use ``GoogleBaseHook.impersonation_chain``. Please note that
      the ``delegate_to`` parameter used to be inherited in all Google hooks, and from now its support is removed
      everywhere within the Google provider
    * Removed ``GoogleDiscoveryApiHook.delegate_to``. Please use ``GoogleDiscoveryApiHook.impersonation_chain`` instead
    * Removed ``GoogleCampaignManagerHook.delegate_to``. Please use ``GoogleCampaignManagerHook.impersonation_chain`` instead
    * Removed ``GoogleDisplayVideo360Hook.delegate_to``. Please use ``GoogleDisplayVideo360Hook.impersonation_chain`` instead
    * Removed ``GoogleSearchAdsHook.delegate_to``. Please use ``GoogleSearchAdsHook.impersonation_chain`` instead
    * Removed ``GoogleCalendarHook.delegate_to``. Please use ``GoogleCalendarHook.impersonation_chain`` instead
    * Removed ``GoogleDriveHook.delegate_to``. Please use ``GoogleDriveHook.impersonation_chain`` instead
    * Removed ``GSheetsHook.delegate_to``. Please use ``GSheetsHook.impersonation_chain`` instead
    * Removed ``BigQueryHook.credentials_path``. This property is no longer in actual use
    * Removed ``GKEHook.get_conn()``. Please use ``GKEHook.get_cluster_manager_client()`` instead
    * Removed ``GKEHook.get_client()``. Please use ``GKEHook.get_cluster_manager_client()`` instead
    * Removed ``BigQueryHook.patch_table()``. Please use ``BigQueryHook.update_table()`` instead
    * Removed ``BigQueryHook.patch_dataset()``. Please use ``BigQueryHook.update_dataset()`` instead
    * Removed ``BigQueryHook.get_dataset_tables_list()``. Please use ``BigQueryHook.get_dataset_tables()`` instead
    * Removed ``BigQueryHook.run_table_delete()``. Please use ``BigQueryHook.delete_table()`` instead
    * Removed ``BigQueryHook.get_tabledata()``. Please use ``BigQueryHook.list_rows()`` instead
    * Removed ``BigQueryHook.cancel_query()``. Please use ``BigQueryHook.cancel_job()`` instead
    * Removed ``BigQueryHook.run_with_configuration()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryHook.run_load()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryHook.run_copy()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryHook.run_extract()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryHook.run_query()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryHook.create_external_table()``. Please use ``BigQueryHook.create_empty_table()`` instead
    * Removed ``BigQueryHook.get_service()``. Please use ``BigQueryHook.get_client()`` instead

  * Backends

    * Removed ``CloudSecretManagerBackend.get_conn_uri()``. Please use ``CloudSecretManagerBackend.get_conn_value()`` instead

  * Other deprecations

    * Removed ``BigQueryBaseCursor.create_empty_table()``. Please use ``BigQueryHook.create_empty_table()`` instead
    * Removed ``BigQueryBaseCursor.create_empty_dataset()``. Please use ``BigQueryHook.create_empty_dataset()`` instead
    * Removed ``BigQueryBaseCursor.get_dataset_tables()``. Please use ``BigQueryHook.get_dataset_tables()`` instead
    * Removed ``BigQueryBaseCursor.delete_dataset()``. Please use ``BigQueryHook.delete_dataset()`` instead
    * Removed ``BigQueryBaseCursor.create_external_table()``. Please use ``BigQueryHook.create_empty_table()`` instead
    * Removed ``BigQueryBaseCursor.patch_table()``. Please use ``BigQueryHook.update_table()`` instead
    * Removed ``BigQueryBaseCursor.insert_all()``. Please use ``BigQueryHook.insert_all()`` instead
    * Removed ``BigQueryBaseCursor.update_dataset()``. Please use ``BigQueryHook.update_dataset()`` instead
    * Removed ``BigQueryBaseCursor.patch_dataset()``. Please use ``BigQueryHook.update_dataset()`` instead
    * Removed ``BigQueryBaseCursor.get_dataset_tables_list()``. Please use ``BigQueryHook.get_dataset_tables()`` instead
    * Removed ``BigQueryBaseCursor.get_datasets_list()``. Please use ``BigQueryHook.get_datasets_list()`` instead
    * Removed ``BigQueryBaseCursor.get_dataset()``. Please use ``BigQueryHook.get_dataset()`` instead
    * Removed ``BigQueryBaseCursor.run_grant_dataset_view_access()``. Please use ``BigQueryHook.run_grant_dataset_view_access()`` instead
    * Removed ``BigQueryBaseCursor.run_table_upsert()``. Please use ``BigQueryHook.run_table_upsert()`` instead
    * Removed ``BigQueryBaseCursor.run_table_delete()``. Please use ``BigQueryHook.delete_table()`` instead
    * Removed ``BigQueryBaseCursor.get_tabledata()``. Please use ``BigQueryHook.list_rows()`` instead
    * Removed ``BigQueryBaseCursor.get_schema()``. Please use ``BigQueryHook.get_schema()`` instead
    * Removed ``BigQueryBaseCursor.poll_job_complete()``. Please use ``BigQueryHook.poll_job_complete()`` instead
    * Removed ``BigQueryBaseCursor.cancel_query()``. Please use ``BigQueryHook.cancel_job()`` instead
    * Removed ``BigQueryBaseCursor.run_with_configuration()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryBaseCursor.run_load()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryBaseCursor.run_copy()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryBaseCursor.run_extract()``. Please use ``BigQueryHook.insert_job()`` instead
    * Removed ``BigQueryBaseCursor.run_query()``. Please use ``BigQueryHook.insert_job()`` instead


* ``Remove deprecated functionality from Google provider (#43953)``

Features
~~~~~~~~

* ``feat: add OpenLineage support for BigQueryToBigQueryOperator (#44214)``
* ``Introduce gcp advance API (V3) translate native datasets operators (#44271)``
* ``Introduce new gcp TranslateText and TranslateTextBatch operators (#43860)``
* ``Add gcloud command to DataprocCreateClusterOperator to be able to create dataproc on GKE cluster (#44185)``

Bug Fixes
~~~~~~~~~

* ``Fix incorrect query in 'BigQueryAsyncHook.create_job_for_partition_get'. (#44225)``
* ``Fix Dataplex Data Quality partial update (#44262)``

Misc
~~~~

* ``Bump 'google-cloud-translate' to '3.16' (#44297)``
* ``Bump 'google-cloud-datacatalog' to '3.23' (#44281)``
* ``Rename execution_date to logical_date across codebase (#43902)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Move Asset user facing components to task_sdk (#43773)``

10.26.0
.......

Features
~~~~~~~~

* ``Add support for IAM database authentication for CloudSQL connection (#43631)``
* ``Provide option to 'force_delete' for 'GCSToBigQueryOperator' (#43785)``
* ``Unify reattach_states parameter logic across BigQuery operators (#43259)``

Bug Fixes
~~~~~~~~~

* ``Remove non-existing field  from the ListCustomTrainingJobOperator's template_fields (#43924)``
* ``Fix validating 'parent_model' parameter in 'UploadModelOperator' (#43473)``

Misc
~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``
* ``Remove Airflow 2.1 compat code in Google provider (#43952)``
* ``Explain how to use uv with airflow virtualenv and make it works (#43604)``
* ``Move python operator to Standard provider (#42081)``
* ``Update version of Google ADS (#43474)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix CI ruff format static checks (#43908)``

10.25.0
.......

Features
~~~~~~~~

* ``feat: add Hook Level Lineage support for GCSHook (#42507)``
* ``feat: sensor to check status of Dataform action (#43055)``
* ``Create Operators for Google Cloud Vertex AI Context Caching (#43008)``

Bug Fixes
~~~~~~~~~

* ``Fix outdated CloudRunExecuteJobOperator docs (#43195)``
* ``Fix TestTranslationLegacyModelPredictLink dataset_id error (#42463)``

Misc
~~~~

* ``Add a debug log for creating batch workloads in dataproc (#43265)``
* ``add min version to plyvel (#43129)``
* ``vertex ai training operators: add display_name to rendered fields (#43028)``
* ``Make google provider pyarrow dependency explicit (#42996)``
* ``Restrict looker-sdk version 24.18.0 and microsoft-kiota-http 1.3.4 (#42954)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove dag.run() method (#42761)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

10.24.0
.......

Features
~~~~~~~~

* ``Add 'retry_if_resource_not_ready' logic for DataprocCreateClusterOperator and DataprocCreateBatchOperator (#42703)``

Bug Fixes
~~~~~~~~~

* ``Publish Dataproc Serverless Batch link after it starts if batch_id was provided (#41153)``
* ``fix PubSubAsyncHook in PubsubPullTrigger to use gcp_conn_id (#42671)``
* ``Fix consistent return response from PubSubPullSensor (#42080)``
* ``Undo partition exclusion from the table name when splitting a full BigQuery table name (#42541)``
* ``Fix gcp text to speech uri fetch (#42309)``
* ``Refactor ''bucket.get_blob'' calls in ''GCSHook'' to handle validation for non-existent objects. (#42474)``
* ``Bugfix/dataflow job location passing (#41887)``

Misc
~~~~

* ``Removed conditional check for task context logging in airflow version 2.8.0 and above (#42764)``
* ``Rename dataset related python variable names to asset (#41348)``
* ``Deprecate AutoMLBatchPredictOperator and refactor AutoMl system tests (#42260)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

10.23.0
.......

Features
~~~~~~~~

* ``Add ability to create Flink Jobs in dataproc cluster (#42342)``
* ``Add new Google Search 360 Reporting Operators (#42255)``
* ``Add return_immediately as argument to the PubSubPullSensor class (#41842)``
* ``Add parent_model param in 'UploadModelOperator' (#42091)``
* ``Add DataflowStartYamlJobOperator (#41576)``
* ``Add RunEvaluationOperator for Google Vertex AI Rapid Evaluation API (#41940)``
* ``Add CountTokensOperator for Google Generative AI CountTokensAPI (#41908)``
* ``Add Supervised Fine Tuning Train Operator, Hook, Tests, Docs (#41807)``

Bug Fixes
~~~~~~~~~

* ``Minor fixes to ensure successful Vertex AI LLMops pipeline (#41997)``
* ``Exclude partition from BigQuery table name (#42130)``
* ``[Fix #41763]: Redundant forward slash in SFTPToGCSOperator when destination_path is not specified or have default value (#41928)``
* ``Fix poll_interval in GKEJobTrigger (#41712)``
* ``update pattern for dataflow job id extraction (#41794)``
* ``Enforce deprecation message format with EOL for google provider package (#41637)``
* ``Fix 'do_xcom_push' and 'get_logs' functionality for KubernetesJobOperator (#40814)``

Misc
~~~~

* ``Mark VertexAI AutoMLText deprecation (#42251)``
* ``Exclude google-cloud-spanner 3.49.0 (#42011)``
* ``Remove system test for derepcated Google analytics operators (#41946)``
* ``Update min version of google-cloud-bigquery package (#41882)``
* ``Unpin google-cloud-bigquery package version for Google provider (#41839)``
* ``Move away from deprecated DAG.following_schedule() method (#41773)``
* ``remove deprecated soft_fail from providers (#41710)``
* ``Update the version of google-ads (#41638)``
* ``Remove deprecated log handler argument filename_template (#41552)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

Main
.......

.. warning::
  The previous Search Ads 360 Reporting API <https://developers.google.com/search-ads/v2/how-tos/reporting>
  (which is currently in use in google-provider) was already decommissioned on June 30, 2024
  (see details <https://developers.google.com/search-ads/v2/migration>).
  All new reporting development should use the new Search Ads 360 Reporting API.
  Currently, the Reporting operators, sensors and hooks are failing due to the decommission.
  The new API is not a replacement for the old one, it has a different approach and endpoints.
  Therefore, new operators implemented for the new API.

10.22.0
.......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add 'CloudRunServiceHook' and 'CloudRunCreateServiceOperator' (#40008)``

Bug Fixes
~~~~~~~~~

* ``fix(providers/google): add missing sync_hook_class to CloudDataTransferServiceAsyncHook (#41417)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``
* ``Refactor 'DataprocCreateBatchOperator' (#41527)``
* ``Upgrade package gcloud-aio-auth>=5.2.0 (#41262)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

10.21.1
.......

Bug Fixes
~~~~~~~~~

* ``fix unnecessary imports for CloudSQL hook (#41009)``
* ``Move sensitive information to the secret manager for the system test google_analytics_admin (#40951)``
* ``Fix Custom Training Job operators to accept results without managed model (#40685)``
* ``Fix behavior for reattach_state parameter in BigQueryInsertJobOperator (#40664)``
* ``Fix CloudSQLDatabaseHook temp file handling (#41092)``

Misc
~~~~

* ``Refactor dataproc system tests (#40720)``
* ``openlineage: migrate OpenLineage provider to V2 facets. (#39530)``
* ``Resolve CloudSQLDatabaseHook deprecation warning (#40834)``
* ``Fix BeamRunJavaPipelineOperator fails without job_name set (#40645)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare Providers docs ad hoc release (#41074)``

10.21.0
.......

.. note::

  The change  ``Fix 'GCSToGCSOperator' behavior difference for moving single object (#40162)`` has
  been reverted as it turned out to break too much existing workflows. The behavior of the
  ``GCSToGCSOperator`` has been restored to the previous behavior.

Features
~~~~~~~~

* ``Update Google Cloud Generative Model Hooks/Operators to bring parity with Vertex AI API (#40484)``
* ``DataflowStartFlexTemplateOperator. Check for Dataflow job type each check cycle. (#40584)``
* ``Added chunk_size parameter to LocalFilesystemToGCSOperator (#40379)``
* ``Add support for query parameters to BigQueryCheckOperator (#40558)``
* ``Add link button to dataproc job in DataprocCreateBatchOperator (#40643)``

Bug Fixes
~~~~~~~~~

* ``Revert "Fix 'GCSToGCSOperator' behavior difference for moving single object (#40162)" (#40577)``
* ``fix BigQueryInsertJobOperator's return value and openlineage extraction in deferrable mode (#40457)``
* ``fix OpenLineage extraction for GCP deferrable operators (#40521)``
* ``fix respect project_id in CloudBatchSubmitJobOperator (#40560)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Resolve google deprecations in tests (#40629)``
   * ``Resolve google vertex ai deprecations in tests (#40506)``
   * ``Add notes about reverting the change in GCSToGCSOperator (#40579)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

10.20.0
.......

.. note::

  The ``GCSToGCSOperator`` now retains the nested folder structure when moving or copying a single
  object, aligning its behavior with the behavior for multiple objects. If this change impacts your
  workflows, you may need to adjust your ``source_object`` parameter to include the full path up to
  the folder containing your single file and specify ``destination_object`` explicitly to ignore
  nested folders. For example, if you previously used ``source_object='folder/nested_folder/'``, to
  move file ``'folder/nested_folder/second_nested_folder/file'`` you should now use
  ``source_object='folder/nested_folder/second_nested_folder/'`` and specify
  ``destination_object='folder/nested_folder/'``. This would move the file to ``'folder/nested_folder/file'``
  instead of the fixed behavior of moving it to ``'folder/nested_folder/second_nested_folder/file'``.

.. warning::

  The change above has been reverted in the 10.21.0 release. The behavior of the
  ``GCSToGCSOperator`` has been restored to the previous behavior.

Features
~~~~~~~~

* ``Add generation_config and safety_settings to google cloud multimodal model operators (#40126)``
* ``Add missing location param to 'BigQueryUpdateTableSchemaOperator' (#40237)``
* ``Add support for external IdP OIDC token retrieval for Google Cloud Operators. (#39873)``
* ``Add encryption_configuration parameter to BigQuery operators (#40063)``
* ``Add default gcp_conn_id to GoogleBaseAsyncHook (#40080)``
* ``Add ordering key option for PubSubPublishMessageOperator GCP Operator (#39955)``
* ``Add method to get metadata from GCS blob in GCSHook (#38398)``
* ``Add window parameters to create_auto_ml_forecasting_training_job in AutoMLHook (#39767)``
* ``Implement CloudComposerDAGRunSensor (#40088)``
* ``Implement 'CloudDataTransferServiceRunJobOperator' (#39154)``
* ``Fetch intermediate log async GKEStartPod   (#39348)``
* ``Add OpenLineage support for AzureBlobStorageToGCSOperator in google provider package (#40290)``

Bug Fixes
~~~~~~~~~

* ``Fix hive_partition_sensor system test (#40023)``
* ``Fix openai 1.32 breaking openai tests (#40110)``
* ``Fix credentials initialization revealed by mypy version of google auth (#40108)``
* ``Fix regular expression to exclude double quote and newline in DataflowHook (#39991)``
* ``Fix replace parameter for BigQueryToPostgresOperator (#40278)``
* ``Fix 'GCSToGCSOperator' behavior difference for moving single object (#40162)``

Misc
~~~~

* ``Refactor datapipeline operators (#39716)``
* ``Update pandas minimum requirement for Python 3.12 (#40272)``
* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``openlineage: execute extraction and message sending in separate process (#40078)``
* ``Bump minimum version of google-auth to 2.29.0 (#40190)``
* ``Bump google-ads version to use v17 by default (#40158)``
* ``google: move openlineage imports inside methods (#40062)``
* ``Add job_id as template_field in DataplexGetDataQualityScanResultOperator (#40041)``
* ``Add dependency to httpx >= 0.25.0 everywhere (#40256)``

10.19.0
.......

.. note::
  Several AutoML operators have stopped being supported following the shutdown of a legacy version of
  AutoML Natural Language, Tables, Vision, and Video Intelligence services. This includes
  ``AutoMLDeployModelOperator``, ``AutoMLTablesUpdateDatasetOperator``, ``AutoMLTablesListTableSpecsOperator``
  and ``AutoMLTablesListColumnSpecsOperator``. Please refer to the operator documentation to find out
  about available alternatives, if any. For additional information regarding the AutoML shutdown see:

* `AutoML Natural Language <https://cloud.google.com/natural-language/automl/docs/deprecations>`_
* `AutoML Tables <https://cloud.google.com/automl-tables/docs/deprecations>`_
* `AutoML Vision <https://cloud.google.com/vision/automl/docs/deprecations>`_
* `AutoML Video Intelligence <https://cloud.google.com/video-intelligence/automl/docs/deprecations>`_

Features
~~~~~~~~

* ``Introduce anonymous credentials in GCP base hook (#39695)``


Bug Fixes
~~~~~~~~~

* ``Remove parent_model version suffix if it is passed to Vertex AI operators (#39640)``
* ``Fix BigQueryCursor execute method if the location is missing (#39659)``
* ``Fix acknowledged functionality in deferrable mode for PubSubPullSensor (#39711)``
* ``Reroute AutoML operator links to Google Translation links (#39668)``
* ``Pin google-cloud-bigquery to < 3.21.0 (#39583)``

Misc
~~~~

* ``Remove 'openlineage.common' dependencies in Google and Snowflake providers. (#39614)``
* ``Deprecate AutoML Tables operators (#39752)``
* ``Resolve deprecation warnings in Azure FileShare-to-GCS tests (#39599)``
* ``typo: wrong OpenLineage facet key in spec (#39782)``
* ``removed stale code from StackdriverTaskHandler (#39744)``

10.18.0
.......

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.


Features
~~~~~~~~

* ``add templated fields for google llm operators (#39174)``
* ``Add logic to handle on_kill for BigQueryInsertJobOperator when deferrable=True (#38912)``
* ``Create 'CloudComposerRunAirflowCLICommandOperator' operator (#38965)``
* ``Deferrable mode for Dataflow sensors (#37693)``
* ``Deferrable mode for Custom Training Job operators (#38584)``
* ``Enhancement for SSL-support in CloudSQLExecuteQueryOperator (#38894)``
* ``Create GKESuspendJobOperator and GKEResumeJobOperator operators (#38677)``
* ``Add support for role arn for aws creds in Google Transfer Service operator (#38911)``
* ``Add encryption_configuration parameter to BigQueryCheckOperator and BigQueryTableCheckOperator (#39432)``
* ``Add 'job_id' parameter to 'BigQueryGetDataOperator' (#39315)``

Bug Fixes
~~~~~~~~~

* ``Fix deferrable mode for DataflowTemplatedJobStartOperator and DataflowStartFlexTemplateOperator (#39018)``
* ``Fix batching for BigQueryToPostgresOperator (#39233)``
* ``Fix DataprocSubmitJobOperator in deferrable mode=True when task is marked as failed. (#39230)``
* ``Fix GCSObjectExistenceSensor operator to return the same XCOM value in deferrable and non-deferrable mode (#39206)``
* ``Fix conn_id BigQueryToMsSqlOperator (#39171)``
* ``Fix add retry logic in case of google auth refresh credential error (#38961)``
* ``Fix BigQueryCheckOperator skipped value and error check in deferrable mode (#38408)``
* ``Fix Use prefixes instead of all file paths for OpenLineage datasets in GCSDeleteObjectsOperator (#39059)``
* ``Fix Use prefixes instead of full file paths for OpenLineage datasets in GCSToGCSOperator (#39058)``
* ``Fix OpenLineage datasets in GCSTimeSpanFileTransformOperator (#39064)``
* ``Fix generation temp filename in 'DataprocSubmitPySparkJobOperator' (#39498)``
* ``Fix logic to cancel the external job if the TaskInstance is not in a running or deferred state for DataprocSubmitJobOperator (#39447)``
* ``Fix logic to cancel the external job if the TaskInstance is not in a running or deferred state for BigQueryInsertJobOperator (#39442)``
* ``Fix logic to cancel the external job if the TaskInstance is not in a running or deferred state for DataprocCreateClusterOperator (#39446)``
* ``Fix 'DataprocCreateBatchOperator' with 'result_retry' raises 'AttributeError' (#39462)``
* ``Fix yaml parsing for GKEStartKueueInsideClusterOperator (#39234)``
* ``Fix validation of label values in BigQueryInsertJobOperator (#39568)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``Improve 'DataprocCreateClusterOperator' Triggers for Better Error Handling and Resource Cleanup (#39130)``
* ``Adding MSGraphOperator in Microsoft Azure provider (#38111)``
* ``Apply PROVIDE_PROJECT_ID mypy workaround across Google provider (#39129)``
* ``handle KubernetesDeleteJobOperator import (#39036)``
* ``Remove Airflow 2.6 back compact code (#39558)``
* ``Reapply templates for all providers (#39554)``
* ``Faster 'airflow_version' imports (#39552)``
* ``Add deprecation warnings and raise exception for already deprecated ones (#38673)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``Disconnect GKE operators from deprecated hooks (#39434)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Activate RUF019 that checks for unnecessary key check (#38950)``
   * ``Prepare docs 1st wave May 2024 (#39328)``

10.17.0
.......

Features
~~~~~~~~

* ``Add 'impersonation_scopes' to BigQuery (#38169)``
* ``Add the deferrable mode to RunPipelineJobOperator (#37969)``
* ``Add GKECreateCustomResourceOperator and GKEDeleteCustomResourceOperator operators (#37616)``
* ``Add VertexAI Language Model and Multimodal Model Operators for Google Cloud Generative AI use (#37721)``
* ``Add GKEListJobsOperator and GKEDescribeJobOperator (#37598)``
* ``Create GKEStartKueueJobOperator operator (#37477)``
* ``Create DeleteKubernetesJobOperator and GKEDeleteJobOperator operators (#37793)``
* ``Update GCS hook to get crc32c hash for CMEK-protected objects (#38191)``
* ``Set job labels for traceability in BigQuery jobs (#37736)``
* ``Deferrable mode for CreateBatchPredictionJobOperator (#37818)``

Bug Fixes
~~~~~~~~~

* ``Fix BigQuery connection and add docs (#38430)``
* ``fix(google,log): Avoid log name overriding (#38071)``
* ``Fix credentials error for S3ToGCSOperator trigger (#37518)``
* ``Fix 'parent_model' parameter in GCP Vertex AI AutoML and Custom Job operators (#38417)``
* ``fix(google): add return statement to yield within a while loop in triggers (#38394)``
* ``Fix cursor unique name surpasses Postgres identifier limit in 'PostgresToGCSOperator' (#38040)``
* ``Fix gcs Anonymous user issue because none token (#38102)``
* ``Fix BigQueryTablePartitionExistenceTrigger partition query (#37655)``

Misc
~~~~

* ``Add google-cloud-bigquery as explicit google-provider dependency (#38753)``
* ``Avoid to use 'functools.lru_cache' in class methods in 'google' provider (#38652)``
* ``Refactor GKE hooks (#38404)``
* ``Remove unused loop variable from airflow package (#38308)``
* ``templated fields logic checks for cloud_storage_transfer_service (#37519)``
* ``Rename mlengine's operators' fields' names to comply with templated fields validation (#38053)``
* ``Rename Vertex AI AutoML operators fields' names to comply with templated fields validation (#38049)``
* ``Rename 'DeleteCustomTrainingJobOperator''s fields' names to comply with templated fields validation (#38048)``
* ``Restore delegate_to for Google Transfer Operators retrieving from Google Cloud. (#37925)``
* ``Refactor CreateHyperparameterTuningJobOperator (#37938)``
* ``Upgrade google-ads version (#37787)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``fix: try002 for provider google (#38803)``
   * ``Revert "Delete deprecated AutoML operators and deprecate AutoML hook and links (#38418)" (#38633)``
   * ``Implement deferrable mode for GKEStartJobOperator (#38454)``
   * ``Delete deprecated AutoML operators and deprecate AutoML hook and links (#38418)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``

10.16.0
.......

Features
~~~~~~~~

* ``'CloudRunExecuteJobOperator': Add project_id to hook.get_job calls (#37201)``
* ``Add developer token as authentication method to GoogleAdsHook (#37417)``
* ``Add GKEStartKueueInsideClusterOperator (#37072)``
* ``Add optional 'location' parameter to the BigQueryInsertJobTrigger (#37282)``
* ``feat(GKEPodAsyncHook): use async credentials token implementation (#37486)``
* ``Create GKEStartJobOperator and KubernetesJobOperator (#36847)``

Bug Fixes
~~~~~~~~~

* ``Fix invalid deprecation of 'DataFusionPipelineLinkHelper' (#37755)``
* ``fix templated field assignment 'google/cloud/operators/compute.py' (#37659)``
* ``fix bq_to_mysql init checks (#37653)``
* ``Fix Async GCSObjectsWithPrefixExistenceSensor xcom push (#37634)``
* ``Fix GCSSynchronizeBucketsOperator timeout error (#37237)``
* ``fix: Signature of insert_rows incompatible with supertype DbApiHook (#37391)``
* ``Use offset-naive datetime in _CredentialsToken (#37539)``
* ``Use wait_for_operation in DataprocInstantiateInlineWorkflowTemplateOperator (#37145)``

Misc
~~~~

* ``Fix typo on DataflowStartFlexTemplateOperator documentation (#37595)``
* ``Make 'executemany' keyword arguments only in 'DbApiHook.insert_rows' (#37840)``
* ``Unify 'aws_conn_id' type to always be 'str | None' (#37768)``
* ``Limit 'pandas' to '<2.2' (#37748)``
* ``Remove broken deprecated fallback into the Google provider operators (#37740)``
* ``Implement AIP-60 Dataset URI formats (#37005)``
* ``resolve template fields init checks for 'bigquery' (#37586)``
* ``Update docs for the DataprocCreateBatchOperator (#37562)``
* ``Replace usage of 'datetime.utcnow' and 'datetime.utcfromtimestamp' in providers (#37138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Add D401 fixes (#37348)``
   * ``Avoid to use too broad 'noqa' (#37862)``
   * ``Avoid non-recommended usage of logging (#37792)``

10.15.0
.......

Features
~~~~~~~~

* ``add service_file support to GKEPodAsyncHook (#37081)``
* ``Update GCP Dataproc ClusterGenerator to support GPU params (#37036)``
* ``Create DataprocStartClusterOperator and DataprocStopClusterOperator (#36996)``
* ``Implement deferrable mode for CreateHyperparameterTuningJobOperator (#36594)``
* ``Enable '_enable_tcp_keepalive' functionality for GKEPodHook (#36999)``

Bug Fixes
~~~~~~~~~

* ``fix(providers/google): fix how GKEPodAsyncHook.service_file_as_context is used (#37306)``
* ``Fix metadata override for ComputeEngineSSHHook (#37192)``
* ``Fix assignment of template field in '__init__' in 'custom_job' (#36789)``
* ``Fix location requirement in DataflowTemplatedJobStartOperator (#37069)``
* ``Fix assignment of template field in '__init__' in 'CloudDataTransferServiceCreateJobOperator' (#36909)``
* ``Fixed the hardcoded default namespace value for GCP Data Fusion links. (#35379)``
* ``Do not ignore the internal_ip_only if set to false in Dataproc cluster config (#37014)``

Misc
~~~~

* ``Revert protection against back-compatibility issue with google-core-api (#37111)``
* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 lint fixes for google provider (#37304)``
   * ``D401 lint fixes for all hooks in google provider (#37296)``
   * ``Upgrade mypy to 1.8.0 (#36428)``

10.14.0
.......

.. note::
  The default value of ``parquet_row_group_size`` in ``BaseSQLToGCSOperator`` has changed from 1 to
  100000, in order to have a default that provides better compression efficiency and performance of
  reading the data in the output Parquet files. In many cases, the previous value of 1 resulted in
  very large files, long task durations and out of memory issues. A default value of 100000 may require
  more memory to execute the operator, in which case users can override the ``parquet_row_group_size``
  parameter in the operator. All operators that are derived from ``BaseSQLToGCSOperator`` are affected
  when ``export_format`` is ``parquet``: ``MySQLToGCSOperator``, ``PrestoToGCSOperator``,
  ``OracleToGCSOperator``, ``TrinoToGCSOperator``, ``MSSQLToGCSOperator`` and ``PostgresToGCSOperator``. Due to the above we treat this change as bug fix.


Features
~~~~~~~~

* ``Add templated fields to 'BigQueryToSqlBaseOperator' from 'BigQueryToPostgresOperator' (#36663)``
* ``Added Check for Cancel Workflow Invocation and added new Query Workflow Invocation operator (#36351)``
* ``Implement Google Analytics Admin (GA4) operators (#36276)``
* ``Add operator to diagnose cluster (#36899)``
* ``Add scopes into a GCP token (#36974)``
* ``feat: full support for google credentials in gcloud-aio clients (#36849)``

Bug Fixes
~~~~~~~~~

* ``fix templating field to super constructor (#36934)``
* ``fix: respect connection ID and impersonation in GKEStartPodOperator (#36861)``
* ``Fix stacklevel in warnings.warn into the providers (#36831)``
* ``Fix deprecations into the GCP Dataproc links (#36834)``
* ``fix assignment of templated field in constructor (#36603)``
* ``Check cluster state before defer Dataproc operators to trigger (#36892)``
* ``prevent templated field logic checks in operators __init__ (#36489)``
* ``Preserve ASCII control characters directly through the BigQuery load API (#36533)``
* ``Change default 'parquet_row_group_size' in 'BaseSQLToGCSOperator' (#36817)``
* ``Fix google operators handling of impersonation chain (#36903)``

Misc
~~~~

* ``style(providers/google): improve BigQueryInsertJobOperator type hinting (#36894)``
* ``Deprecate AutoMLTrainModelOperator for Vision and Video (#36473)``
* ``Remove backward compatibility check for KubernetesPodOperator module (#36724)``
* ``Remove backward compatibility check for KubernetesPodTrigger module (#36721)``
* ``Set min pandas dependency to 1.2.5 for all providers and airflow (#36698)``
* ``remove unnecessary templated field (#36491)``
* ``docs(providers/google): reword GoogleBaseHookAsync as GoogleBaseAsyncHook in docstring (#36946)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Run mypy checks for full packages in CI (#36638)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

10.13.1
.......

Misc
~~~~

* ``Remove backcompat code for stackdriver (#36442)``
* ``Remove unused '_parse_version' function (#36450)``
* ``Remove remaining Airflow 2.5 backcompat code from GCS Task Handler (#36443) (#36457)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Remove remaining Airflow 2.5 backcompat code from GCS Task Handler (#36443)" (#36453)``
   * ``Remove remaining Airflow 2.5 backcompat code from GCS Task Handler (#36443)``
   * ``Revert "Remove remaining Airflow 2.5 backcompat code from Google Provider (#36366)" (#36440)``

10.13.0
.......

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.


Features
~~~~~~~~

* ``GCP Secrets Backend Impersonation (#36072)``
* ``Add OpenLineage support to GcsOperators - Delete, Transform and TimeSpanTransform (#35838)``
* ``Add support for service account impersonation with computeEngineSSHHook (google provider) and IAP tunnel (#35136)``
* ``Add Datascan Profiling (#35696)``
* ``Add overrides to template fields of Google Cloud Run Jobs Execute Operator (#36133)``
* ``Implement deferrable mode for BeamRunJavaPipelineOperator (#36122)``
* ``Add ability to run streaming Job for BeamRunPythonPipelineOperator in non deferrable mode (#36108)``
* ``Add use_glob to GCSObjectExistenceSensor (#34137)``


Bug Fixes
~~~~~~~~~

* ``Fix DataprocSubmitJobOperator to retrieve failed job error message (#36053)``
* ``Fix CloudRunExecuteJobOperator not able to retrieve the Cloud Run job status in deferrable mode (#36012)``
* ``Fix gcs listing - ensure blobs are loaded (#34919)``
* ``allow multiple elements in impersonation chain (#35694)``
* ``Change retry type for Google Dataflow Client to async one (#36141)``
* ``Minor fix to DataprocCreateClusterOperator operator docs. (#36322)``
* ``fix(bigquery.py): pass correct project_id to triggerer (#35200)``
* ``iterate through blobs before checking prefixes (#36202)``
* ``Fix incompatibility with google-cloud-monitoring 2.18.0 (#36200)``
   * ``Update 'retry' param typing in PubSubAsyncHook (#36198)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``
* ``Deprecate 'CloudComposerEnvironmentSensor' in favor of 'CloudComposerCreateEnvironmentOperator' with defer mode (#35775)``
* ``Follow BaseHook connection fields method signature in child classes (#36086)``
* ``Allow storage options to be passed (#35820)``
* ``Add feature to build "chicken-egg" packages from sources (#35890)``
* ``Remove remaining Airflow 2.5 backcompat code from Google Provider (#36366)``
* ``Move KubernetesPodTrigger hook to a cached property (#36290)``
* ``Add code snippet formatting in docstrings via Ruff (#36262)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade to latest pre-commit plugins (#36163)``
   * ``Review and mark found potential SSH security issues by bandit (#36162)``
   * ``Prepare docs 1st wave of Providers December 2023 (#36112)``
   * ``Prepare docs 1st wave of Providers December 2023 RC2 (#36190)``

10.12.0
.......

Features
~~~~~~~~

* ``added Topic params for schema_settings and message_retention_duration. (#35767)``
* ``Add OpenLineage support to GCSToBigQueryOperator (#35778)``
* ``Add OpenLineage support to BigQueryToGCSOperator (#35660)``
* ``Add support for driver pool, instance flexibility policy, and min_num_instances for Dataproc (#34172)``
* ``Add "NON_PREEMPTIBLE" as a valid preemptibility type for Dataproc workers (#35669)``
* ``Add ability to pass impersonation_chain to BigQuery triggers (#35629)``
* ``Add a filter for local files in GoogleDisplayVideo360CreateQueryOperator (#35635)``
* ``Extend task context logging support for remote logging using GCP GCS (#32970)``

Bug Fixes
~~~~~~~~~

* ``Fix and reapply templates for provider documentation (#35686)``
* ``Fix the logic of checking dataflow job state (#34785)``

Misc
~~~~

* ``Remove usage of deprecated method from BigQueryToBigQueryOperator (#35605)``
* ``Check attr on parent not self re TaskContextLogger set_context (#35780)``
* ``Remove backcompat with Airflow 2.3/2.4 in providers (#35727)``
* ``Restore delegate_to param in GoogleDiscoveryApiHook (#35728)``
* ``Remove usage of deprecated methods from BigQueryCursor (#35606)``
* ``Align documentation of 'MSSQLToGCSOperator' (#35715)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use reproducible builds for providers (#35693)``

10.11.1
.......

Misc
~~~~

* ``Update Google Ads API version from v14 to v15 (#35295)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch from Black to Ruff formatter (#35287)``

10.11.0
.......

Features
~~~~~~~~

* ``AIP-58: Add Airflow ObjectStore (AFS) (#34729)``
* ``Improve Dataprep hook (#34880)``

Misc
~~~~

* ``Added 'overrides' parameter to CloudRunExecuteJobOperator (#34874)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Update gcs.py Create and List comment Examples (#35028)``
   * ``Upgrade pre-commits (#35033)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``

10.10.1
.......

Misc
~~~~

* ``Add links between documentation related to Google Cloud Storage (#34994)``
* ``Migrate legacy version of AI Platform Prediction to VertexAI (#34922)``
* ``Cancel workflow in on_kill in DataprocInstantiate{Inline}WorkflowTemplateOperator (#34957)``

10.10.0
.......

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.


Features
~~~~~~~~

* ``improvement: introduce project_id in BigQueryIntervalCheckOperator (#34573)``

Bug Fixes
~~~~~~~~~

* ``respect soft_fail argument when exception is raised for google sensors (#34501)``
* ``Fix GCSToGoogleDriveOperator and gdrive system tests (#34545)``
* ``Fix LookerHook serialize missing 1 argument error (#34678)``
* ``Fix Dataform system tests (#34329)``

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Refactor DataFusionInstanceLink usage (#34514)``
* ``Use 'airflow.models.dag.DAG' in Google Provider examples (#34614)``
* ``Deprecate Life Sciences Operator and Hook (#34549)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor: consolidate import time in providers (#34402)``
   * ``Refactor usage of str() in providers (#34320)``
   * ``Refactor dedent nested loops (#34409)``
   * ``Refactor multiple equals to contains in providers (#34441)``
   * ``Refactor: reduce some conditions in providers (#34440)``
   * ``Refactor shorter defaults in providers (#34347)``
   * ``Update Vertex AI system tests (#34364)``
   * ``Fix typo in DataplexGetDataQualityScanResultOperator (#34681)``

10.9.0
......

Features
~~~~~~~~

* ``Add explicit support of stream (realtime) pipelines for CloudDataFusionStartPipelineOperator (#34271)``
* ``Add 'expected_terminal_state' parameter to Dataflow operators (#34217)``

Bug Fixes
~~~~~~~~~

* ``Fix 'ComputeEngineInsertInstanceOperator' doesn't respect jinja-templated instance name when given in body argument (#34171)``
* ``fix: BigQuery job error message (#34208)``
* ``GKEPodHook ignores gcp_conn_id parameter. (#34194)``

Misc
~~~~

* ``Bump min common-sql provider version for Google provider (#34257)``
* ``Remove unnecessary call to keys() method on dictionaries (#34260)``
* ``Refactor: Think positively in providers (#34279)``
* ``Refactor: Simplify code in providers/google (#33229)``
* ``Refactor: Simplify comparisons (#34181)``
* ``Deprecate AutoMLTrainModelOperator for NL (#34212)``
* ``Simplify  to bool(...) (#34258)``
* ``Make Google Dataform operators templated_fields more consistent (#34187)``

10.8.0
......


Features
~~~~~~~~

* ``Add deferrable mode to Dataplex DataQuality. (#33954)``
* ``allow impersonation_chain to be set on Google Cloud connection (#33715)``

Bug Fixes
~~~~~~~~~

* ``fix(providers/google-marketing-platform): respect soft_fail argument when exception is raised (#34165)``
* ``fix: docstring in endpoint_service.py (#34135)``
* ``Fix BigQueryValueCheckOperator deferrable mode optimisation (#34018)``
* ``Dynamic setting up of artifact versions for Datafusion pipelines (#34068)``
* ``Early delete a Dataproc cluster if started in the ERROR state. (#33668)``
* ``Avoid blocking event loop when using DataFusionAsyncHook by replacing sleep by asyncio.sleep (#33756)``

Misc
~~~~

* ``Consolidate importing of os.path.* (#34060)``
* ``Refactor regex in providers (#33898)``
* ``Move the try outside the loop when this is possible in Google provider (#33976)``
* ``Combine similar if logics in providers (#33987)``
* ``Remove useless string join from providers (#33968)``
* ``Update Azure fileshare hook to use azure-storage-file-share instead of azure-storage-file (#33904)``
* ``Refactor unneeded  jumps in providers (#33833)``
* ``replace loop by any when looking for a positive value in providers (#33984)``
* ``Replace try - except pass by contextlib.suppress in providers (#33980)``
* ``Remove some useless try/except from providers code (#33967)``
* ``Replace sequence concatenation by unpacking in Airflow providers (#33933)``
* ``Remove a deprecated option from 'BigQueryHook.get_pandas_df' (#33819)``
* ``replace unnecessary dict comprehension by dict() in providers (#33857)``
* ``Improve modules import in google provider by move some of them into a type-checking block (#33783)``
* ``Use a single  statement with multiple contexts instead of nested  statements in providers (#33768)``
* ``Use literal dict instead of calling dict() in providers (#33761)``
* ``remove unnecessary and rewrite it using list in providers (#33763)``
* ``Refactor: Simplify a few loops (#33736)``
* ``E731: replace lambda by a def method in Airflow providers (#33757)``
* ``Use f-string instead of  in Airflow providers (#33752)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``fix google CHANGELOG.rst (#34007)``
   * ``Fix Google 10.7.0 changelog (#33953)``
   * ``Fix Cloud Worflows system test (#33386)``
   * ``fix entry in Google provider CHANGELOG.rst (#33890)``
   * ``Generate Python API docs for Google ADS (#33814)``

10.7.0
......

Features
~~~~~~~~

* ``Add CloudRunHook and operators (#33067)``
* ``Add 'CloudBatchHook' and operators (#32606)``
* ``Adding Support for Google Cloud's Data Pipelines Run Operator (#32846)``
* ``Add parameter sftp_prefetch to SFTPToGCSOperator (#33274)``
* ``Add Google Cloud's Data Pipelines Create Operator (#32843)``
* ``Add Dataplex Data Quality operators. (#32256)``

Bug Fixes
~~~~~~~~~

* ``Fix BigQueryCreateExternalTableOperator when using a foramt different to CSV (#33540)``
* ``Fix DataplexDataQualityJobStatusSensor and add unit tests (#33440)``
* ``Avoid importing pandas and numpy in runtime and module level (#33483)``

Misc
~~~~

* ``Add missing template fields to DataformCreateCompilationResultOperator (#33585)``
* ``Consolidate import and usage of pandas (#33480)``
* ``Import utc from datetime and normalize its import (#33450)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   ``Refactor: Use random.choices (#33631)``
   ``Further improvements for provider verification (#33670)``
   ``Refactor: Remove useless str() calls (#33629)``
   ``Refactor: lists and paths in dev (#33626)``
   ``Do not create lists we don't need (#33519)``
   ``Replace strftime with f-strings where nicer (#33455)``
   ``Refactor: Better percentage formatting (#33595)``
   ``Fix typos (double words and it's/its) (#33623)``
   ``Fix system test example_cloud_storage_transfer_service_aws (#33429)``
   ``Enable D205 Support (#33398)``
   ``Update Error details for Generic Error Code  (#32847)``
   ``D205 Support - Providers - Final Pass (#33303)``

10.6.0
......

Features
~~~~~~~~

* ``openlineage, bigquery: add openlineage method support for BigQueryExecuteQueryOperator (#31293)``
* ``Add GCS Requester Pays bucket support to GCSToS3Operator (#32760)``
* ``Add system test and docs for CloudDataTransferServiceGCSToGCSOperator (#32960)``
* ``Add a new parameter to SQL operators to specify conn id field (#30784)``

Bug Fixes
~~~~~~~~~

* ``Fix 'DataFusionAsyncHook' catch 404 (#32855)``
* ``Fix system test for MetastoreHivePartitionSensor (#32861)``
* ``Fix catching 409 error (#33173)``
* ``make 'sql' a cached property in 'BigQueryInsertJobOperator' (#33218)``

Misc
~~~~

* ``refactor(providers.google): use module level __getattr__ for DATAPROC_JOB_LOG_LINK to DATAPROC_JOB_LINK and add deprecation warning (#33189)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Get rid of Python2 numeric relics (#33050)``
   * ``Refactor of links in Dataproc. (#31895)``
   * ``Handle multiple connections using exceptions (#32365)``
   * ``openlineage,gcs: use proper name for openlineage methods (#32956)``
   * ``Fix DataflowStartSqlJobOperator system test (#32823)``
   * ``Alias 'DATAPROC_JOB_LOG_LINK' to 'DATAPROC_JOB_LINK' (#33148)``
   * ``Prepare docs for Aug 2023 1st wave of Providers (#33128)``
   * ``Prepare docs for RC2 providers (google, redis) (#33185)``

10.5.0
......

Features
~~~~~~~~

* ``openlineage, gcs: add openlineage methods for GcsToGcsOperator (#31350)``
* ``Add Spot Instances support with Dataproc Operators (#31644)``
* ``Install sqlalchemy-spanner package into Google provider (#31925)``
* ``Filtering and ordering results of DataprocListBatchesOperator (#32500)``

Bug Fixes
~~~~~~~~~

* ``Fix BigQueryGetDataOperator where project_id is not being respected in deferrable mode (#32488)``
* ``Refresh GKE OAuth2 tokens (#32673)``
* ``Fix 'BigQueryInsertJobOperator' not exiting deferred state (#31591)``

Misc
~~~~

* ``Fixup docstring for deprecated DataprocSubmitSparkJobOperator and refactoring system tests (#32743)``
* ``Add more accurate typing for DbApiHook.run method (#31846)``
* ``Add deprecation info to the providers modules and classes docstring (#32536)``
* ``Fixup docstring for deprecated DataprocSubmitHiveJobOperator (#32723)``
* ``Fixup docstring for deprecated DataprocSubmitPigJobOperator (#32739)``
* ``Fix Datafusion system tests (#32749)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixup docs and optimize system test for DataprocSubmitJobOperator (Hadoop job) (#32722)``
   * ``Fixup system test for DataprocSubmitJobOperator (SparkSQL job) (#32745)``
   * ``Fixup system test for DataprocSubmitJobOperator (PySpark job) (#32740)``
   * ``Migrate system test for PostgresToGCSOperator to new design AIP-47 (#32641)``
   * ``misc: update MLEngine system tests (#32881)``

10.4.0
......

Features
~~~~~~~~

* ``Implement deferrable mode for S3ToGCSOperator (#29462)``

Bug Fixes
~~~~~~~~~

* ``Bugfix GCSToGCSOperator when copy files to folder without wildcard (#32486)``
* ``Fix 'cache_control' parameter of upload function in 'GCSHook'  (#32440)``
* ``Fix BigQuery transfer operators to respect project_id arguments (#32232)``
* ``Fix the gcp_gcs_delete_objects on empty list (#32383)``
* ``Fix endless loop of defer in cloud_build (#32387)``
* ``Fix GCSToGCSOperator copy without wildcard and exact_match=True (#32376)``

Misc
~~~~

* ``Allow a destination folder to be provided (#31885)``
* ``Moves 'AzureBlobStorageToGCSOperator' from Azure to Google provider (#32306)``
* ``Give better link to job configuration docs in BigQueryInsertJobOperator (#31736)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Provider: Google (#32356)``
   * ``Migrating Google AutoML example_dags to sys tests (#32368)``
   * ``build(pre-commit): check deferrable default value (#32370)``

10.3.0
......

Features
~~~~~~~~

* ``Add 'on_finish_action' to 'KubernetesPodOperator' (#30718)``
* ``Add deferrable mode to CloudSQLExportInstanceOperator (#30852)``
* ``Adding 'src_fmt_configs' to the list of template fields. (#32097)``

Bug Fixes
~~~~~~~~~

* ``[Issue-32069] Fix name format in the batch requests (#32070)``
* ``Fix 'BigQueryInsertJobOperator'  error handling in deferrable mode (#32034)``
* ``Fix 'BIGQUERY_JOB_DETAILS_LINK_FMT' in 'BigQueryConsoleLink' (#31953)``
* ``Make the deferrable version of DataprocCreateBatchOperator handle a batch_id that already exists (#32216)``


Misc
~~~~

* ``Switch Google Ads API version from v13 to v14 (#32028)``
* ``Deprecate 'delimiter' param and source object's wildcards in GCS, introduce 'match_glob' param. (#31261)``
* ``Refactor GKECreateClusterOperator's body validation (#31923)``
* ``Optimize deferrable mode execution for 'BigQueryValueCheckOperator' (#31872)``
* ``Add default_deferrable config (#31712)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve provider documentation and README structure (#32125)``
   * ``Google provider docstring improvements (#31731)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``

10.2.0
......

.. note::
  This release dropped support for Python 3.7

Features
~~~~~~~~

* ``add a return when the event is yielded in a loop to stop the execution (#31985)``
* ``Add deferrable mode to PubsubPullSensor (#31284)``
* ``Add a new param to set parquet row group size in 'BaseSQLToGCSOperator' (#31831)``
* ``Add 'cacheControl' field to google cloud storage (#31338)``
* ``Add 'preserveAsciiControlCharacters' to 'src_fmt_configs' (#31643)``
* ``Add support for credential configuation file auth to Google Secrets Manager secrets backend (#31597)``
* ``Add credential configuration file support to Google Cloud Hook (#31548)``
* ``Add deferrable mode to 'GCSUploadSessionCompleteSensor' (#31081)``
* ``Add append_job_name parameter in DataflowStartFlexTemplateOperator (#31511)``
* ``FIPS environments: Mark uses of md5 as "not-used-for-security" (#31171)``
* ``Implement MetastoreHivePartitionSensor (#31016)``

Bug Fixes
~~~~~~~~~

* ``Bigquery: fix links for already existing tables and datasets. (#31589)``
* ``Provide missing project id and creds for TabularDataset (#31991)``

Misc
~~~~

* ``Optimize deferrable mode execution for 'DataprocSubmitJobOperator' (#31317)``
* ``Optimize deferrable mode execution for 'BigQueryInsertJobOperator' (#31249)``
* ``Remove return statement after yield from triggers class (#31703)``
* ``Replace unicodecsv with standard csv library (#31693)``
* ``Optimize deferrable mode (#31758)``
* ``Remove Python 3.7 support (#30963)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Replace spelling directive with spelling:word-list (#31752)``
   * ``Add D400 pydocstyle check - Google provider only (#31422)``
   * ``Add discoverability for triggers in provider.yaml (#31576)``
   * ``Revert "Fix 'BIGQUERY_JOB_DETAILS_LINK_FMT' in 'BigQueryConsoleLink' (#31457)" (#31935)``
   * ``Fix 'BIGQUERY_JOB_DETAILS_LINK_FMT' in 'BigQueryConsoleLink' (#31457)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

10.1.1
......

Bug Fixes
~~~~~~~~~

* ``Fix accessing a GKE cluster through the private endpoint in 'GKEStartPodOperator' (#31391)``
* ``Fix 'BigQueryGetDataOperator''s query job bugs in deferrable mode (#31433)``

10.1.0
......

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. note::
  This release changed default Google ads to v13.
  Since v12 is deprecated by Google and soon to be removed we are not consider this to be a breaking change in Airflow.

.. note::
  This version of the provider bumped many Google packages.
  Please review packages change logs

Features
~~~~~~~~

* ``Add deferrable mode to DataprocInstantiateInlineWorkflowTemplateOperator (#30878)``
* ``Add deferrable mode to 'GCSObjectUpdateSensor' (#30579)``
* ``Add protocol to define methods relied upon by KubernetesPodOperator (#31298)``
* ``Add BigQueryToPostgresOperator (#30658)``

Bug Fixes
~~~~~~~~~

* ``'DataflowTemplatedJobStartOperator' fix overwriting of location with default value, when a region is provided. (#31082)``
* ``Poke once before defer for GCSObjectsWithPrefixExistenceSensor (#30939)``
* ``Add deferrable mode to 'GCSObjectsWithPrefixExistenceSensor' (#30618)``
* ``allow multiple prefixes in gcs delete/list hooks and operators (#30815)``
* ``Fix removed delegate_to parameter in deferrable GCS sensor (#30810)``


Misc
~~~~

* ``Add 'use_legacy_sql' param to 'BigQueryGetDataOperator' (#31190)``
* ``Add 'as_dict' param to 'BigQueryGetDataOperator' (#30887)``
* ``Add flag apply_gcs_prefix to S3ToGCSOperator (b/245077385) (#31127)``
* ``Add 'priority' parameter to BigQueryHook (#30655)``
* ``Bump minimum Airflow version in providers (#30917)``
* ``implement gcs_schema_object for BigQueryCreateExternalTableOperator (#30961)``
* ``Optimize deferred execution mode (#30946)``
* ``Optimize deferrable mode execution (#30920)``
* ``Optimize deferrable mode in 'GCSObjectExistenceSensor' (#30901)``
* ``'CreateBatchPredictionJobOperator' Add batch_size param for Vertex AI BatchPredictionJob objects (#31118)``
* ``GKEPodHook needs to have all methods KPO calls (#31266)``
* ``Add CloudBuild build id log (#30516)``
* ``Switch default Google ads to v13 (#31382)``
* ``Switch to google ads v13 (#31369)``
* ``Update SDKs for google provider package (#30067)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move TaskInstanceKey to a separate file (#31033)``
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Small refactors in ClusterGenerator of dataproc (#30714)``
   * ``Upgrade ruff to 0.0.262 (#30809)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add cli cmd to list the provider trigger info (#30822)``
   * ``Docstring improvements (#31375)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Add get_namespace to GKEPodHook (#31397)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

10.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

Google has announced sunset of Campaign Manager 360 v3.5 by Apr 20, 2023. For more information
please check: `<https://developers.google.com/doubleclick-advertisers/deprecation>`_ . As a result, the
default api version for Campaign Manager 360 operator was updated to the latest v4 version.

.. warning::
  In this version of the provider, deprecated ``delegate_to`` param is removed from all GCP operators, hooks, and triggers, as well as from firestore and gsuite
  transfer operators that interact with GCS. Impersonation can be achieved instead by utilizing the ``impersonation_chain`` param.
  The ``delegate_to`` param will still be available only in gsuite and marketing platform hooks and operators, that don't interact with Google Cloud.

* ``remove delegate_to from GCP operators and hooks (#30748)``
* ``Update Google Campaign Manager360 operators to use API v4 (#30598)``

Bug Fixes
~~~~~~~~~

* ``Update DataprocCreateCluster operator to use 'label' parameter properly (#30741)``

Misc
~~~~

* ``add missing project_id in BigQueryGetDataOperator (#30651)``
* ``Display Video 360 cleanup v1 API usage (#30577)``

9.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Google  announced sunset of Bid manager API v1 and v1.1 by April 27, 2023 for more information
please check: `docs <https://developers.google.com/bid-manager/v1.1>`_  As a result default value of api_version
in GoogleDisplayVideo360Hook and related operators updated to v2

This version of provider contains a temporary workaround to issue with ``v11`` version of
google-ads API being discontinued, while the google provider dependencies preventing installing
any google-ads client supporting ``v12`` API. This version contains vendored-in version of google-ads
library ``20.0.0`` v12 support only. The workaround (and vendored-in library) will be removed
as soon as dependencies of the provider will allow to use google-ads supporting newer
API versions of google-ads.

.. note::

  ONLY v12 version of google ads is supported. You should set v12 when your create an operator or client.

* ``Update DV360 operators to use API v2 (#30326)``
* ``Fix dynamic imports in google ads vendored in library (#30544)``
* ``Fix one more dynamic import needed for vendored-in google ads (#30564)``

Features
~~~~~~~~

* ``Add deferrable mode to GKEStartPodOperator (#29266)``

Bug Fixes
~~~~~~~~~

* ``BigQueryHook list_rows/get_datasets_list can return iterator (#30543)``
* ``Fix cloud build async credentials (#30441)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``
   * ``Small quotation fix (#30448)``

8.12.0
......

Features
~~~~~~~~

* ``Add missing 'poll_interval' in Bigquery operator (#30132)``
* ``Add poll_interval param in BigQueryInsertJobOperator (#30091)``
* ``Add 'job_id' to 'BigQueryToGCSOperator' templated_fields (#30006)``
* ``Support deleting the local log files when using remote logging (#29772)``

Bug Fixes
~~~~~~~~~

* ``fix setting project_id for gs to bq and bq to gs (#30053)``
* ``Fix location on cloud build operators (#29937)``
* ``'GoogleDriveHook': Fixing log message + adding more verbose documentation (#29694)``
* ``Add "BOOLEAN" to type_map of MSSQLToGCSOperator, fix incorrect bit->int type conversion by specifying BIT fields explicitly (#29902)``
* ``Google Cloud Providers - Fix _MethodDefault deepcopy failure (#29518)``
* ``Handling project location param on async BigQuery dts trigger (#29786)``
* ``Support CloudDataTransferServiceJobStatusSensor without specifying a project_id (#30035)``
* ``Wait insert_job result in normal mode (#29925)``

Misc
~~~~

* ``merge BigQueryTableExistenceAsyncSensor into BigQueryTableExistenceSensor (#30235)``
* ``Remove  unnecessary upper constraints from google provider (#29915)``
* ``Merge BigQueryTableExistencePartitionAsyncSensor into BigQueryTableExistencePartitionSensor (#30231)``
* ``Merge GCSObjectExistenceAsyncSensor logic to GCSObjectExistenceSensor (#30014)``
* ``Align cncf provider file names with AIP-21 (#29905)``
* ``Switch to using vendored-in google ads. (#30410)``
* ``Merging of the google ads vendored-in code. (#30399)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``adding trigger info to provider yaml (#29950)``

8.11.0
......

Features
~~~~~~~~

* ``Add deferrable mode to BigQueryTablePartitionExistenceSensor. (#29735)``
* ``Add a new param for BigQuery operators to support additional actions when resource exists (#29394)``
* ``Add deferrable mode to DataprocInstantiateWorkflowTemplateOperator (#28618)``
* ``Dataproc batches (#29136)``
* ``Add 'CloudSQLCloneInstanceOperator' (#29726)``

Bug Fixes
~~~~~~~~~

* ``Fix 'NoneType' object is not subscriptable. (#29820)``
* ``Fix and augment 'check-for-inclusive-language' CI check (#29549)``
* ``Don't push secret in XCOM in BigQueryCreateDataTransferOperator (#29348)``

Misc
~~~~

* ``Google Cloud Providers - Introduce GoogleCloudBaseOperator (#29680)``
* ``Update google cloud dlp package and adjust hook and operators (#29234)``
* ``Refactor Dataproc Trigger (#29364)``
* ``Remove <2.0.0 limit on google-cloud-bigtable (#29644)``
* ``Move help message to the google auth code (#29888)``

8.10.0
......

Features
~~~~~~~~

* ``Add defer mode to GKECreateClusterOperator and GKEDeleteClusterOperator (#28406)``

Bug Fixes
~~~~~~~~~
* ``Move cloud_sql_binary_path from connection to Hook (#29499)``
* ``Check that cloud sql provider version is valid (#29497)``
* ``'GoogleDriveHook': Add folder_id param to upload_file (#29477)``

Misc
~~~~
* ``Add documentation for BigQuery transfer operators (#29466)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade Mypy to 1.0 (#29468)``
   * ``Restore trigger logging (#29482)``
   * ``Revert "Enable individual trigger logging (#27758)" (#29472)``
   * ``Revert "Upgrade mypy to 0.991 (#28926)" (#29470)``
   * ``Upgrade mypy to 0.991 (#28926)``

8.9.0
.....

Features
~~~~~~~~

* ``Add deferrable capability to existing ''DataprocDeleteClusterOperator'' (#29349)``
* ``Add deferrable mode to dataflow operators (#27776)``
* ``Add deferrable mode to DataprocCreateBatchOperator (#28457)``
* ``Add deferrable mode to DataprocCreateClusterOperator and DataprocUpdateClusterOperator (#28529)``
* ``Add deferrable mode to MLEngineStartTrainingJobOperator (#27405)``
* ``Add deferrable mode to DataFusionStartPipelineOperator (#28690)``
* ``Add deferrable mode for Big Query Transfer operator (#27833)``
* ``Add support for write_on_empty in BaseSQLToGCSOperator (#28959)``
* ``Add DataprocCancelOperationOperator (#28456)``
* ``Enable individual trigger logging (#27758)``
* ``Auto ML assets (#25466)``

Bug Fixes
~~~~~~~~~

* ``Fix GoogleDriveHook writing files to trashed folders on upload v2 (#29119)``
* ``fix Google provider CHANGELOG.rst (#29122)``
* ``fix Google provider CHANGELOG.rst (#29114)``
* ``Keyfile dict can be dict not str (#29135)``
* ``GCSTaskHandler may use remote log conn id (#29117)``

Misc
~~~~
* ``Deprecate 'delegate_to' param in GCP operators and update docs (#29088)``

8.8.0
.....

Features
~~~~~~~~

* ``Add deferrable ''GCSObjectExistenceSensorAsync'' (#28763)``
* ``Support partition_columns in BaseSQLToGCSOperator (#28677)``

Bug Fixes
~~~~~~~~~

* ``'BigQueryCreateExternalTableOperator' fix field delimiter not working with csv (#28856)``
* ``Fix using private _get_credentials instead of public get_credentials (#28588)``
* ``Fix'GoogleCampaignManagerReportSensor' with 'QUEUED' status (#28735)``
* ``Fix BigQueryColumnCheckOperator runtime error (#28796)``
* ``assign "datasetReference" attribute to dataset_reference dict. by default if not already set in create_empty_dataset method of bigquery hook (#28782)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch to ruff for faster static checks (#28893)``

8.7.0
.....

Features
~~~~~~~~

* ``Add table_resource to template fields for BigQueryCreateEmptyTableOperator (#28235)``
* ``Add retry param in GCSObjectExistenceSensor (#27943)``
* ``Add preserveAsciiControlCharacters to src_fmt_configs (#27679)``
* ``Add deferrable mode to CloudBuildCreateBuildOperator (#27783)``
* ``GCSToBigQueryOperator allows autodetect None and infers schema (#28564)``
* ``Improve memory usage in Dataproc deferrable operators (#28117)``
* ``Push job_id in xcom for dataproc submit job op (#28639)``

Bug Fixes
~~~~~~~~~

* ``Fix for issue with reading schema fields for JSON files in GCSToBigQueryOperator (#28284)``
* ``Fix GCSToBigQueryOperator not respecting schema_obj (#28444)``
* ``Fix GCSToGCSOperator copying list of objects without wildcard (#28111)``
* ``Fix: re-enable use of parameters in gcs_to_bq which had been disabled (#27961)``
* ``Set bigquery ''use_legacy_sql'' param in job config correctly (#28522)``

Misc
~~~~

* ``Remove 'pylint' messages control instructions (#28555)``
* ``Remove deprecated AIPlatformConsoleLinkk from google/provider.yaml (#28449)``
* ``Use object instead of array in config.yml for config template (#28417)``
* ``[misc] Get rid of 'pass' statement in conditions (#27775)``
* ``Change log level to DEBUG when secret not found for google secret manager (#27856)``
* ``[misc] Replace XOR '^' conditions by 'exactly_one' helper in providers (#27858)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.6.0
.....

Features
~~~~~~~~

* ``Persist DataprocLink for workflow operators regardless of job status (#26986)``
* ``Deferrable mode for BigQueryToGCSOperator (#27683)``
* ``Add Export Format to Template Fields in BigQueryToGCSOperator (#27910)``

Bug Fixes
~~~~~~~~~

* ``Fix to read location parameter properly in BigQueryToBigQueryOperator (#27661)``
* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

8.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``
* ``Rename  hook bigquery function '_bq_cast' to 'bq_cast' (#27543)``
* ``Use non-deprecated method for on_kill in BigQueryHook (#27547)``
* ``Typecast biquery job response col value (#27236)``
* ``Remove <2 limit on google-cloud-storage (#26922)``
* ``Replace urlparse with urlsplit (#27389)``

Features
~~~~~~~~

When defining a connection in environment variables or secrets backend, previously ``extra`` fields
needed to be defined with prefix ``extra__google_cloud_platform__``.  Now this is no longer required.
So for example you may store the keyfile json as ``keyfile_dict`` instead of
``extra__google_cloud_platform__keyfile_dict``.  If both are present, the short name will be preferred.

* ``Add backward compatibility with old versions of Apache Beam (#27263)``
* ``Add deferrable mode to GCSToBigQueryOperator + tests (#27052)``
* ``Add system tests for Vertex AI operators in new approach (#27053)``
* ``Dataform operators, links, update system tests and docs (#27144)``
* ``Allow values in WorkflowsCreateExecutionOperator execution argument to be dicts (#27361)``
* ``DataflowStopJobOperator Operator (#27033)``
* ``Allow for the overriding of stringify_dict for json/jsonb column data type in Postgres #26875 (#26876)``
* ``Allow and prefer non-prefixed extra fields for dataprep hook (#27039)``
* ``Update google hooks to prefer non-prefixed extra fields (#27023)``

Bug Fixes
~~~~~~~~~

* ``Add new Compute Engine Operators and fix system tests (#25608)``
* ``Common sql bugfixes and improvements (#26761)``
* ``Fix delay in Dataproc CreateBatch operator (#26126)``
* ``Remove unnecessary newlines around single arg in signature (#27525)``
* ``set project_id and location when canceling BigQuery job (#27521)``
* ``use the proper key to retrieve the dataflow job_id (#27336)``
* ``Make GSheetsHook return an empty list when there are no values (#27261)``
* ``Cloud ML Engine operators assets (#26836)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Change dataprep system tests assets (#26488)``
   * ``Upgrade dependencies in order to avoid backtracking (#27531)``
   * ``Migration of System Tests: Cloud Composer (AIP-47)  (#27227)``
   * ``Rewrite system tests for ML Engine service (#26915)``
   * ``Migration of System Tests: Cloud BigQuery Data Transfer (AIP-47) (#27312)``
   * ``Migration of System Tests: Dataplex (AIP-47) (#26989)``
   * ``Migration of System Tests: Cloud Vision Operators (AIP-47) (#26963)``
   * ``Google Drive to local - system tests migrations (AIP-47) (#26798)``
   * ``Migrate Bigtable operators system tests according to AIP-47 (#26911)``
   * ``Migrate Dataproc Metastore system tests according to AIP-47 (#26858)``
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Local filesystem to Google Drive Operator - system tests migration (AIP-47) (#26797)``
   * ``SFTP to Google Cloud Storage Transfer system tests migration (AIP-47) (#26799)``

8.4.0
.....

Features
~~~~~~~~

* ``Add BigQuery Column and Table Check Operators (#26368)``
* ``Add deferrable big query operators and sensors (#26156)``
* ``Add 'output' property to MappedOperator (#25604)``
* ``Added append_job_name parameter to DataflowTemplatedJobStartOperator (#25746)``
* ``Adding a parameter for exclusion of trashed files in GoogleDriveHook (#25675)``
* ``Cloud Data Loss Prevention Operators assets (#26618)``
* ``Cloud Storage Transfer Operators assets & system tests migration (AIP-47) (#26072)``
* ``Merge deferrable BigQuery operators to exisitng one (#26433)``
* ``specifying project id when calling wait_for_operation in delete/create cluster (#26418)``
* ``Auto tail file logs in Web UI (#26169)``
* ``Cloud Functions Operators assets & system tests migration (AIP-47) (#26073)``
* ``GCSToBigQueryOperator Resolve 'max_id_key' job retrieval and xcom return (#26285)``
* ``Allow for the overriding of 'stringify_dict' for json export format on BaseSQLToGCSOperator (#26277)``
* ``Append GoogleLink base in the link class (#26057)``
* ``Cloud Video Intelligence Operators assets & system tests migration (AIP-47) (#26132)``
* ``Life Science assets & system tests migration (AIP-47) (#25548)``
* ``GCSToBigQueryOperator allow for schema_object in alternate GCS Bucket (#26190)``
* ``Use AsyncClient for Composer Operators in deferrable mode (#25951)``
* ``Use project_id to get authenticated client (#25984)``
* ``Cloud Build assets & system tests migration (AIP-47) (#25895)``
* ``Dataproc submit job operator async (#25302)``
* ``Support project_id argument in BigQueryGetDataOperator (#25782)``

Bug Fixes
~~~~~~~~~

* ``Fix JSONDecodeError in Datafusion operators (#26202)``
* ``Fixed never ending loop to in CreateWorkflowInvocation (#25737)``
* ``Update gcs.py (#26570)``
* ``Don't throw an exception when a BQ cusor job has no schema (#26096)``
* ``Google Cloud Tasks Sensor for queue being empty (#25622)``
* ``Correcting the transfer config name. (#25719)``
* ``Fix parsing of optional 'mode' field in BigQuery Result Schema (#26786)``
* ``Fix MaxID logic for GCSToBigQueryOperator (#26768)``

Misc
~~~~

* ``Sql to GSC operators update docs for parquet format (#25878)``
* ``Limit Google Protobuf for compatibility with biggtable client (#25886)``
* ``Make GoogleBaseHook credentials functions public (#25785)``
* ``Consolidate to one 'schedule' param (#25410)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Data Loss Prevention system tests according to AIP-47 (#26060)``
   * ``Google Drive to Google Cloud Storage Transfer Operator - system tests migration (AIP-47) (#26487)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to core airflow (#26290)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``Replace SQL with Common SQL in pre commit (#26058)``
   * ``Hook into Mypy to get rid of those cast() (#26023)``
   * ``Work around pyupgrade edge cases (#26384)``
   * ``D400 first line should end with period batch02 (#25268)``
   * ``Fix GCS sensor system tests failing with DebugExecutor (#26742)``
   * ``Update docs for September Provider's release (#26731)``

8.3.0
.....

Features
~~~~~~~~

* ``add description method in BigQueryCursor class (#25366)``
* ``Add project_id as a templated variable in two BQ operators (#24768)``
* ``Remove deprecated modules in Amazon provider (#25543)``
* ``Move all "old" SQL operators to common.sql providers (#25350)``
* ``Improve taskflow type hints with ParamSpec (#25173)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``
* ``Bump typing-extensions and mypy for ParamSpec (#25088)``
* ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``
* ``Dataform operators (#25587)``

Bug Fixes
~~~~~~~~~

* ``Fix GCSListObjectsOperator docstring (#25614)``
* ``Fix BigQueryInsertJobOperator cancel_on_kill (#25342)``
* ``Fix BaseSQLToGCSOperator approx_max_file_size_bytes (#25469)``
* ``Fix PostgresToGCSOperat bool dtype (#25475)``
* ``Fix Vertex AI Custom Job training issue (#25367)``
* ``Fix Flask Login user setting for Flask 2.2 and Flask-Login 0.6.2 (#25318)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Google example trino_to_gcs to new design AIP-47 (#25420)``
   * ``Migrate Google example automl_nl_text_extraction to new design AIP-47 (#25418)``
   * ``Memorystore assets & system tests migration (AIP-47) (#25361)``
   * ``Translate system tests migration (AIP-47) (#25340)``
   * ``Migrate Google example life_sciences to new design AIP-47 (#25264)``
   * ``Migrate Google example natural_language to new design AIP-47 (#25262)``
   * ``Delete redundant system test bigquery_to_bigquery (#25261)``
   * ``Migrate Google example bigquery_to_mssql to new design AIP-47 (#25174)``
   * ``Migrate Google example compute_igm to new design AIP-47 (#25132)``
   * ``Migrate Google example automl_vision to new design AIP-47 (#25152)``
   * ``Migrate Google example gcs_to_sftp to new design AIP-47 (#25107)``
   * ``Migrate Google campaign manager example to new design AIP-47 (#25069)``
   * ``Migrate Google analytics example to new design AIP-47 (#25006)``

8.2.0
.....

Features
~~~~~~~~

* ``PubSub assets & system tests migration (AIP-47) (#24867)``
* ``Add handling state of existing Dataproc batch (#24924)``
* ``Add links for Google Kubernetes Engine operators (#24786)``
* ``Add test_connection method to 'GoogleBaseHook' (#24682)``
* ``Add gcp_conn_id argument to GoogleDriveToLocalOperator (#24622)``
* ``Add DeprecationWarning for column_transformations parameter in AutoML (#24467)``
* ``Modify BigQueryCreateExternalTableOperator to use updated hook function (#24363)``
* ``Move all SQL classes to common-sql provider (#24836)``
* ``Datacatalog assets & system tests migration (AIP-47) (#24600)``
* ``Upgrade FAB to 4.1.1 (#24399)``

Bug Fixes
~~~~~~~~~

* ``GCSDeleteObjectsOperator empty prefix bug fix (#24353)``
* ``perf(BigQuery): pass table_id as str type (#23141)``
* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Google sheets example to new design AIP-47 (#24975)``
   * ``Migrate Google ads example to new design AIP-47 (#24941)``
   * ``Migrate Google example gcs_to_gdrive to new design AIP-47 (#24949)``
   * ``Migrate Google firestore example to new design AIP-47 (#24830)``
   * ``Automatically detect if non-lazy logging interpolation is used (#24910)``
   * ``Migrate Google example sql_to_sheets to new design AIP-47 (#24814)``
   * ``Remove "bad characters" from our codebase (#24841)``
   * ``Migrate Google example DAG mssql_to_gcs to new design AIP-47 (#24541)``
   * ``Align Black and blacken-docs configs (#24785)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Use our yaml util in all providers (#24720)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``
   * ``Migrate Google example DAG s3_to_gcs to new design AIP-47 (#24641)``
   * ``Migrate Google example DAG bigquery_transfer to new design AIP-47 (#24543)``
   * ``Migrate Google example DAG oracle_to_gcs to new design AIP-47 (#24542)``
   * ``Migrate Google example DAG mysql_to_gcs to new design AIP-47 (#24540)``
   * ``Migrate Google search_ads DAG to new design AIP-47 (#24298)``
   * ``Migrate Google gcs_to_sheets DAG to new design AIP-47 (#24501)``

8.1.0
.....

Features
~~~~~~~~

* ``Update Oracle library to latest version (#24311)``
* ``Expose SQL to GCS Metadata (#24382)``

Bug Fixes
~~~~~~~~~

* ``fix typo in google provider additional extras (#24431)``
* ``Use insert_job in the BigQueryToGCPOpertor and adjust links (#24416)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix links to sources for examples (#24386)``
   * ``Deprecate remaining occurrences of 'bigquery_conn_id' in favor of 'gcp_conn_id' (#24376)``
   * ``Migrate Google calendar example DAG to new design AIP-47 (#24333)``
   * ``Migrate Google azure_fileshare example DAG to new design AIP-47 (#24349)``
   * ``Remove bigquery example already migrated to AIP-47 (#24379)``
   * ``Migrate Google sheets example DAG to new design AIP-47 (#24351)``

8.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add key_secret_project_id parameter which specifies a project with KeyFile (#23930)``
* ``Added impersonation_chain for DataflowStartFlexTemplateOperator and DataflowStartSqlJobOperator (#24046)``
* ``Add fields to CLOUD_SQL_EXPORT_VALIDATION. (#23724)``
* ``Update credentials when using ADC in Compute Engine (#23773)``
* ``set color to operators in cloud_sql.py (#24000)``
* ``Sql to gcs with exclude columns (#23695)``
* ``[Issue#22846] allow option to encode or not encode UUID when uploading from Cassandra to GCS (#23766)``
* ``Workflows assets & system tests migration (AIP-47) (#24105)``
* ``Spanner assets & system tests migration (AIP-47) (#23957)``
* ``Speech To Text assets & system tests migration (AIP-47) (#23643)``
* ``Cloud SQL assets & system tests migration (AIP-47) (#23583)``
* ``Cloud Storage assets & StorageLink update (#23865)``

Bug Fixes
~~~~~~~~~

* ``fix BigQueryInsertJobOperator (#24165)``
* ``Fix the link to google workplace (#24080)``
* ``Fix DataprocJobBaseOperator not being compatible with dotted names (#23439). (#23791)``
* ``Remove hack from BigQuery DTS hook (#23887)``
* ``Fix GCSToGCSOperator cannot copy a single file/folder without copying other files/folders with that prefix (#24039)``
* ``Workaround job race bug on biguery to gcs transfer (#24330)``

Misc
~~~~

* ``Fix BigQuery system tests (#24013)``
* ``Ensure @contextmanager decorates generator func (#23103)``
* ``Migrate Dataproc to new system tests design (#22777)``
* ``AIP-47 - Migrate google leveldb DAGs to new design ##22447 (#24233)``
* ``Apply per-run log templates to log handlers (#24153)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Introduce 'flake8-implicit-str-concat' plugin to static checks (#23873)``
   * ``Clean up f-strings in logging calls (#23597)``
   * ``pydocstyle D202 added (#24221)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

7.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Remove deprecated parameters from PubSub operators: (#23261)``

* ``Upgrade to support Google Ads v10 (#22965)``

* ``'DataprocJobBaseOperator' changes (#23350)``

  * ``'DataprocJobBaseOperator': order of parameters has changed.``
  * ``'region' parameter has no default value affected functions/classes: 'DataprocHook.cancel_job' 'DataprocCreateClusterOperator' 'DataprocJobBaseOperator'``

* ``'DatastoreHook': Remove 'datastore_conn_id'. Please use 'gcp_conn_id' (#23323)``
* ``'CloudBuildCreateBuildOperator': Remove 'body'. Please use 'build' (#23263)``

* ``Replica cluster id removal (#23251)``

  * ``'BigtableCreateInstanceOperator' Remove 'replica_cluster_id', 'replica_cluster_zone'. Please use 'replica_clusters'``
  * ``'BigtableHook.create_instance': Remove 'replica_cluster_id', 'replica_cluster_zone'. Please use 'replica_clusters'``

* ``Remove params (#23230)``

  * ``'GoogleDisplayVideo360CreateReportOperator': Remove 'params'. Please use 'parameters'``
  * ``'FacebookAdsReportToGcsOperator': Remove 'params'. Please use 'parameters'``

* ``'GoogleDriveToGCSOperator': Remove 'destination_bucket' and 'destination_object'. Please use 'bucket_name' and 'object_name' (#23072)``

* ``'GCSObjectsWtihPrefixExistenceSensor' removed. Please use 'GCSObjectsWithPrefixExistenceSensor' (#23050)``

* ``Remove 'project': (#23231)``

  * ``'PubSubCreateTopicOperator': Remove 'project'. Please use 'project_id'``
  * ``'PubSubCreateSubscriptionOperator': Remove 'topic_project'. Please use 'project_id'``
  * ``'PubSubCreateSubscriptionOperator': Remove 'subscription_project'. Please use 'subscription_project_id'``
  * ``'PubSubDeleteTopicOperator': Remove 'project'. Please use 'project_id'``
  * ``'PubSubDeleteSubscriptionOperator': Remove 'project'. Please use 'project_id'``
  * ``'PubSubPublishMessageOperator': Remove 'project'. Please use 'project_id'``
  * ``'PubSubPullSensor': Remove 'project'. Please use 'project_id'``
  * ``'PubSubPullSensor': Remove 'return_immediately'``

* ``Remove 'location' - replaced with 'region' (#23250)``

  * ``'DataprocJobSensor': Remove 'location'. Please use 'region'``
  * ``'DataprocCreateWorkflowTemplateOperator': Remove 'location'. Please use 'region'``
  * ``'DataprocCreateClusterOperator': Remove 'location'. Please use 'region'``
  * ``'DataprocSubmitJobOperator': Remove 'location'. Please use 'region'``
  * ``'DataprocHook': Remove 'location' parameter. Please use 'region'``
  * ``Affected functions are:``

    * ``'cancel_job'``
    * ``'create_workflow_template'``
    * ``'get_batch_client'``
    * ``'get_cluster_client'``
    * ``'get_job'``
    * ``'get_job_client'``
    * ``'get_template_client'``
    * ``'instantiate_inline_workflow_template'``
    * ``'instantiate_workflow_template'``
    * ``'submit_job'``
    * ``'update_cluster'``
    * ``'wait_for_job'``

  * ``'DataprocHook': Order of parameters in 'wait_for_job' function has changed``
  * ``'DataprocSubmitJobOperator': order of parameters has changed.``

* ``Removal of xcom_push (#23252)``

  * ``'CloudDatastoreImportEntitiesOperator': Remove 'xcom_push'. Please use 'BaseOperator.do_xcom_push'``
  * ``'CloudDatastoreExportEntitiesOperator': Remove 'xcom_push'. Please use 'BaseOperator.do_xcom_push'``

* ``'bigquery_conn_id' and 'google_cloud_storage_conn_id' is removed. Please use 'gcp_conn_id' (#23326)``.

  * ``Affected classes:``

    * ``'BigQueryCheckOperator'``
    * ``'BigQueryCreateEmptyDatasetOperator'``
    * ``'BigQueryDeleteDatasetOperator'``
    * ``'BigQueryDeleteTableOperator'``
    * ``'BigQueryExecuteQueryOperator'``
    * ``'BigQueryGetDataOperator'``
    * ``'BigQueryHook'``
    * ``'BigQueryIntervalCheckOperator'``
    * ``'BigQueryTableExistenceSensor'``
    * ``'BigQueryTablePartitionExistenceSensor'``
    * ``'BigQueryToBigQueryOperator'``
    * ``'BigQueryToGCSOperator'``
    * ``'BigQueryUpdateTableSchemaOperator'``
    * ``'BigQueryUpsertTableOperator'``
    * ``'BigQueryValueCheckOperator'``
    * ``'GCSToBigQueryOperator'``
    * ``'ADLSToGCSOperator'``
    * ``'BaseSQLToGCSOperator'``
    * ``'CassandraToGCSOperator'``
    * ``'GCSBucketCreateAclEntryOperator'``
    * ``'GCSCreateBucketOperator'``
    * ``'GCSDeleteObjectsOperator'``
    * ``'GCSHook'``
    * ``'GCSListObjectsOperator'``
    * ``'GCSObjectCreateAclEntryOperator'``
    * ``'GCSToBigQueryOperator'``
    * ``'GCSToGCSOperator'``
    * ``'GCSToLocalFilesystemOperator'``
    * ``'LocalFilesystemToGCSOperator'``

* ``'S3ToGCSOperator': Remove 'dest_gcs_conn_id'. Please use 'gcp_conn_id' (#23348)``

* ``'BigQueryHook' changes (#23269)``

  * ``'BigQueryHook.create_empty_table' Remove 'num_retries'. Please use 'retry'``
  * ``'BigQueryHook.run_grant_dataset_view_access' Remove 'source_project'. Please use 'project_id'``

* ``'DataprocHook': Remove deprecated function 'submit' (#23389)``


Features
~~~~~~~~

* ``[FEATURE] google provider - BigQueryInsertJobOperator log query (#23648)``
* ``[FEATURE] google provider - split GkeStartPodOperator execute (#23518)``
* ``Add exportContext.offload flag to CLOUD_SQL_EXPORT_VALIDATION. (#23614)``
* ``Create links for BiqTable operators (#23164)``
* ``implements #22859 - Add .sql as templatable extension (#22920)``
* ``'GCSFileTransformOperator': New templated fields 'source_object', 'destination_object' (#23328)``

Bug Fixes
~~~~~~~~~

* ``Fix 'PostgresToGCSOperator' does not allow nested JSON (#23063)``
* ``Fix GCSToGCSOperator ignores replace parameter when there is no wildcard (#23340)``
* ``update processor to fix broken download URLs (#23299)``
* ``'LookerStartPdtBuildOperator', 'LookerCheckPdtBuildSensor' : fix empty materialization id handling (#23025)``
* ``Change ComputeSSH to throw provider import error instead paramiko (#23035)``
* ``Fix cancel_on_kill after execution timeout for DataprocSubmitJobOperator (#22955)``
* ``Fix select * query xcom push for BigQueryGetDataOperator (#22936)``
* ``MSSQLToGCSOperator fails: datetime is not JSON Serializable (#22882)``

Misc
~~~~

* ``Add Stackdriver assets and migrate system tests to AIP-47 (#23320)``
* ``CloudTasks assets & system tests migration (AIP-47) (#23282)``
* ``TextToSpeech assets & system tests migration (AIP-47) (#23247)``
* ``Fix code-snippets in google provider (#23438)``
* ``Bigquery assets (#23165)``
* ``Remove redundant docstring in 'BigQueryUpdateTableSchemaOperator' (#23349)``
* ``Migrate gcs to new system tests design (#22778)``
* ``add missing docstring in 'BigQueryHook.create_empty_table' (#23270)``
* ``Cleanup Google provider CHANGELOG.rst (#23390)``
* ``migrate system test gcs_to_bigquery into new design (#22753)``
* ``Add example DAG for demonstrating usage of GCS sensors (#22808)``
* ``Clean up in-line f-string concatenation (#23591)``
* ``Bump pre-commit hook versions (#22887)``
* ``Use new Breese for building, pulling and verifying the images. (#23104)``
* ``Fix new MyPy errors in main (#22884)``

6.8.0
.....

Features
~~~~~~~~

* ``Add autodetect arg in BQCreateExternalTable Operator (#22710)``
* ``Add links for BigQuery Data Transfer (#22280)``
* ``Modify transfer operators to handle more data (#22495)``
* ``Create Endpoint and Model Service, Batch Prediction and Hyperparameter Tuning Jobs operators for Vertex AI service (#22088)``
* ``PostgresToGoogleCloudStorageOperator - BigQuery schema type for time zone naive fields (#22536)``
* ``Update secrets backends to use get_conn_value instead of get_conn_uri (#22348)``

Bug Fixes
~~~~~~~~~

* ``Fix the docstrings (#22497)``
* ``Fix 'download_media' url in 'GoogleDisplayVideo360SDFtoGCSOperator' (#22479)``
* ``Fix to 'CloudBuildRunBuildTriggerOperator' fails to find build id. (#22419)``
* ``Fail ''LocalFilesystemToGCSOperator'' if src does not exist (#22772)``
* ``Remove coerce_datetime usage from GCSTimeSpanFileTransformOperator (#22501)``

Misc
~~~~

* ``Refactor: BigQuery to GCS Operator (#22506)``
* ``Remove references to deprecated operators/params in PubSub operators (#22519)``
* ``New design of system tests (#22311)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update black precommit (#22521)``

6.7.0
.....

Features
~~~~~~~~

* ``Add dataflow_default_options to templated_fields (#22367)``
* ``Add 'LocalFilesystemToGoogleDriveOperator' (#22219)``
* ``Add timeout and retry to the BigQueryInsertJobOperator (#22395)``

Bug Fixes
~~~~~~~~~

* ``Fix skipping non-GCS located jars (#22302)``
* ``[FIX] typo doc of gcs operator (#22290)``
* ``Fix mistakenly added install_requires for all providers (#22382)``

6.6.0
.....

Features
~~~~~~~~

* ``Support Uploading Bigger Files to Google Drive (#22179)``
* ``Change the default 'chunk_size' to a clear representation & add documentation (#22222)``
* ``Add guide for DataprocInstantiateInlineWorkflowTemplateOperator (#22062)``
* ``Allow for uploading metadata with GCS Hook Upload (#22058)``
* ``Add Dataplex operators (#20377)``

Misc
~~~~~

* ``Add support for ARM platform (#22127)``
* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``
* ``Use yaml safe load (#22091)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add map_index to XCom model and interface (#22112)``
   * ``Fix spelling (#22107)``
   * ``Use yaml safe load (#22085)``
   * ``Update ''GKEDeleteClusterOperator', ''GKECreateClusterOperator'' docstrings (#22212)``
   * ``Revert "Use yaml safe load (#22085)" (#22089)``
   * ``Protect against accidental misuse of XCom.get_value() (#22244)``

6.5.0
.....

Features
~~~~~~~~

* ``Add Looker PDT operators (#20882)``
* ``Add autodetect arg to external table creation in GCSToBigQueryOperator (#21944)``
* ``Add Dataproc assets/links (#21756)``
* ``Add Auto ML operators for Vertex AI service (#21470)``
* ``Add GoogleCalendarToGCSOperator (#20769)``
* ``Make project_id argument optional in all dataproc operators (#21866)``
* ``Allow templates in more DataprocUpdateClusterOperator fields (#21865)``
* ``Dataflow Assets (#21639)``
* ``Extract ClientInfo to module level (#21554)``
* ``Datafusion assets (#21518)``
* ``Dataproc metastore assets (#21267)``
* ``Normalize *_conn_id parameters in BigQuery sensors (#21430)``

Bug Fixes
~~~~~~~~~

* ``Fix bigquery_dts parameter docstring typo (#21786)``
* ``Fixed PostgresToGCSOperator fail on empty resultset for use_server_side_cursor=True (#21307)``
* ``Fix multi query scenario in bigquery example DAG (#21575)``

Misc
~~~~

* ``Support for Python 3.10``
* ``Unpin 'google-cloud-memcache' (#21912)``
* ``Unpin ''pandas-gbq'' and remove unused code (#21915)``
* ``Suppress hook warnings from the Bigquery transfers (#20119)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Change BaseOperatorLink interface to take a ti_key, not a datetime (#21798)``

6.4.0
.....

Features
~~~~~~~~

* ``Add hook for integrating with Google Calendar (#20542)``
* ``Add encoding parameter to 'GCSToLocalFilesystemOperator' to fix #20901 (#20919)``
* ``batch as templated field in DataprocCreateBatchOperator (#20905)``
* ``Make timeout Optional for wait_for_operation (#20981)``
* ``Add more SQL template fields renderers (#21237)``
* ``Create CustomJob and Datasets operators for Vertex AI service (#21253)``
* ``Support to upload file to Google Shared Drive (#21319)``
* ``(providers_google) add a location check in bigquery (#19571)``
* ``Add support for BeamGoPipelineOperator (#20386)``
* ``Google Cloud Composer opearators (#21251)``
* ``Enable asynchronous job submission in BigQuery hook (#21385)``
* ``Optionally raise an error if source file does not exist in GCSToGCSOperator (#21391)``

Bug Fixes
~~~~~~~~~

* ``Cloudsql import links fix. (#21199)``
* ``Fix BigQueryDataTransferServiceHook.get_transfer_run() request parameter (#21293)``
* ``:bug: (BigQueryHook) fix compatibility with sqlalchemy engine (#19508)``

Misc
~~~~

* ``Refactor operator links to not create ad hoc TaskInstances (#21285)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix last google provider MyPy errors (#21010)``
   * ``Add optional features in providers. (#21074)``
   * ``Revert "Create CustomJob and Datasets operators for Vertex AI service (#20077)" (#21203)``
   * ``Create CustomJob and Datasets operators for Vertex AI service (#20077)``
   * ``Extend dataproc example dag (#21091)``
   * ``Squelch more deprecation warnings (#21003)``
   * ``Remove a few stray ':type's in docs (#21014)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Fix BigQuery system test (#21320)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Never set DagRun.state to State.NONE (#21263)``
   * ``Add pre-commit check for docstring param types (#21398)``
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``

6.3.0
.....

Features
~~~~~~~~

* ``Add optional location to bigquery data transfer service (#15088) (#20221)``
* ``Add Google Cloud Tasks how-to documentation (#20145)``
* ``Added example DAG for MSSQL to Google Cloud Storage (GCS) (#19873)``
* ``Support regional GKE cluster (#18966)``
* ``Delete pods by default in KubernetesPodOperator (#20575)``

Bug Fixes
~~~~~~~~~

* ``Fixes docstring for PubSubCreateSubscriptionOperator (#20237)``
* ``Fix missing get_backup method for Dataproc Metastore (#20326)``
* ``BigQueryHook fix typo in run_load doc string (#19924)``
* ``Fix passing the gzip compression parameter on sftp_to_gcs. (#20553)``
* ``switch to follow_redirects on httpx.get call in CloudSQL provider (#20239)``
* ``avoid deprecation warnings in BigQuery transfer operators (#20502)``
* ``Change download_video parameter to resourceName (#20528)``
* ``Fix big query to mssql/mysql transfer issues (#20001)``
* ``Fix setting of project ID in ''provide_authorized_gcloud'' (#20428)``

Misc
~~~~

* ``Move source_objects datatype check out of GCSToBigQueryOperator.__init__ (#20347)``
* ``Organize S3 Classes in Amazon Provider (#20167)``
* ``Providers facebook hook multiple account (#19377)``
* ``Remove deprecated method call (blob.download_as_string) (#20091)``
* ``Remove deprecated template_fields from GoogleDriveToGCSOperator (#19991)``

Note! optional features of the ``apache-airflow-providers-facebook`` and ``apache-airflow-providers-amazon``
require newer versions of the providers (as specified in the dependencies)

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix mypy errors for google.cloud_build (#20234)``
   * ``Fix MyPy for Google Bigquery (#20329)``
   * ``Fix remaining MyPy errors in Google Provider (#20358)``
   * ``Fix MyPy Errors for dataproc package (#20327)``
   * ``Fix MyPy errors for google.cloud.tasks (#20233)``
   * ``Fix MyPy Errors for Apache Beam (and Dataflow) provider. (#20301)``
   * ``Fix MyPy errors in leveldb (#20222)``
   * ``Fix MyPy errors for google.cloud.transfers (#20229)``
   * ``Fix MyPY errors for google.cloud.example_dags (#20232)``
   * ``Fix MyPy errors for google/marketing_platform and suite (#20227)``
   * ``Fix MyPy errors in google.cloud.sensors (#20228)``
   * ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
   * ``Finalised Datastore documentation (#20138)``
   * ``Update Sphinx and Sphinx-AutoAPI (#20079)``
   * ``Update doc reference links (#19909)``
   * ``Use Python3.7+ syntax in pyupgrade (#20501)``
   * ``Fix MyPy errors in Google Cloud (again) (#20469)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix Google mlengine MyPy errors (#20569)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix Google Mypy Dataproc errors (#20570)``
   * ``Fix mypy errors in Google Cloud provider (#20611)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Fix mypy errors in google/cloud/operators/stackdriver (#20601)``
   * ``Update documentation for provider December 2021 release (#20523)``

6.2.0
.....

Features
~~~~~~~~

* ``Added wait mechanizm to the DataprocJobSensor to avoid 509 errors when Job is not available (#19740)``
* ``Add support in GCP connection for reading key from Secret Manager (#19164)``
* ``Add dataproc metastore operators (#18945)``
* ``Add support of 'path' parameter for GCloud Storage Transfer Service operators (#17446)``
* ``Move 'bucket_name' validation out of '__init__' in Google Marketing Platform operators (#19383)``
* ``Create dataproc serverless spark batches operator (#19248)``
* ``updates pipeline_timeout CloudDataFusionStartPipelineOperator (#18773)``
* ``Support impersonation_chain parameter in the GKEStartPodOperator (#19518)``

Bug Fixes
~~~~~~~~~

* ``Fix badly merged impersonation in GKEPodOperator (#19696)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix typos in warnings, docstrings, exceptions (#19424)``
   * ``Fix Cloud SQL system tests (#19014)``
   * ``Fix GCS system tests (#19227)``
   * ``Update 'default_args' value in example_functions DAG from str to int (#19865)``
   * ``Clean up ''default_args'' usage in docs (#19803)``
   * ``Clean-up of google cloud example dags - batch 3 (#19664)``
   * ``Misc. documentation typos and language improvements (#19599)``
   * ``Cleanup dynamic 'start_date' use for miscellaneous Google example DAGs (#19400)``
   * ``Remove reference to deprecated operator in example_dataproc (#19619)``
   * ``#16691 Providing more information in docs for DataprocCreateCluster operator migration (#19446)``
   * ``Clean-up of google cloud example dags - batch 2 (#19527)``
   * ``Update Azure modules to comply with AIP-21 (#19431)``
   * ``Remove remaining 'pylint: disable' comments (#19541)``
   * ``Clean-up of google cloud example dags (#19436)``

6.1.0
.....

Features
~~~~~~~~

* ``Add value to 'namespaceId' of query (#19163)``
* ``Add pre-commit hook for common misspelling check in files (#18964)``
* ``Support query timeout as an argument in CassandraToGCSOperator (#18927)``
* ``Update BigQueryCreateExternalTableOperator doc and parameters (#18676)``
* ``Replacing non-attribute template_fields for BigQueryToMsSqlOperator (#19052)``
* ``Upgrade the Dataproc package to 3.0.0 and migrate from v1beta2 to v1 api (#18879)``
* ``Use google cloud credentials when executing beam command in subprocess (#18992)``
* ``Replace default api_version of FacebookAdsReportToGcsOperator (#18996)``
* ``Dataflow Operators - use project and location from job in on_kill method. (#18699)``

Bug Fixes
~~~~~~~~~

* ``Fix hard-coded /tmp directory in CloudSQL Hook (#19229)``
* ``Fix bug in Dataflow hook when no jobs are returned (#18981)``
* ``Fix BigQueryToMsSqlOperator documentation (#18995)``
* ``Move validation of templated input params to run after the context init (#19048)``
* ``Google provider catch invalid secret name (#18790)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update dataflow.py (#19231)``
   * ``More f-strings (#18855)``
   * ``Simplify strings previously split across lines (#18679)``

6.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~
* ``Migrate Google Cloud Build from Discovery API to Python SDK (#18184)``

Features
~~~~~~~~

* ``Add index to the dataset name to have separate dataset for each example DAG (#18459)``
* ``Add missing __init__.py files for some test packages (#18142)``
* ``Add possibility to run DAGs from system tests and see DAGs logs (#17868)``
* ``Rename AzureDataLakeStorage to ADLS (#18493)``
* ``Make next_dagrun_info take a data interval (#18088)``
* ``Use parameters instead of params (#18143)``
* ``New google operator: SQLToGoogleSheetsOperator (#17887)``

Bug Fixes
~~~~~~~~~

* ``Fix part of Google system tests (#18494)``
* ``Fix kubernetes engine system test (#18548)``
* ``Fix BigQuery system test (#18373)``
* ``Fix error when create external table using table resource (#17998)``
* ``Fix ''BigQuery'' data extraction in ''BigQueryToMySqlOperator'' (#18073)``
* ``Fix providers tests in main branch with eager upgrades (#18040)``
* ``fix(CloudSqlProxyRunner): don't query connections from Airflow DB (#18006)``
* ``Remove check for at least one schema in GCSToBigquery (#18150)``
* ``deduplicate running jobs on BigQueryInsertJobOperator (#17496)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updating miscellaneous provider DAGs to use TaskFlow API where applicable (#18278)``
   * ``Inclusive Language (#18349)``
   * ``Change TaskInstance and TaskReschedule PK from execution_date to run_id (#17719)``

5.1.0
.....

Features
~~~~~~~~

* ``Add error check for config_file parameter in GKEStartPodOperator (#17700)``
* ``Gcp ai hyperparameter tuning (#17790)``
* ``Allow omission of 'initial_node_count' if 'node_pools' is specified (#17820)``
* ``[Airflow 13779] use provided parameters in the wait_for_pipeline_state hook (#17137)``
* ``Enable specifying dictionary paths in 'template_fields_renderers' (#17321)``
* ``Don't cache Google Secret Manager client (#17539)``
* ``[AIRFLOW-9300] Add DatafusionPipelineStateSensor and aync option to the CloudDataFusionStartPipelineOperator (#17787)``

Bug Fixes
~~~~~~~~~

* ``GCP Secret Manager error handling for missing credentials (#17264)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``
* ``Adds secrets backend/logging/auth information to provider yaml (#17625)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Fix missing Data Fusion sensor integration (#17914)``
   * ``Remove all deprecation warnings in providers (#17900)``

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Updated GoogleAdsHook to support newer API versions after google deprecated v5. Google Ads v8 is the new default API. (#17111)``
* ``Google Ads Hook: Support newer versions of the google-ads library (#17160)``

.. warning:: The underlying google-ads library had breaking changes.

   Previously the google ads library returned data as native protobuf messages. Now it returns data as proto-plus objects that behave more like conventional Python objects.

   To preserve compatibility the hook's ``search()`` converts the data back to native protobuf before returning it. Your existing operators *should* work as before, but due to the urgency of the v5 API being deprecated it was not tested too thoroughly. Therefore you should carefully evaluate your operator and hook functionality with this new version.

   In order to use the API's new proto-plus format, you can use the ``search_proto_plus()`` method.

   For more information, please consult `google-ads migration document <https://developers.google.com/google-ads/api/docs/client-libs/python/library-version-10>`__:


Features
~~~~~~~~

* ``Standardise dataproc location param to region (#16034)``
* ``Adding custom Salesforce connection type + SalesforceToS3Operator updates (#17162)``

Bug Fixes
~~~~~~~~~

* ``Update alias for field_mask in Google Memmcache (#16975)``
* ``fix: dataprocpysparkjob project_id as self.project_id (#17075)``
* ``Fix GCStoGCS operator with replace diabled and existing destination object (#16991)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Fixes several failing tests after broken main (#17222)``
   * ``Fixes statich check failures (#17218)``
   * ``[CASSANDRA-16814] Fix cassandra to gcs type inconsistency. (#17183)``
   * ``Updating Google Cloud example DAGs to use XComArgs (#16875)``
   * ``Updating miscellaneous Google example DAGs to use XComArgs (#16876)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

* ``Move plyvel to google provider extra (#15812)``
* ``Fixes AzureFileShare connection extras (#16388)``

Features
~~~~~~~~

* ``Add extra links for google dataproc (#10343)``
* ``add oracle  connection link (#15632)``
* ``pass wait_for_done parameter down to _DataflowJobsController (#15541)``
* ``Use api version only in GoogleAdsHook not operators (#15266)``
* ``Implement BigQuery Table Schema Update Operator (#15367)``
* ``Add BigQueryToMsSqlOperator (#15422)``

Bug Fixes
~~~~~~~~~

* ``Fix: GCS To BigQuery source_object (#16160)``
* ``Fix: Unnecessary downloads in ``GCSToLocalFilesystemOperator`` (#16171)``
* ``Fix bigquery type error when export format is parquet (#16027)``
* ``Fix argument ordering and type of bucket and object (#15738)``
* ``Fix sql_to_gcs docstring lint error (#15730)``
* ``fix: ensure datetime-related values fully compatible with MySQL and BigQuery (#15026)``
* ``Fix deprecation warnings location in google provider (#16403)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Rename the main branch of the Airflow repo to be 'main' (#16149)``
   * ``Check synctatic correctness for code-snippets (#16005)``
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Get rid of requests as core dependency (#15781)``
   * ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Fix spelling (#15699)``
   * ``Add short description to BaseSQLToGCSOperator docstring (#15728)``
   * ``More documentation update for June providers release (#16405)``
   * ``Remove class references in changelogs (#16454)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Change in ``AutoMLPredictOperator``
```````````````````````````````````

The ``params`` parameter in ``airflow.providers.google.cloud.operators.automl.AutoMLPredictOperator`` class
was renamed ``operation_params`` because it conflicted with a ``param`` parameter in the ``BaseOperator`` class.

Integration with the ``apache.beam`` provider
`````````````````````````````````````````````

In 3.0.0 version of the provider we've changed the way of integrating with the ``apache.beam`` provider.
The previous versions of both providers caused conflicts when trying to install them together
using PIP > 20.2.4. The conflict is not detected by PIP 20.2.4 and below but it was there and
the version of ``Google BigQuery`` python client was not matching on both sides. As the result, when
both ``apache.beam`` and ``google`` provider were installed, some features of the ``BigQuery`` operators
might not work properly. This was cause by ``apache-beam`` client not yet supporting the new google
python clients when ``apache-beam[gcp]`` extra was used. The ``apache-beam[gcp]`` extra is used
by ``Dataflow`` operators and while they might work with the newer version of the ``Google BigQuery``
python client, it is not guaranteed.

This version introduces additional extra requirement for the ``apache.beam`` extra of the ``google`` provider
and symmetrically the additional requirement for the ``google`` extra of the ``apache.beam`` provider.
Both ``google`` and ``apache.beam`` provider do not use those extras by default, but you can specify
them when installing the providers. The consequence of that is that some functionality of the ``Dataflow``
operators might not be available.

Unfortunately the only ``complete`` solution to the problem is for the ``apache.beam`` to migrate to the
new (>=2.0.0) Google Python clients.

This is the extra for the ``google`` provider:

.. code-block:: python

        extras_require = (
            {
                # ...
                "apache.beam": ["apache-airflow-providers-apache-beam", "apache-beam[gcp]"],
                # ...
            },
        )

And likewise this is the extra for the ``apache.beam`` provider:

.. code-block:: python

        extras_require = ({"google": ["apache-airflow-providers-google", "apache-beam[gcp]"]},)

You can still run this with PIP version <= 20.2.4 and go back to the previous behaviour:

.. code-block:: shell

  pip install apache-airflow-providers-google[apache.beam]

or

.. code-block:: shell

  pip install apache-airflow-providers-apache-beam[google]

But be aware that some ``BigQuery`` operators functionality might not be available in this case.

Features
~~~~~~~~

* ``[Airflow-15245] - passing custom image family name to the DataProcClusterCreateoperator (#15250)``

Bug Fixes
~~~~~~~~~

* ``Bugfix: Fix rendering of ''object_name'' in ''GCSToLocalFilesystemOperator'' (#15487)``
* ``Fix typo in DataprocCreateClusterOperator (#15462)``
* ``Fixes wrongly specified path for leveldb hook (#15453)``


2.2.0
.....

Features
~~~~~~~~

* ``Adds 'Trino' provider (with lower memory footprint for tests) (#15187)``
* ``update remaining old import paths of operators (#15127)``
* ``Override project in dataprocSubmitJobOperator (#14981)``
* ``GCS to BigQuery Transfer Operator with Labels and Description parameter (#14881)``
* ``Add GCS timespan transform operator (#13996)``
* ``Add job labels to bigquery check operators. (#14685)``
* ``Use libyaml C library when available. (#14577)``
* ``Add Google leveldb hook and operator (#13109) (#14105)``

Bug fixes
~~~~~~~~~

* ``Google Dataflow Hook to handle no Job Type (#14914)``

2.1.0
.....

Features
~~~~~~~~

* ``Corrects order of argument in docstring in GCSHook.download method (#14497)``
* ``Refactor SQL/BigQuery/Qubole/Druid Check operators (#12677)``
* ``Add GoogleDriveToLocalOperator (#14191)``
* ``Add 'exists_ok' flag to BigQueryCreateEmptyTable(Dataset)Operator (#14026)``
* ``Add materialized view support for BigQuery (#14201)``
* ``Add BigQueryUpdateTableOperator (#14149)``
* ``Add param to CloudDataTransferServiceOperator (#14118)``
* ``Add gdrive_to_gcs operator, drive sensor, additional functionality to drive hook  (#13982)``
* ``Improve GCSToSFTPOperator paths handling (#11284)``

Bug Fixes
~~~~~~~~~

* ``Fixes to dataproc operators and hook (#14086)``
* ``#9803 fix bug in copy operation without wildcard  (#13919)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Updated ``google-cloud-*`` libraries
````````````````````````````````````

This release of the provider package contains third-party library updates, which may require updating your
DAG files or custom hooks and operators, if you were using objects from those libraries.
Updating of these libraries is necessary to be able to use new features made available by new versions of
the libraries and to obtain bug fixes that are only available for new versions of the library.

Details are covered in the UPDATING.md files for each library, but there are some details
that you should pay attention to.


+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| Library name                                                                                        | Previous constraints | Current constraints | Upgrade Documentation                                                                                                               |
+=====================================================================================================+======================+=====================+=====================================================================================================================================+
| `google-cloud-automl <https://pypi.org/project/google-cloud-automl/>`_                              | ``>=0.4.0,<2.0.0``   | ``>=2.1.0,<3.0.0``  | `Upgrading google-cloud-automl <https://github.com/googleapis/python-automl/blob/main/UPGRADING.md>`_                               |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-bigquery-datatransfer <https://pypi.org/project/google-cloud-bigquery-datatransfer>`_ | ``>=0.4.0,<2.0.0``   | ``>=3.0.0,<4.0.0``  | `Upgrading google-cloud-bigquery-datatransfer <https://github.com/googleapis/python-bigquery-datatransfer/blob/main/UPGRADING.md>`_ |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-datacatalog <https://pypi.org/project/google-cloud-datacatalog>`_                     | ``>=0.5.0,<0.8``     | ``>=3.0.0,<4.0.0``  | `Upgrading google-cloud-datacatalog <https://github.com/googleapis/python-datacatalog/blob/main/UPGRADING.md>`_                     |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-dataproc <https://pypi.org/project/google-cloud-dataproc/>`_                          | ``>=1.0.1,<2.0.0``   | ``>=2.2.0,<3.0.0``  | `Upgrading google-cloud-dataproc <https://github.com/googleapis/python-dataproc/blob/main/UPGRADING.md>`_                           |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-kms <https://pypi.org/project/google-cloud-kms>`_                                     | ``>=1.2.1,<2.0.0``   | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-kms <https://github.com/googleapis/python-kms/blob/main/UPGRADING.md>`_                                     |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-logging <https://pypi.org/project/google-cloud-logging/>`_                            | ``>=1.14.0,<2.0.0``  | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-logging <https://github.com/googleapis/python-logging/blob/main/UPGRADING.md>`_                             |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-monitoring <https://pypi.org/project/google-cloud-monitoring>`_                       | ``>=0.34.0,<2.0.0``  | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-monitoring <https://github.com/googleapis/python-monitoring/blob/main/UPGRADING.md)>`_                      |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-os-login <https://pypi.org/project/google-cloud-os-login>`_                           | ``>=1.0.0,<2.0.0``   | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-os-login <https://github.com/googleapis/python-oslogin/blob/main/UPGRADING.md>`_                            |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-pubsub <https://pypi.org/project/google-cloud-pubsub>`_                               | ``>=1.0.0,<2.0.0``   | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-pubsub <https://github.com/googleapis/python-pubsub/blob/main/UPGRADING.md>`_                               |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-tasks <https://pypi.org/project/google-cloud-tasks>`_                                 | ``>=1.2.1,<2.0.0``   | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-task <https://github.com/googleapis/python-tasks/blob/main/UPGRADING.md>`_                                  |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------+

The field names use the snake_case convention
`````````````````````````````````````````````

If your DAG uses an object from the above mentioned libraries passed by XCom, it is necessary to update the
naming convention of the fields that are read. Previously, the fields used the CamelSnake convention,
now the snake_case convention is used.

**Before:**

.. code-block:: python

    set_acl_permission = GCSBucketCreateAclEntryOperator(
        task_id="gcs-set-acl-permission",
        bucket=BUCKET_NAME,
        entity="user-{{ task_instance.xcom_pull('get-instance')['persistenceIamIdentity'].split(':', 2)[1] }}",
        role="OWNER",
    )


**After:**

.. code-block:: python

    set_acl_permission = GCSBucketCreateAclEntryOperator(
        task_id="gcs-set-acl-permission",
        bucket=BUCKET_NAME,
        entity="user-{{ task_instance.xcom_pull('get-instance')['persistence_iam_identity']"
        ".split(':', 2)[1] }}",
        role="OWNER",
    )


Features
~~~~~~~~

* ``Add Apache Beam operators (#12814)``
* ``Add Google Cloud Workflows Operators (#13366)``
* ``Replace 'google_cloud_storage_conn_id' by 'gcp_conn_id' when using 'GCSHook' (#13851)``
* ``Add How To Guide for Dataflow (#13461)``
* ``Generalize MLEngineStartTrainingJobOperator to custom images (#13318)``
* ``Add Parquet data type to BaseSQLToGCSOperator (#13359)``
* ``Add DataprocCreateWorkflowTemplateOperator (#13338)``
* ``Add OracleToGCS Transfer (#13246)``
* ``Add timeout option to gcs hook methods. (#13156)``
* ``Add regional support to dataproc workflow template operators (#12907)``
* ``Add project_id to client inside BigQuery hook update_table method (#13018)``

Bug fixes
~~~~~~~~~

* ``Fix four bugs in StackdriverTaskHandler (#13784)``
* ``Decode Remote Google Logs (#13115)``
* ``Fix and improve GCP BigTable hook and system test (#13896)``
* ``updated Google DV360 Hook to fix SDF issue (#13703)``
* ``Fix insert_all method of BigQueryHook to support tables without schema (#13138)``
* ``Fix Google BigQueryHook method get_schema() (#13136)``
* ``Fix Data Catalog operators (#13096)``


1.0.0
.....

Initial version of the provider.
