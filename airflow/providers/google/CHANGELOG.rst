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

Changelog
---------

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

.. Review and move the new changes to one of the sections above:

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

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

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

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

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
