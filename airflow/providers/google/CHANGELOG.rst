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

Changelog
---------

7.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``apache-airflow-providers-google uses deprecated Google Ads API V8 (#22111)``

.. warning:: The underlying google-ads library has been updated

   This drops support for versions v6 and v7 of the Google Ads API, and updates
   the default version of the Google Ads API from the deprecated v8 to v10.

   For more information, see `Deprecation and sunset <https://developers.google.com/google-ads/api/docs/sunset-dates>`_
   and `Upgrading to the newest version <https://developers.google.com/google-ads/api/docs/version-migration>`_

* ``BigtableCreateInstanceOperator`` Remove ``replica_cluster_id``, ``replica_cluster_zone``. Please use ``replica_clusters``.

* ``BigtableHook.create_instance``: Remove ``replica_cluster_id``, ``replica_cluster_zone``. Please use ``replica_clusters``.

* ``GoogleDisplayVideo360CreateReportOperator``: remove ``params``. Please use ``parameters``

* ``FacebookAdsReportToGcsOperator``: remove ``params``. Please use ``parameters``

* ``GoogleDriveToGCSOperator``: Remove ``destination_bucket`` and ``destination_object``. Please use ``bucket_name`` and ``object_name``.

* ``GCSObjectsWtihPrefixExistenceSensor`` removed. Please use ``GCSObjectsWithPrefixExistenceSensor``.

* ``PubSubPullSensor``: Remove ``project``. Please use ``project_id``

* ``PubSubPullSensor``: Remove ``return_immediately``

* ``DataprocJobSensor``: Remove ``location``. Please use ``region``.

* ``DataprocCreateWorkflowTemplateOperator``: Remove ``location``. Please use ``region``.

* ``DataprocCreateClusterOperator``: Remove ``location``. Please use ``region``.

* ``DataprocSubmitJobOperator``: Remove ``location``. Please use ``region``.

* ``DataprocHook``: Remove ``location`` parameter. Please use ``region``.
  affected functions are:
  ``cancel_job``, ``create_workflow_template``, ``get_batch_client``,
  ``get_cluster_client``, ``get_job``, ``get_job_client``, ``get_template_client``,
  ``instantiate_inline_workflow_template``, ``instantiate_workflow_template``,
  ``submit_job``, ``update_cluster``,``wait_for_job``

* ``DataprocHook``: order of parameters in ``wait_for_job`` function has changed.

* ``DataprocSubmitJobOperator``: order of parameters has changed.

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
        entity="user-{{ task_instance.xcom_pull('get-instance')['persistenceIamIdentity']"
        ".split(':', 2)[1] }}",
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
