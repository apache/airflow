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

``apache-airflow-providers-amazon``

Changelog
---------

8.11.0
......

Breaking changes
~~~~~~~~~~~~~~~~


Features
~~~~~~~~


* ``Add support for anonymous access to s3 buckets for objectstorage (#35273)``
* ``ECS Executor Health Check (#35412)``

Bug Fixes
~~~~~~~~~

* ``Fix AWS RDS hook's DB instance state check (#34773)``
* ``Fix parameter syntax in Amazon docstrings (#35349)``
* ``Improve error handling in AWS Links (#35518)``
* ``Update ECS executor healthcheck with a catchall except (#35512)``

Misc
~~~~

* ``Move ECS Executor to its own file (#35418)``
* ``Clarify "task" in ECS Executor log messages (#35304)``
* ``Make optional 'output_location' attribute in 'AthenaOperator' (#35265)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add verificationy that provider docs are as expected (#35424)``
   * ``Work around typing issue in examples and providers (#35494)``
   * ``Improve docs on objectstorage (#35294)``


8.10.0
......

.. note::
  This release introduce experimental feature: AWS ECS Executor.

Features
~~~~~~~~

* ``Add AWS ECS Executor (#34381)``
* ``AIP-58: Add Airflow ObjectStore (AFS) (#34729)``
* ``Add Http to s3 operator (#35176)``

Bug Fixes
~~~~~~~~~

* ``Enable encryption in S3 download_files() hook. (#35037)``

Misc
~~~~

* ``Use base aws classes in Amazon AppFlow Operators (#35082)``
* ``Use base aws classes in Amazon Athena Operators/Sensors/Triggers (#35133)``
* ``Use base aws classes in Amazon Lambda Operators/Sensors (#34890)``
* ``Use base aws classes in Amazon S3 Glacier Operators/Sensors (#35108)``
* ``Expose catalog parameter in 'AthenaOperator' (#35103)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor string splitting (#34185)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``

8.9.0
.....

Features
~~~~~~~~

* ``Add Glue 'DataBrew' operator (#34807)``
* ``Add 'check_interval' and 'max_attempts' as parameter of 'DynamoDBToS3Operator' (#34972)``

Bug Fixes
~~~~~~~~~

* ``Set 'EcsRunTaskOperator' default waiter duration to 70 days (#34928)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 Support - A thru Common (Inclusive) (#34934)``

8.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Implements 'AwsBaseOperator' and 'AwsBaseSensor' (#34784)``
* ``Extend hooks arguments into 'AwsBaseWaiterTrigger' (#34884)``
* ``Allow setup 'endpoint_url' per-service in AWS Connection (#34593)``
* ``Include AWS Lambda execution logs to task logs (#34692)``

Bug Fixes
~~~~~~~~~

* ``fix(providers/amazon): respect soft_fail argument when exception is raised (#34134)``
* ``do not fail operator if we cannot find logs (#34570)``
* ``Respect 'soft_fail' argument when running 'BatchSensors' (#34592)``
* ``Respect 'soft_fail' argument when running 'SqsSensor' (#34569)``
* ``Respect 'soft_fail' argument when running 'EcsBaseSensor' (#34596)``
* ``Respect 'soft_fail' argument when running 'SageMakerBaseSensor' (#34565)``
* ``Respect 'soft_fail' parameter in 'S3KeysUnchangedSensor' and 'S3KeySensor' (#34550)``
* ``Respect 'soft_fail' parameter in 'LambdaFunctionStateSensor' (#34551)``
* ``Respect 'soft_fail' parameter in 'AthenaSensor' (#34553)``
* ``Respect 'soft_fail' parameter in 'QuickSightSensor' (#34555)``
* ``Respect 'soft_fail' parameter in 'GlacierJobOperationSensor' (#34557)``
* ``Respect 'soft_fail' parameter in 'GlueJobSensor', 'GlueCatalogPartitionSensor' and 'GlueCrawlerSensor' (#34559)``
* ``Respect 'soft_fail' parameter in 'StepFunctionExecutionSensor' (#34560)``

Misc
~~~~

* ``Refactor consolidate import from io in providers (#34378)``
* ``Upgrade watchtower to 3.0.1 (#25019) (#34747)``
* ``Bump min airflow version of providers (#34728)``
* ``Refactor: consolidate import time in providers (#34402)``
* ``Refactor usage of str() in providers (#34320)``
* ``Refactor import from collections (#34406)``
* ``Clarify Amazon Lambda invocation and sensing (#34653)``
* ``Refactor multiple equals to contains in providers (#34441)``
* ``Rename 'bucket' to 'gcs_bucket' in 'GCSToS3Operator' (#33031)``
* ``Remove duplicate 'asgiref' dependency in Amazon Provider (#34580)``
* ``Update 'BatchOperator' operator_extra_links property (#34506)``
* ``sagemaker.py spell error fix (#34445)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``
* ``Use 'AirflowProviderDeprecationWarning' in the deprecated decorator in Amazon provider (#34488)``
* ``Use 'AirflowProviderDeprecationWarning' in EMR Operators (#34453)``
* ``Deprecate get_hook in DataSyncOperator and use hook instead (#34427)``
* ``Refactor shorter defaults in providers (#34347)``

8.7.1
.....

Bug Fixes
~~~~~~~~~

* ``Bugfix: Fix RDS triggers parameters so that they handle serialization/deserialization (#34222)``
* ``Use a AwsBaseWaiterTrigger-based trigger in EmrAddStepsOperator deferred mode (#34216)``

Misc
~~~~

* ``Refactor: Think positively in providers (#34279)``
* ``Remove unused parameter 'cluster_role_arn' from 'EksPodOperator''s docstring (#34300)``
* ``Correct parameter names in docstring for 'S3CreateObjectOperator' (#34263)``
* ``Refactor: Simplify comparisons (#34181)``
* ``Simplify  to bool(...) (#34258)``

8.7.0
.....

.. warning:: A bug introduced in version 8.0.0 caused all ``EcsRunTaskOperator`` tasks to detach from the ECS task
  and fail after 10 minutes, even if the ECS task was still running.
  In this version we are fixing it by returning the default ``waiter_max_attempts`` value to ``sys.maxsize``.

Features
~~~~~~~~

* ``Add Amazon SQS Notifier (#33962)``
* ``Add Amazon SNS Notifier (#33828)``

Bug Fixes
~~~~~~~~~

* ``Increase 'waiter_max_attempts' default value in 'EcsRunTaskOperator' (#33712)``
* ``Fix AWS 'EmrStepSensor' ignoring the specified 'aws_conn_id' in deferred mode  (#33952)``
* ``Fix type annotation in AppflowHook (#33881)``
* ``Make Amazon Chime connection lazy loaded and consistent with docs (#34000)``
* ``respect "soft_fail" argument when running BatchSensor in deferrable mode (#33405)``

Misc
~~~~

 * ``Refactor: Consolidate import and usage of random (#34108)``
 * ``Consolidate importing of os.path.* (#34060)``
 * ``Refactor regex in providers (#33898)``
 * ``Refactor: Simplify loop in aws/triggers/batch.py (#34052)``
 * ``Combine similar if logics in providers (#33987)``
 * ``Replace single quotes by double quotes in tests (#33864)``
 * ``Remove useless string join from providers (#33968)``
 * ``Make 'aws.session_factory' part of Amazon provider configuration documentation (#33960)``
 * ``Refactor unneeded  jumps in providers (#33833)``
 * ``Replace try - except pass by contextlib.suppress in providers (#33980)``
 * ``Remove some useless try/except from providers code (#33967)``
 * ``Refactor: Replace lambdas with comprehensions in providers (#33771)``
 * ``Replace sequence concatenation by unpacking in Airflow providers (#33933)``
 * ``Reorganize devel_only extra in airflow's setup.py (#33907)``
 * ``Remove explicit str concat from Airflow providers package and tests (#33860)``
 * ``Improve modules import in AWS provider by move some of them into a type-checking block (#33780)``
 * ``Always use 'Literal' from 'typing_extensions' (#33794)``
 * ``Use literal dict instead of calling dict() in providers (#33761)``
 * ``remove unnecessary and rewrite it using list in providers (#33763)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add decorator for suppress optional internal methods in Amazon Provider (#34034)``

8.6.0
.....

Features
~~~~~~~~

* ``Added Amazon SageMaker Notebook hook and operators (#33219)``
* ``Add 'deferrable' option to 'LambdaCreateFunctionOperator' (#33327)``
* ``Add Deferrable mode to GlueCatalogPartitionSensor (#33239)``
* ``Add 'sql_hook_params' parameter to 'S3ToSqlOperator' (#33427)``
* ``Add 'sql_hook_params' parameter to 'SqlToS3Operator' (#33425)``
* ``Add parameter to pass role ARN to 'GlueJobOperator ' (#33408)``
* ``Add new RdsStartExportTaskOperator parameters (#33251)``

Bug Fixes
~~~~~~~~~

* ``Fix bug in task logs when using AWS CloudWatch. Do not set 'start_time' (#33673)``
* ``Fix AWS Batch waiter failure state (#33656)``
* ``Fix AWS appflow waiter (#33613)``
* ``Fix striping tags when falling back to update in 'SageMakerEndpointOperator' (#33487)``


Misc
~~~~

* ``Simplify conditions on len() in providers/amazon (#33565)``
* ``Remove non-public interface usage in EcsRunTaskOperator (#29447)``
* ``Upgrade botocore/aiobotocore minimum requirements (#33649)``
* ``Consolidate import and usage of itertools (#33479)``
* ``Consolidate import and usage of pandas (#33480)``
* ``always push ECS task ARN to xcom in 'EcsRunTaskOperator' (#33703)``
* ``Use 'boto3.client' linked to resource meta instead of create new one for waiters (#33552)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add Appflow system test + improvements (#33614)``
   * ``Fix typos (double words and it's/its) (#33623)``
   * ``Refactor: Remove useless str() calls (#33629)``
   * ``Replace strftime with f-strings where nicer (#33455)``
   * ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``

8.5.1
.....

Bug Fixes
~~~~~~~~~

* ``Get failure information on EMR job failure (#32151)``
* ``Fix get_log_events() in AWS logs hook (#33290)``

Misc
~~~~

* ``Improve fetching logs from AWS (#33231)``
* ``Refactor: Simplify code in providers/amazon (#33222)``
* ``Implement EventBridge enable and disable rule operators (#33226)``
* ``Update mypy-boto3-appflow dependency (#32930)``
* ``use 'cached_property' from functools in 'RdsBaseOperator' (#33133)``
* ``Use set for 'template_fields' of 'EcsDeregisterTaskDefinitionOperator' (#33129)``

8.5.0
.....

Features
~~~~~~~~

* ``openlineage, sagemaker: add OpenLineage support for SageMaker's Processing, Transform and Training operators (#31816)``
* ``Add Amazon EventBridge PutRule hook and operator (#32869)``
* ``Add GCS Requester Pays bucket support to GCSToS3Operator (#32760)``

Bug Fixes
~~~~~~~~~

* ``Check google provider version in GCSToS3Operator before provide match_glob param (#32925)``
* ``Set longer default 'waiter_max_attempts' for deferred BatchJobOperator (#33045)``

Misc
~~~~

* ``openlineage, sagemaker: add missing OpenLineage type signature (#33114)``
* ``Add S3Bucket for mypy (#33028)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Deferrable mode for Sqs Sensor (#32809)``
   * ``Increase the number of attempts in AWS system test 'example_rds_export' (#32976)``

8.4.0
.....

Features
~~~~~~~~

* ``Add endpoint_url in test_connection (#32664)``
* ``Add support for querying Redshift Serverless clusters (#32785)``
* ``Add Deferrable mode to StepFunctionStartExecutionOperator (#32563)``
* ``Add Deferrable mode for EMR Serverless Start Job Operator (#32534)``
* ``Add Eventbridge PutEvents operator and hook (#32498)``
* ``add deferrable mode to rds start & stop DB (#32437)``
* ``EMR serverless Create/Start/Stop/Delete Application deferrable mode (#32513)``
* ``Make Start and Stop SageMaker Pipelines operators deferrable (#32683)``
* ``Deferrable mode for EKS Create/Delete Operator (#32355)``

Bug Fixes
~~~~~~~~~

* ``FIX AWS deferrable operators by using AioCredentials when using 'assume_role' (#32733)``
* ``[bugfix] fix AWS triggers where deserialization would crash if region was not specified (#32729)``
* ``Fix bug in prune_dict where empty dict and list would be removed even in strict mode (#32573)``
* ``Fix S3ToRedshiftOperator does not support default values on UPSERT (#32558)``
* ``Do not return success from AWS ECS trigger after max_attempts (#32589)``

Misc
~~~~

* ``Move all k8S classes to cncf.kubernetes provider (#32767)``
* ``Limit Appflow mypy to 1.28.12 as it introduces strange typing issue (#32901)``
* ``Further limit mypy-boto3-appflow as the fix is not in sight (#32927)``

8.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Append region info to S3ToRedshitOperator if present (#32328)``

8.3.0
.....

Features
~~~~~~~~

* ``Add 'ChimeWebhookHook' (#31939)``
* ``Add 'ChimeNotifier' (#32222)``
* ``Add deferrable mode to S3KeysUnchangedSensor (#31940)``
* ``Add deferrable mode to 'RdsCreateDbInstanceOperator' and 'RdsDeleteDbInstanceOperator' (#32171)``
* ``Add deferrable mode for 'AthenaOperator' (#32186)``
* ``Add a deferrable mode to 'BatchCreateComputeEnvironmentOperator' (#32036)``
* ``Add deferrable mode in EMR operator and sensor (#32029)``
* ``add async wait method to the "with logging" aws utils (#32055)``
* ``Add custom waiters to EMR Serverless  (#30463)``
* ``Add an option to 'GlueJobOperator' to stop the job run when the TI is killed (#32155)``
* ``deferrable mode for 'SageMakerTuningOperator' and 'SageMakerEndpointOperator' (#32112)``
* ``EKS Create/Delete Nodegroup Deferrable mode (#32165)``
* ``Deferrable mode for ECS operators (#31881)``
* ``feature: AWS - GlueJobOperator - job_poll_interval (#32147)``
* ``Added 'AzureBlobStorageToS3Operator' transfer operator (#32270)``
* ``Introduce a base class for aws triggers (#32274)``

Bug Fixes
~~~~~~~~~

* ``bugfix: break down run+wait method in ECS operator (#32104)``
* ``Handle 'UnboundLocalError' while parsing invalid 's3_url' (#32120)``
* ``Fix 'LambdaInvokeFunctionOperator' payload parameter type (#32259)``
* ``Bug fix GCSToS3Operator: avoid 'ValueError' when 'replace=False' with files already in S3 (#32322)``

Misc
~~~~

* ``Deprecate 'delimiter' param and source object's wildcards in GCS, introduce 'match_glob' param. (#31261)``
* ``aws waiter util: log status info with error level on waiter error (#32247)``
* ``rewrite method used in ecs to fetch less logs (#31786)``
* ``Refactor Eks Create Cluster Operator code (#31960)``
* ``Use a waiter in 'AthenaHook' (#31942)``
* ``Add 'on_finish_action' to 'KubernetesPodOperator' (#30718)``
* ``Add default_deferrable config (#31712)``
* ``deprecate arbitrary parameter passing to RDS hook (#32352)``
* ``quick fix on RDS operator to prevent parameter collision (#32436)``
* ``Remove ability to specify arbitrary hook params in AWS RDS trigger (#32386)``
* ``Only update crawler tags if present in config dict (#32331)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "add deferrable mode for 'AthenaOperator' (#32110)" (#32172)``
   * ``add deferrable mode for 'AthenaOperator' (#32110)``
   * ``D205 Support - Auto-fixes and Stragglers (#32212)``
   * ``D205 Support - Providers: Amazon/AWS (#32224)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Minor name change for the util wait method. (#32152)``
   * ``Clean up string concatenation (#32129)``
   * ``cleanup Amazon CHANGELOG.rst (#32031)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: Stragglers and new additions (#32447)``
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``

8.2.0
.....

.. note::
  This release dropped support for Python 3.7


Features
~~~~~~~~

* ``Add deferrable option to EmrTerminateJobFlowOperator (#31646)``
* ``Add Deferrable option to EmrCreateJobFlowOperator (#31641)``
* ``Add deferrable mode to 'BatchSensor'  (#30279)``
* ``Add deferrable mode for S3KeySensor (#31018)``
* ``Add Deferrable mode to Emr Add Steps operator (#30928)``
* ``Add deferrable mode in Redshift delete cluster (#30244)``
* ``Add deferrable mode to AWS glue operators (Job & Crawl) (#30948)``
* ``Add deferrable param in BatchOperator (#30865)``
* ``Add Deferrable Mode to RedshiftCreateClusterSnapshotOperator (#30856)``
* ``Deferrable mode for EksCreateFargateProfileOperator and EksDeleteFargateProfileOperator (#31657)``
* ``allow anonymous AWS access (#31659)``
* ``Support of wildcard in S3ListOperator and S3ToGCSOperator (#31640)``
* ``Add 'deferrable' param in 'EmrContainerSensor' (#30945)``
* ``Add realtime container execution logs for BatchOperator (#31837)``

Bug Fixes
~~~~~~~~~

* ``Various fixes on ECS run task operator (#31838)``
* ``fix return values on glue operators deferrable mode (#31694)``
* ``Add back missing AsyncIterator import (#31710)``
* ``Use a continuation token to get logs in ecs (#31824)``
* ``Fetch status in while loop so as to not exit too early (#31804)``
* ``[AWS hook] use provided client to get the official waiter on fallback (#31748)``
* ``handle missing LogUri in emr 'describe_cluster' API response (#31482)``

Misc
~~~~

* ``Add Python 3.11 support (#27264)``
* ``Added config template field to EmrServerlessStartJobOperator (#31746)``
* ``Add null check for host in Amazon Redshift connection (#31567)``
* ``add workgroup to templated fields (#31574)``
* ``Add docstring and signature for _read_remote_logs (#31623)``
* ``Deprecate 'wait_for_completion' from 'EcsRegisterTaskDefinitionOperator' and 'EcsDeregisterTaskDefinitionOperator' (#31884)``
* ``Remove Python 3.7 support (#30963)``
* ``Change Deferrable implementation for RedshiftResumeClusterOperator to follow standard (#30864)``
* ``Change Deferrable implementation for RedshiftPauseClusterOperator to follow standard (#30853)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check (#31742)``
   * ``Add D400 pydocstyle check - Amazon provider only (#31423)``
   * ``AWS system test example_dynamodb_to_s3: add retry when fecthing the export time (#31388)``
   * ``Amazon provider docstring improvements (#31729)``
   * ``Replace spelling directive with spelling:word-list (#31752)``
   * ``Remove aws unused code (#31610)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``
   * ``Add discoverability for triggers in provider.yaml (#31576)``

8.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``DynamoDBToS3Operator - Add a feature to export the table to a point in time. (#31142)``
* ``Add deferrable param in SageMakerTransformOperator (#31063)``
* ``Add deferrable param in SageMakerTrainingOperator (#31042)``
* ``Add deferrable param in SageMakerProcessingOperator (#31062)``
* ``Add IAM authentication to Amazon Redshift Connection by AWS Connection (#28187)``
* ``'StepFunctionStartExecutionOperator': get logs in case of failure (#31072)``
* ``Add on_kill to EMR Serverless Job Operator (#31169)``
* ``Add Deferrable Mode for EC2StateSensor (#31130)``

Bug Fixes
~~~~~~~~~

* ``bigfix: EMRHook  Loop through paginated response to check for cluster id (#29732)``

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Add template field to S3ToRedshiftOperator (#30781)``
* ``Add extras links to some more EMR Operators and Sensors (#31032)``
* ``Add retries to S3 delete_bucket (#31192)``
* ``Add tags param in RedshiftCreateClusterSnapshotOperator (#31006)``
* ``improve/fix glue job logs printing (#30886)``
* ``Import aiobotocore only if deferrable is true (#31094)``
* ``Update return types of 'get_key' methods on 'S3Hook' (#30923)``
* ``Support 'shareIdentifier' in BatchOperator (#30829)``
* ``BaseAWS - Override client when resource_type is user to get custom waiters (#30897)``
* ``Add future-compatible mongo Hook typing (#31289)``
* ``Handle temporary credentials when resource_type is used to get custom waiters (#31333)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move TaskInstanceKey to a separate file (#31033)``
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``DynamoDBToS3Operator - Add feature to export table to a point in time (#30501)``
   * ``Revert "DynamoDBToS3Operator - Add feature to export table to a point in time (#30501)" (#31139)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Bring back detection of implicit single-line string concatenation (#31270)``
   * ``Fix AWS system test example_dynamodb (#31395)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Fix AWS system test example_dynamodb_to_s3 (#31362)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

8.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In this version of the provider, deprecated GCS hook's parameter ``delegate_to`` is removed from the following operators: ``GCSToS3Operator``, ``GlacierToGCSOperator`` and ``GoogleApiToS3Operator``.
  Impersonation can be achieved instead by utilizing the ``impersonation_chain`` param.

  Removed deprecated parameter ``google_cloud_storage_conn_id`` from ``GCSToS3Operator``, ``gcp_conn_id`` should be used instead.

  Removed deprecated parameter ``max_tries`` from the Athena & EMR hook & operators in favor of ``max_polling_attempts``.

  Removed deprecated method ``waiter`` from emr hook in favor of the more generic ``airflow.providers.amazon.aws.utils.waiter.waiter``

  Removed deprecated unused parameter ``cluster_identifier`` from Redshift Cluster's hook method ``get_cluster_snapshot_status``

  Removed deprecated method ``find_processing_job_by_name`` from Sagemaker hook, use ``count_processing_jobs_by_name`` instead.

  Removed deprecated module ``airflow.providers.amazon.aws.operators.aws_lambda`` in favor of ``airflow.providers.amazon.aws.operators.lambda_function``

  Removed EcsOperator in favor of EcsRunTaskOperator.
  EcsTaskLogFetcher and EcsProtocol should be imported from the hook.

  Removed AwsLambdaInvokeFunctionOperator in favor of LambdaInvokeFunctionOperator.

  Removed deprecated param ``await_result`` from RedshiftDataOperator in favor of ``wait_for_completion``.
  Some methods from this operator should be imported from the hook instead.

  Removed deprecated ``RedshiftSQLOperator`` in favor of the generic ``SQLExecuteQueryOperator``.
  The parameter that was passed as ``redshift_conn_id`` needs to be changed to ``conn_id``, and the behavior should stay the same.

  Removed deprecated method ``get_conn_uri`` from secrets manager in favor of ``get_conn_value``
  Also removed deprecated method ``get_conn_uri`` from systems manager. ``deserialize_connection(...).get_uri()`` should be used instead.

  Removed deprecated and unused param ``s3_conn_id`` from ``ImapAttachmentToS3Operator``, ``MongoToS3Operator`` and ``S3ToSFTPOperator``.

* ``remove delegate_to from GCP operators and hooks (#30748)``
* ``Remove deprecated code from Amazon provider (#30755)``

Features
~~~~~~~~

* ``add a stop operator to emr serverless (#30720)``
* ``SqlToS3Operator - Add feature to partition SQL table (#30460)``
* ``New AWS sensor — DynamoDBValueSensor (#28338)``
* ``Add a "force" option to emr serverless stop/delete operator (#30757)``
* ``Add support for deferrable operators in AMPP (#30032)``

Bug Fixes
~~~~~~~~~

* ``Fixed logging issue (#30703)``
* ``DynamoDBHook - waiter_path() to consider 'resource_type' or 'client_type' (#30595)``
* ``Add ability to override waiter delay in EcsRunTaskOperator (#30586)``
* ``Add support in AWS Batch Operator for multinode jobs (#29522)``
* ``AWS logs. Exit fast when 3 consecutive responses are returned from AWS Cloudwatch logs (#30756)``
* ``Fix async conn for none aws_session_token (#30868)``

Misc
~~~~

* ``Remove @poke_mode_only from EmrStepSensor (#30774)``
* ``Organize Amazon providers docs index (#30541)``
* ``Remove duplicate param docstring in EksPodOperator (#30634)``
* ``Update AWS EMR Cluster Link to use the new dashboard (#30844)``
* ``Restore aiobotocore as optional dependency of amazon provider (#30874)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Decouple "job runner" from BaseJob ORM model (#30255)``
   * ``Upgrade ruff to 0.0.262 (#30809)``
   * ``fixes to system tests following obsolete cleanup (#30804)``
   * ``restore fallback to empty connection behavior (#30806)``
   * ``Prepare docs for adhoc release of providers (#30787)``
   * ``Prepare docs for ad-hoc release of Amazon provider (#30848)``

7.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix 'RedshiftResumeClusterOperator' deferrable implementation (#30370)``

Misc
~~~~

* ``Add more info to quicksight error messages (#30466)``
* ``add template field for s3 bucket (#30472)``
* ``Add s3_bucket to template fields in SFTP to S3 operator (#30444)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add AWS deferrable BatchOperator (#29300)``
   * ``Revert "Add AWS deferrable BatchOperator (#29300)" (#30489)``
   * ``Add mechanism to suspend providers (#30422)``

7.4.0
.....

Features
~~~~~~~~

* ``Add deferrable mode to 'RedshiftResumeClusterOperator' (#30090)``
* ``Add 'AwsToAwsBaseOperator' (#30044)``
* ``Add deferrable mode in RedshiftPauseClusterOperator (#28850)``
* ``Add support of a different AWS connection for DynamoDB (#29452)``
* ``Add 'EC2CreateInstanceOperator', 'EC2TerminateInstanceOperator' (#29548)``
* ``Make update config behavior optional in GlueJobOperator (#30162)``
* ``custom waiters with dynamic values, applied to appflow (#29911)``
* ``Support deleting the local log files when using remote logging (#29772)``

Misc
~~~~
* ``Move string enum class to utils module + add test (#29906)``
* ``Align cncf provider file names with AIP-21 (#29905)``
* ``rewrite polling code for appflow hook (#28869)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move and convert all AWS example dags to system tests (#30003)``
   * ``Remove aws async ci job (#30127)``

7.3.0
.....

Features
~~~~~~~~

* ``add num rows affected to Redshift Data API hook (#29797)``
* ``Add 'wait_for_completion' param in 'RedshiftCreateClusterOperator' (#29657)``
* ``Add Amazon Redshift-data to S3<>RS Transfer Operators (#27947)``
* ``Allow to specify which connection, variable or config are being looked up in the backend using *_lookup_pattern parameters (#29580)``
* ``Implement file credentials provider for AWS hook AssumeRoleWithWebIdentity (#29623)``
* ``Implement custom boto waiters for some EMR operators (#29822)``

Bug Fixes
~~~~~~~~~

* ``fix code checking job names in sagemaker (#29245)``
* ``Avoid emitting fallback message for S3TaskHandler if streaming logs (#29708)``
* ``Use waiters in ECS Operators instead of inner sensors (#29761)``

Misc
~~~~

* ``Impovements for RedshiftDataOperator: better error reporting and an ability to return SQL results (#29434)``
* ``Standardize AWS lambda naming (#29749)``
* ``AWS Glue job hook: Make s3_bucket parameter optional (#29659)``
* ``'RedshiftDataOperator' replace 'await_result' with 'wait_for_completion' (#29633)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix Amazon ECS Enums (#29871)``

7.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Explicitly handle exceptions raised by config parsing in AWS provider (#29587)``

Misc
~~~~

* ``Fix docstring for EcsRunTaskOperator region_name -> region (#29562)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Restore trigger logging (#29482)``
   * ``Revert "Enable individual trigger logging (#27758)" (#29472)``

7.2.0
.....

Features
~~~~~~~~

* ``Add option to wait for completion on the EmrCreateJobFlowOperator (#28827)``
* ``Add transfer operator S3 to (generic) SQL (#29085)``
* ``add retries to stop_pipeline on conflict (#29077)``
* ``Add log for AWS Glue Job Console URL (#28925)``
* ``Enable individual trigger logging (#27758)``

Bug Fixes
~~~~~~~~~

* ``fix: 'num_of_dpus' typehints- GlueJobHook/Operator (#29176)``
* ``Fix typo in DataSyncHook boto3 methods for create location in NFS and EFS (#28948)``
* ``Decrypt SecureString value obtained by SsmHook (#29142)``

Misc
~~~~

* ``log the observed status in redshift sensor (#29274)``
* ``Use thin/passthrough hook instead of one-liner hook method (#29252)``
* ``Move imports in AWS SqlToS3Operator transfer to callable function (#29045)``
* ``introduce base class for EKS sensors (#29053)``
* ``introduce a method to convert dictionaries to boto-style key-value lists (#28816)``
* ``Update provide_bucket_name() decorator to handle new conn_type (#28706)``
* ``uniformize getting hook through cached property in aws sensors (#29001)``
* ``Use boto3 intersphinx inventory in documentation/docstrings. (#28945)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``shorten other wait times in sys tests (#29254)``
   * ``Fix false-positive spellcheck failure (#29190)``

7.1.0
.....

Features
~~~~~~~~

* ``Add ''configuration_overrides'' to templated fields (#28920)``
* ``Add a new SSM hook and use it in the System Test context builder (#28755)``
* ``Add waiter config params to emr.add_job_flow_steps (#28464)``
* ``Add AWS Sagemaker Auto ML operator and sensor (#28472)``
* ``new operator to create a sagemaker experiment (#28837)``

Bug Fixes
~~~~~~~~~

* ``Avoid circular import from S3HookUriParseFailure (#28908)``
* ``Use compat for cached_property in AWS Batch modules (#28835)``
* ``Apply "unify bucket and key" before "provide bucket" (#28710)``

Misc
~~~~

* ``Update S3ToRedshiftOperator docs to inform users about multiple key functionality (#28705)``
* ``Refactor waiter function and improve unit tests (#28753)``
* ``Better exception raised in case of numpy missing (#28722)``
* ``Don't call get_connection from provide_bucket_name (#28716)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch to ruff for faster static checks (#28893)``


7.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

JSON secrets in the 'SecretsManagerBackend' are never interpreted as urlencoded. In ``5.x`` and ``6.x``, the
code would infer whether the JSON secret values were urlencoded based on context clues; now the unaltered
values are *always* used to construct ``Connection`` objects.

Pandas is now an optional dependency of the provider. The ``SqlToS3Operator`` and ``HiveToDynamoDBOperator``
require Pandas to be installed (you can install it automatically by adding ``[pandas]`` extra when installing
the provider.

* ``Make pandas dependency optional for Amazon Provider (#28505)``

Features
~~~~~~~~

* ``Deprecate 'full_url_mode' for SecretsManagerBackend; whether a secret is a JSON or URL is inferred (#27920)``
* ``Add execution role parameter to AddStepsOperator (#28484)``
* ``Add AWS SageMaker operator to register a model's version (#28024)``
* ``Add link for EMR Steps Sensor logs (#28180)``
* ``Add Amazon Elastic Container Registry (ECR) Hook (#28279)``
* ``Add EMR Notebook operators (#28312)``
* ``Create 'LambdaCreateFunctionOperator' and sensor (#28241)``
* ``Better support for Boto Waiters (#28236)``
* ``Amazon Provider Package user agent (#27823)``
* ``Allow waiter to be configured via EmrServerless Operators (#27784)``
* ``Add operators + sensor for aws sagemaker pipelines (#27786)``
* ``Update RdsHook docstrings to match correct argument names (#28108)``
* ``add some important log in aws athena hook (#27917)``
* ``Lambda hook: make runtime and handler optional (#27778)``

Bug Fixes
~~~~~~~~~

* ``Fix EmrAddStepsOperature wait_for_completion parameter is not working (#28052)``
* ``Correctly template Glue Jobs 'create_job_kwargs' arg (#28403)``
* ``Fix template rendered bucket_key in S3KeySensor (#28340)``
* ``Fix Type Error while using DynamoDBToS3Operator (#28158)``
* ``AWSGlueJobHook updates job configuration if it exists (#27893)``
* ``Fix GlueCrawlerOperature failure when using tags (#28005)``

Misc
~~~~

* ``Fix S3KeySensor documentation (#28297)``
* ``Improve docstrings for 'AwsLambdaInvokeFunctionOperator' (#28233)``
* ``Remove outdated compat imports/code from providers (#28507)``
* ``add description of breaking changes (#28582)``
* ``[misc] Get rid of 'pass' statement in conditions (#27775)``
* ``[misc] Replace XOR '^' conditions by 'exactly_one' helper in providers (#27858)``

6.2.0
.....

Features
~~~~~~~~

* ``Use Boto waiters instead of customer _await_status method for RDS Operators (#27410)``
* ``Handle transient state errors in 'RedshiftResumeClusterOperator' and 'RedshiftPauseClusterOperator' (#27276)``
* ``Add retry option in RedshiftDeleteClusterOperator to retry when an operation is running in the cluster (#27820)``

Bug Fixes
~~~~~~~~~

* ``Correct job name matching in SagemakerProcessingOperator (#27634)``
* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``System Test for EMR (AIP-47) (#27286)``
   * ``Prepare for follow-up release for November providers (#27774)``

6.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``
* ``Replace urlparse with urlsplit (#27389)``

Features
~~~~~~~~

* ``Add info about JSON Connection format for AWS SSM Parameter Store Secrets Backend (#27134)``
* ``Add default name to EMR Serverless jobs (#27458)``
* ``Adding 'preserve_file_name' param to 'S3Hook.download_file' method (#26886)``
* ``Add GlacierUploadArchiveOperator (#26652)``
* ``Add RdsStopDbOperator and RdsStartDbOperator (#27076)``
* ``'GoogleApiToS3Operator' : add 'gcp_conn_id' to template fields (#27017)``
* ``Add SQLExecuteQueryOperator (#25717)``
* ``Add information about Amazon Elastic MapReduce Connection (#26687)``
* ``Add BatchOperator template fields (#26805)``
* ``Improve testing AWS Connection response (#26953)``

Bug Fixes
~~~~~~~~~

* ``SagemakerProcessingOperator stopped honoring 'existing_jobs_found' (#27456)``
* ``CloudWatch task handler doesn't fall back to local logs when Amazon CloudWatch logs aren't found (#27564)``
* ``Fix backwards compatibility for RedshiftSQLOperator (#27602)``
* ``Fix typo in redshift sql hook get_ui_field_behaviour (#27533)``
* ``Fix example_emr_serverless system test (#27149)``
* ``Fix param in docstring RedshiftSQLHook get_table_primary_key method (#27330)``
* ``Adds s3_key_prefix to template fields (#27207)``
* ``Fix assume role if user explicit set credentials (#26946)``
* ``Fix failure state in waiter call for EmrServerlessStartJobOperator. (#26853)``
* ``Fix a bunch of deprecation warnings AWS tests (#26857)``
* ``Fix null strings bug in SqlToS3Operator in non parquet formats (#26676)``
* ``Sagemaker hook: remove extra call at the end when waiting for completion (#27551)``
* ``ECS Buglette (#26921)``
* ``Avoid circular imports in AWS Secrets Backends if obtain secrets from config (#26784)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``sagemaker operators: mutualize init of aws_conn_id (#27579)``
   * ``Upgrade dependencies in order to avoid backtracking (#27531)``
   * ``Code quality improvements on sagemaker operators/hook (#27453)``
   * ``Update old style typing (#26872)``
   * ``System test for SQL to S3 Transfer (AIP-47) (#27097)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Convert emr_eks example dag to system test (#26723)``
   * ``System test for Dynamo DB (#26729)``
   * ``ECS System Test (#26808)``
   * ``RDS Instance System Tests (#26733)``

6.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In this version of provider Amazon S3 Connection (``conn_type="s3"``) removed due to the fact that it was always
  an alias to AWS connection ``conn_type="aws"``
  In practice the only impact is you won't be able to ``test`` the connection in the web UI / API.
  In order to restore ability to test connection you need to change connection type from **Amazon S3** (``conn_type="s3"``)
  to **Amazon Web Services** (``conn_type="aws"``) manually.

* ``Remove Amazon S3 Connection Type (#25980)``

Features
~~~~~~~~

* ``Add RdsDbSensor to amazon provider package (#26003)``
* ``Set template_fields on RDS operators (#26005)``
* ``Auto tail file logs in Web UI (#26169)``

Bug Fixes
~~~~~~~~~

* ``Fix SageMakerEndpointConfigOperator's return value (#26541)``
* ``EMR Serverless Fix for Jobs marked as success even on failure (#26218)``
* ``Fix AWS Connection warn condition for invalid 'profile_name' argument (#26464)``
* ``Athena and EMR operator max_retries mix-up fix (#25971)``
* ``Fixes SageMaker operator return values (#23628)``
* ``Remove redundant catch exception in Amazon Log Task Handlers (#26442)``

Misc
~~~~

* ``Remove duplicated connection-type within the provider (#26628)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Redshift to S3 and S3 to Redshift System test (AIP-47) (#26613)``
   * ``Convert example_eks_with_fargate_in_one_step.py and example_eks_with_fargate_profile to AIP-47 (#26537)``
   * ``Redshift System Test (AIP-47) (#26187)``
   * ``GoogleAPIToS3Operator System Test (AIP-47) (#26370)``
   * ``Convert EKS with Nodegroups sample DAG to a system test (AIP-47) (#26539)``
   * ``Convert EC2 sample DAG to system test (#26540)``
   * ``Convert S3 example DAG to System test (AIP-47) (#26535)``
   * ``Convert 'example_eks_with_nodegroup_in_one_step' sample DAG to system test (AIP-47) (#26410)``
   * ``Migrate DMS sample dag to system test (#26270)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``D400 first line should end with period batch02 (#25268)``
   * ``Change links to 'boto3' documentation (#26708)``

5.1.0
.....


Features
~~~~~~~~

* ``Additional mask aws credentials (#26014)``
* ``Add RedshiftDeleteClusterSnapshotOperator (#25975)``
* ``Add redshift create cluster snapshot operator (#25857)``
* ``Add common-sql lower bound for common-sql (#25789)``
* ``Allow AWS Secrets Backends use AWS Connection capabilities (#25628)``
* ``Implement 'EmrEksCreateClusterOperator' (#25816)``
* ``Improve error handling/messaging around bucket exist check (#25805)``

Bug Fixes
~~~~~~~~~

* ``Fix display aws connection info (#26025)``
* ``Fix 'EcsBaseOperator' and 'EcsBaseSensor' arguments (#25989)``
* ``Fix RDS system test (#25839)``
* ``Avoid circular import problems when instantiating AWS SM backend (#25810)``
* ``fix bug construction of Connection object in version 5.0.0rc3 (#25716)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix EMR serverless system test (#25969)``
   * ``Add 'output' property to MappedOperator (#25604)``
   * ``Add Airflow specific warning classes (#25799)``
   * ``Replace SQL with Common SQL in pre commit (#26058)``
   * ``Hook into Mypy to get rid of those cast() (#26023)``
   * ``Raise an error on create bucket if use regional endpoint for us-east-1 and region not set (#25945)``
   * ``Update AWS system tests to use SystemTestContextBuilder (#25748)``
   * ``Convert Quicksight Sample DAG to System Test (#25696)``
   * ``Consolidate to one 'schedule' param (#25410)``

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Avoid requirement that AWS Secret Manager JSON values be urlencoded. (#25432)``
* ``Remove deprecated modules (#25543)``
* ``Resolve Amazon Hook's 'region_name' and 'config' in wrapper (#25336)``
* ``Resolve and validate AWS Connection parameters in wrapper (#25256)``
* ``Standardize AwsLambda (#25100)``
* ``Refactor monolithic ECS Operator into Operators, Sensors, and a Hook (#25413)``
* ``Remove deprecated modules from Amazon provider package (#25609)``

Features
~~~~~~~~

* ``Add EMR Serverless Operators and Hooks (#25324)``
* ``Hide unused fields for Amazon Web Services connection (#25416)``
* ``Enable Auto-incrementing Transform job name in SageMakerTransformOperator (#25263)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``
* ``SQSPublishOperator should allow sending messages to a FIFO Queue (#25171)``
* ``Glue Job Driver logging (#25142)``
* ``Bump typing-extensions and mypy for ParamSpec (#25088)``
* ``Enable multiple query execution in RedshiftDataOperator (#25619)``

Bug Fixes
~~~~~~~~~

* ``Fix S3Hook transfer config arguments validation (#25544)``
* ``Fix BatchOperator links on wait_for_completion = True (#25228)``
* ``Makes changes to SqlToS3Operator method _fix_int_dtypes (#25083)``
* ``refactor: Deprecate parameter 'host' as an extra attribute for the connection. Depreciation is happening in favor of 'endpoint_url' in extra. (#25494)``
* ``Get boto3.session.Session by appropriate method (#25569)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``System test for EMR Serverless  (#25559)``
   * ``Convert Local to S3 example DAG to System Test (AIP-47) (#25345)``
   * ``Convert ECS Fargate Sample DAG to System Test (#25316)``
   * ``Sagemaker System Tests - Part 3 of 3 - example_sagemaker_endpoint.py (AIP-47) (#25134)``
   * ``Convert RDS Export Sample DAG to System Test (AIP-47) (#25205)``
   * ``AIP-47 - Migrate redshift DAGs to new design #22438 (#24239)``
   * ``Convert Glue Sample DAG to System Test (#25136)``
   * ``Convert the batch sample dag to system tests (AIP-47) (#24448)``
   * ``Migrate datasync sample dag to system tests (AIP-47) (#24354)``
   * ``Sagemaker System Tests - Part 2 of 3 - example_sagemaker.py (#25079)``
   * ``Migrate lambda sample dag to system test (AIP-47) (#24355)``
   * ``SageMaker system tests - Part 1 of 3 - Prep Work (AIP-47) (#25078)``
   * ``Prepare docs for new providers release (August 2022) (#25618)``

4.1.0
.....

Features
~~~~~~~~

* ``Add test_connection method to AWS hook (#24662)``
* ``Add AWS operators to create and delete RDS Database (#24099)``
* ``Add batch option to 'SqsSensor' (#24554)``
* ``Add AWS Batch & AWS CloudWatch Extra Links (#24406)``
* ``Refactoring EmrClusterLink and add for other AWS EMR Operators (#24294)``
* ``Move all SQL classes to common-sql provider (#24836)``
* ``Amazon appflow (#24057)``
* ``Make extra_args in S3Hook immutable between calls (#24527)``

Bug Fixes
~~~~~~~~~

* ``Refactor and fix AWS secret manager invalid exception (#24898)``
* ``fix: RedshiftDataHook and RdsHook not use cached connection (#24387)``
* ``Fix links to sources for examples (#24386)``
* ``Fix S3KeySensor. See #24321 (#24378)``
* ``Fix: 'emr_conn_id' should be optional in 'EmrCreateJobFlowOperator' (#24306)``
* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Convert RDS Event and Snapshot Sample DAGs to System Tests (#24932)``
   * ``Convert Step Functions Example DAG to System Test (AIP-47) (#24643)``
   * ``Update AWS Connection docs and deprecate some extras (#24670)``
   * ``Remove 'xcom_push' flag from providers (#24823)``
   * ``Align Black and blacken-docs configs (#24785)``
   * ``Restore Optional value of script_location (#24754)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Use our yaml util in all providers (#24720)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``
   * ``Convert SQS Sample DAG to System Test (#24513)``
   * ``Convert Cloudformation Sample DAG to System Test (#24447)``
   * ``Convert SNS Sample DAG to System Test (#24384)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add partition related methods to GlueCatalogHook: (#23857)``
* ``Add support for associating  custom tags to job runs submitted via EmrContainerOperator (#23769)``
* ``Add number of node params only for single-node cluster in RedshiftCreateClusterOperator (#23839)``

Bug Fixes
~~~~~~~~~

* ``fix: StepFunctionHook ignores explicit set 'region_name' (#23976)``
* ``Fix Amazon EKS example DAG raises warning during Imports (#23849)``
* ``Move string arg evals to 'execute()' in 'EksCreateClusterOperator' (#23877)``
* ``fix: patches #24215. Won't raise KeyError when 'create_job_kwargs' contains the 'Command' key. (#24308)``

Misc
~~~~

* ``Light Refactor and Clean-up AWS Provider (#23907)``
* ``Update sample dag and doc for RDS (#23651)``
* ``Reformat the whole AWS documentation (#23810)``
* ``Replace "absolute()" with "resolve()" in pathlib objects (#23675)``
* ``Apply per-run log templates to log handlers (#24153)``
* ``Refactor GlueJobHook get_or_create_glue_job method. (#24215)``
* ``Update the DMS Sample DAG and Docs (#23681)``
* ``Update doc and sample dag for Quicksight (#23653)``
* ``Update doc and sample dag for EMR Containers (#24087)``
* ``Add AWS project structure tests (re: AIP-47) (#23630)``
* ``Add doc and sample dag for GCSToS3Operator (#23730)``
* ``Remove old Athena Sample DAG (#24170)``
* ``Clean up f-strings in logging calls (#23597)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Introduce 'flake8-implicit-str-concat' plugin to static checks (#23873)``
   * ``pydocstyle D202 added (#24221)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

3.4.0
.....

Features
~~~~~~~~

* ``Add Quicksight create ingestion Hook and Operator (#21863)``
* ``Add default 'aws_conn_id' to SageMaker Operators #21808 (#23515)``
* ``Add RedshiftCreateClusterOperator``
* ``Add 'S3CreateObjectOperator' (#22758)``
* ``Add 'RedshiftDeleteClusterOperator' support (#23563)``

Bug Fixes
~~~~~~~~~

* ``Fix conn close error on retrieving log events (#23470)``
* ``Fix LocalFilesystemToS3Operator and S3CreateObjectOperator to support full s3:// style keys (#23180)``
* ``Fix attempting to reattach in 'ECSOperator' (#23370)``
* ``Fix doc build failure on main (#23240)``
* ``Fix "Chain not supported for different length Iterable"``
* ``'S3Hook': fix 'load_bytes' docstring (#23182)``
* ``Deprecate 'S3PrefixSensor' and 'S3KeySizeSensor' in favor of 'S3KeySensor' (#22737)``
* ``Allow back script_location in Glue to be None (#23357)``

Misc
~~~~

* ``Add doc and example dag for Amazon SQS Operators (#23312)``
* ``Add doc and sample dag for S3CopyObjectOperator and S3DeleteObjectsOperator (#22959)``
* ``Add sample dag and doc for S3KeysUnchangedSensor``
* ``Add doc and sample dag for S3FileTransformOperator``
* ``Add doc and example dag for AWS Step Functions Operators``
* ``Add sample dag and doc for S3ListOperator (#23449)``
* ``Add doc and sample dag for EC2 (#23547)``
* ``Add sample dag and doc for S3ListPrefixesOperator (#23448)``
* ``Amazon Sagemaker Sample DAG and docs update (#23256)``
* ``Update the Athena Sample DAG and Docs (#23428)``
* ``Update sample dag and doc for Datasync (#23511)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix new MyPy errors in main (#22884)``
   * ``Clean up in-line f-string concatenation (#23591)``
   * ``Update docs Amazon Glacier Docs (#23372)``
   * ``Bump pre-commit hook versions (#22887)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``


3.3.0
.....

Features
~~~~~~~~

* ``Pass custom headers through in SES email backend (#22667)``
* ``Update secrets backends to use get_conn_value instead of get_conn_uri (#22348)``


Misc
~~~~

* ``Add doc and sample dag for SqlToS3Operator (#22603)``
* ``Adds HiveToDynamoDB Transfer Sample DAG and Docs (#22517)``
* ``Add doc and sample dag for MongoToS3Operator (#22575)``
* ``Add doc for LocalFilesystemToS3Operator (#22574)``
* ``Add doc and example dag for AWS CloudFormation Operators (#22533)``
* ``Add doc and sample dag for S3ToFTPOperator and FTPToS3Operator (#22534)``
* ``GoogleApiToS3Operator: update sample dag and doc (#22507)``
* ``SalesforceToS3Operator: update sample dag and doc (#22489)``


3.2.0
.....

Features
~~~~~~~~

* ``Add arguments to filter list: start_after_key, from_datetime, to_datetime, object_filter callable (#22231)``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``
* ``ImapAttachmentToS3Operator: fix it, update sample dag and update doc (#22351)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update sample dag and doc for S3CreateBucketOperator, S3PutBucketTaggingOperator, S3GetBucketTaggingOperator, S3DeleteBucketTaggingOperator, S3DeleteBucketOperator (#22312)``
   * ``Add docs and example dag for AWS Glue (#22295)``
   * ``Update doc and sample dag for S3ToSFTPOperator and SFTPToS3Operator (#22313)``

3.1.1
.....

Features
~~~~~~~~

* ``Added AWS RDS sensors (#21231)``
* ``Added AWS RDS operators (#20907)``
* ``Add RedshiftDataHook (#19137)``
* ``Feature: Add invoke lambda function operator (#21686)``
* ``Add JSON output on SqlToS3Operator (#21779)``
* ``Add SageMakerDeleteModelOperator (#21673)``
* ``Added Hook for Amazon RDS. Added 'boto3_stub' library for autocomplete. (#20642)``
* ``Added SNS example DAG and rst (#21475)``
* ``retry on very specific eni provision failures (#22002)``
* ``Configurable AWS Session Factory (#21778)``
* ``S3KeySensor to use S3Hook url parser (#21500)``
* ``Get log events after sleep to get all logs (#21574)``
* ``Use temporary file in GCSToS3Operator (#21295)``

Bug Fixes
~~~~~~~~~

* ``AWS RDS integration fixes (#22125)``
* ``Fix the Type Hints in ''RedshiftSQLOperator'' (#21885)``
* ``Bug Fix - S3DeleteObjectsOperator will try and delete all keys (#21458)``
* ``Fix Amazon SES emailer signature (#21681)``
* ``Fix EcsOperatorError, so it can be loaded from a picklefile (#21441)``
* ``Fix RedshiftDataOperator and update doc (#22157)``
* ``Bugfix for retrying on provision failuers(#22137)``
* ``If uploading task logs to S3 fails, retry once (#21981)``
* ``Bug-fix GCSToS3Operator (#22071)``
* ``fixes query status polling logic (#21423)``
* ``use different logger to avoid duplicate log entry (#22256)``

Misc
~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``
* ``Support for Python 3.10``
* ``[doc] Improve s3 operator example by adding task upload_keys (#21422)``
* ``Rename 'S3' hook name to 'Amazon S3' (#21988)``
* ``Add template fields to DynamoDBToS3Operator (#22080)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``additional information in the ECSOperator around support of launch_type=EXTERNAL (#22093)``
   * ``Add map_index to XCom model and interface (#22112)``
   * ``Add sample dags and update doc for RedshiftClusterSensor, RedshiftPauseClusterOperator and RedshiftResumeClusterOperator (#22128)``
   * ``Add sample dag and doc for RedshiftToS3Operator (#22060)``
   * ``Add docs and sample dags for AWS Batch (#22010)``
   * ``Add documentation for Feb Providers release (#22056)``
   * ``Change BaseOperatorLink interface to take a ti_key, not a datetime (#21798)``
   * ``Add pre-commit check for docstring param types (#21398)``
   * ``Resolve mypy issue in athena example dag (#22020)``
   * ``refactors polling logic for athena queries (#21488)``
   * ``EMR on EKS Sample DAG and Docs Update (#22095)``
   * ``Dynamo to S3 Sample DAG and Docs (#21920)``
   * ``Cleanup RedshiftSQLOperator documentation (#21976)``
   * ``Move S3ToRedshiftOperator documentation to transfer dir (#21975)``
   * ``Protect against accidental misuse of XCom.get_value() (#22244)``
   * ``Update ECS sample DAG and Docs to new standards (#21828)``
   * ``Update EKS sample DAGs and docs (#21523)``
   * ``EMR Sample DAG and Docs Update (#22189)``

3.0.0
.....

Breaking Changes
~~~~~~~~~~~~~~~~

The CloudFormationCreateStackOperator and CloudFormationDeleteStackOperator
used ``params`` as one of the constructor arguments, however this name clashes with params
argument ``params`` field which is processed differently in Airflow 2.2.
The ``params`` parameter has been renamed to ``cloudformation_parameters`` to make it non-ambiguous.

Any usage of CloudFormationCreateStackOperator and CloudFormationDeleteStackOperator where
``params`` were passed, should be changed to use ``cloudformation_parameters`` instead.

* ``Rename params to cloudformation_parameter in CloudFormation operators. (#20989)``

Features
~~~~~~~~

* ``[SQSSensor] Add opt-in to disable auto-delete messages (#21159)``
* ``Create a generic operator SqlToS3Operator and deprecate the MySqlToS3Operator.  (#20807)``
* ``Move some base_aws logging from info to debug level (#20858)``
* ``AWS: Adds support for optional kwargs in the EKS Operators (#20819)``
* ``AwsAthenaOperator: do not generate ''client_request_token'' if not provided (#20854)``
* ``Add more SQL template fields renderers (#21237)``
* ``Add conditional 'template_fields_renderers' check for new SQL lexers (#21403)``


Bug fixes
~~~~~~~~~

* ``fix: cloudwatch logs fetch logic (#20814)``
* ``Fix all Amazon Provider MyPy errors (#20935)``
* ``Bug fix in AWS glue operator related to num_of_dpus #19787 (#21353)``
* ``Fix to check if values are integer or float and convert accordingly. (#21277)``


Misc
~~~~

* ``Alleviate import warning for 'EmrClusterLink' in deprecated AWS module (#21195)``
* ``Rename amazon EMR hook name (#20767)``
* ``Standardize AWS SQS classes names (#20732)``
* ``Standardize AWS Batch naming (#20369)``
* ``Standardize AWS Redshift naming (#20374)``
* ``Standardize DynamoDB naming (#20360)``
* ``Standardize AWS ECS naming (#20332)``
* ``Refactor operator links to not create ad hoc TaskInstances (#21285)``
* ``eks_hook log level fatal -> FATAL  (#21427)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' directives from 'SqlToS3Operator' (#21079)``
   * ``Remove a few stray ':type's in docs (#21014)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Remove all "fake" stub files (#20936)``
   * ``Fix MyPy issues in AWS Sensors (#20863)``
   * ``Explain stub files are introduced for Mypy errors in examples (#20827)``
   * ``Fix mypy in providers/aws/hooks (#20353)``
   * ``Fix MyPy issues in AWS Sensors (#20717)``
   * ``Fix MyPy in Amazon provider for Sagemaker operator (#20715)``
   * ``Fix MyPy errors for Amazon DMS in hooks and operator (#20710)``
   * ``Fix MyPy issues in ''airflow/providers/amazon/aws/transfers'' (#20708)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.6.0
.....

Features
~~~~~~~~

* ``Add aws_conn_id to DynamoDBToS3Operator (#20363)``
* ``Add RedshiftResumeClusterOperator and RedshiftPauseClusterOperator (#19665)``
* ``Added function in AWSAthenaHook to get s3 output query results file URI  (#20124)``
* ``Add sensor for AWS Batch (#19850) (#19885)``
* ``Add state details to EMR container failure reason (#19579)``
* ``Add support to replace S3 file on MySqlToS3Operator (#20506)``

Bug Fixes
~~~~~~~~~

* ``Fix backwards compatibility issue in AWS provider's _get_credentials (#20463)``
* ``Fix deprecation messages after splitting redshift modules (#20366)``
* ``ECSOperator: fix KeyError on missing exitCode (#20264)``
* ``Bug fix in AWS glue operator when specifying the WorkerType & NumberOfWorkers (#19787)``

Misc
~~~~

* ``Organize Sagemaker classes in Amazon provider (#20370)``
* ``move emr_container hook (#20375)``
* ``Standardize AWS Athena naming (#20305)``
* ``Standardize AWS EKS naming (#20354)``
* ``Standardize AWS Glue naming (#20372)``
* ``Standardize Amazon SES naming (#20367)``
* ``Standardize AWS CloudFormation naming (#20357)``
* ``Standardize AWS Lambda naming (#20365)``
* ``Standardize AWS Kinesis/Firehose naming (#20362)``
* ``Standardize Amazon SNS naming (#20368)``
* ``Split redshift sql and cluster objects (#20276)``
* ``Organize EMR classes in Amazon provider (#20160)``
* ``Rename DataSync Hook and Operator (#20328)``
* ``Deprecate passing execution_date to XCom methods (#19825)``
* ``Organize Dms classes in Amazon provider (#20156)``
* ``Organize S3 Classes in Amazon Provider (#20167)``
* ``Organize Step Function classes in Amazon provider (#20158)``
* ``Organize EC2 classes in Amazon provider (#20157)``
* ``Move to watchtower 2.0.1 (#19907)``
* ``Fix mypy aws example dags (#20497)``
* ``Delete pods by default in KubernetesPodOperator (#20575)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix mypy errors in aws/transfers (#20403)``
   * ``Fix mypy errors in aws/sensors (#20402)``
   * ``Fix mypy errors in providers/amazon/aws/operators (#20401)``
   * ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix static checks on few other not sorted stub files (#20572)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Fix mypy errors in amazon aws transfer (#20590)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.5.0
.....

Features
~~~~~~~~

* ``Adding support for using ''client_type'' API for interacting with EC2 and support filters (#9011)``
* ``Do not check for S3 key before attempting download (#19504)``
* ``MySQLToS3Operator  actually allow writing parquet files to s3. (#19094)``

Bug Fixes
~~~~~~~~~

* ``Amazon provider remove deprecation, second try (#19815)``
* ``Catch AccessDeniedException in AWS Secrets Manager Backend (#19324)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix duplicate changelog entries (#19759)``
   * ``Revert 'Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)' (#19791)``
   * ``Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)``
   * ``Cleanup of start_date and default arg use for Amazon example DAGs (#19237)``
   * ``Remove remaining 'pylint: disable' comments (#19541)``

2.4.0
.....

Features
~~~~~~~~

* ``MySQLToS3Operator add support for parquet format (#18755)``
* ``Add RedshiftSQLHook, RedshiftSQLOperator (#18447)``
* ``Remove extra postgres dependency from AWS Provider (#18844)``
* ``Removed duplicated code on S3ToRedshiftOperator (#18671)``

Bug Fixes
~~~~~~~~~

* ``Fixing ses email backend (#18042)``
* ``Fixup string concatenations (#19099)``
* ``Update S3PrefixSensor to support checking multiple prefixes within a bucket (#18807)``
* ``Move validation of templated input params to run after the context init (#19048)``
* ``fix SagemakerProcessingOperator ThrottlingException (#19195)``
* ``Fix S3ToRedshiftOperator (#19358)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``More f-strings (#18855)``
   * ``Prepare documentation for RC2 Amazon Provider release for September (#18830)``
   * ``Doc: Fix typos in variable and comments (#19349)``
   * ``Remove duplicated entries in changelog (#19331)``
   * ``Prepare documentation for October Provider's release (#19321)``

2.3.0
.....

The Redshift operators in this version require at least ``2.3.0`` version of the Postgres Provider. This is
reflected in the ``[postgres]`` extra, but extras do not guarantee that the right version of
dependencies is installed (depending on the installation method). In case you have problems with
running Redshift operators, upgrade ``apache-airflow-providers-postgres`` provider to at least
version 2.3.0.


Features
~~~~~~~~

* ``Add IAM Role Credentials to S3ToRedshiftTransfer and RedshiftToS3Transfer (#18156)``
* ``Adding missing 'replace' param in docstring (#18241)``
* ``Added upsert method on S3ToRedshift operator (#18027)``
* ``Add Spark to the EMR cluster for the job flow examples (#17563)``
* ``Update s3_list.py (#18561)``
* ``ECSOperator realtime logging (#17626)``
* ``Deprecate default pod name in EKSPodOperator (#18036)``
* ``Aws secrets manager backend (#17448)``
* ``sftp_to_s3 stream file option (#17609)``
* ``AwsBaseHook make client_type resource_type optional params for get_client_type, get_resource_type (#17987)``
* ``Delete unnecessary parameters in EKSPodOperator (#17960)``
* ``Enable AWS Secrets Manager backend to retrieve conns using different fields (#18764)``
* ``Add emr cluster link (#18691)``
* ``AwsGlueJobOperator: add wait_for_completion to Glue job run (#18814)``
* ``Enable FTPToS3Operator to transfer several files (#17937)``
* ``Amazon Athena Example (#18785)``
* ``AwsGlueJobOperator: add run_job_kwargs to Glue job run (#16796)``
* ``Amazon SQS Example (#18760)``
* ``Adds an s3 list prefixes operator (#17145)``
* ``Add additional dependency for postgres extra for amazon provider (#18737)``
* ``Support all Unix wildcards in S3KeySensor (#18211)``
* ``Add AWS Fargate profile support (#18645)``

Bug Fixes
~~~~~~~~~

* ``ECSOperator returns last logs when ECS task fails (#17209)``
* ``Refresh credentials for long-running pods on EKS (#17951)``
* ``ECSOperator: airflow exception on edge case when cloudwatch log stream is not found (#18733)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify s3 unify_bucket_name_and_key (#17325)``
   * ``Updating miscellaneous provider DAGs to use TaskFlow API where applicable (#18278)``
   * ``Inclusive Language (#18349)``
   * ``Simplify strings previously split across lines (#18679)``
   * ``Update documentation for September providers release (#18613)``

2.2.0
.....


Features
~~~~~~~~

* ``Add an Amazon EMR on EKS provider package (#16766)``
* ``Add optional SQL parameters in ''RedshiftToS3Operator'' (#17640)``
* ``Add new LocalFilesystemToS3Operator under Amazon provider (#17168) (#17382)``
* ``Add Mongo projections to hook and transfer (#17379)``
* ``make platform version as independent parameter of ECSOperator (#17281)``
* ``Improve AWS SQS Sensor (#16880) (#16904)``
* ``Implemented Basic EKS Integration (#16571)``

Bug Fixes
~~~~~~~~~

* ``Fixing ParamValidationError when executing load_file in Glue hooks/operators (#16012)``
* ``Fixes #16972 - Slugify role session name in AWS base hook (#17210)``
* ``Fix broken XCOM in EKSPodOperator (#17918)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``
* ``Fix provider.yaml errors due to exit(0) in test (#17858)``
* ``Adds secrets backend/logging/auth information to provider yaml (#17625)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Doc: Fix docstrings for ''MongoToS3Operator'' (#17588)``
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Remove all deprecation warnings in providers (#17900)``

2.1.0
.....

Features
~~~~~~~~
* ``Allow attaching to previously launched task in ECSOperator (#16685)``
* ``Update AWS Base hook to use refreshable credentials (#16770) (#16771)``
* ``Added select_query to the templated fields in RedshiftToS3Operator (#16767)``
* ``AWS Hook - allow IDP HTTP retry (#12639) (#16612)``
* ``Update Boto3 API calls in ECSOperator (#16050)``
* ``Adding custom Salesforce connection type + SalesforceToS3Operator updates (#17162)``
* ``Adding SalesforceToS3Operator to Amazon Provider (#17094)``

Bug Fixes
~~~~~~~~~

* ``AWS DataSync default polling adjusted from 5s to 30s (#11011)``
* ``Fix wrong template_fields_renderers for AWS operators (#16820)``
* ``AWS DataSync cancel task on exception (#11011) (#16589)``
* ``Fixed template_fields_renderers for Amazon provider (#17087)``
* ``removing try-catch block (#17081)``
* ``ECSOperator / pass context to self.xcom_pull as it was missing (when using reattach) (#17141)``
* ``Made S3ToRedshiftOperator transaction safe (#17117)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Bump sphinxcontrib-spelling and minor improvements (#16675)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Added docs &amp; doc ref's for AWS transfer operators between SFTP &amp; S3 (#16964)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Updating Amazon-AWS example DAGs to use XComArgs (#16868)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Features
~~~~~~~~

* ``CloudwatchTaskHandler reads timestamp from Cloudwatch events (#15173)``
* ``remove retry for now (#16150)``
* ``Remove the 'not-allow-trailing-slash' rule on S3_hook (#15609)``
* ``Add support of capacity provider strategy for ECSOperator (#15848)``
* ``Update copy command for s3 to redshift (#16241)``
* ``Make job name check optional in SageMakerTrainingOperator (#16327)``
* ``Add AWS DMS replication task operators (#15850)``

Bug Fixes
~~~~~~~~~

* ``Fix S3 Select payload join (#16189)``
* ``Fix spacing in 'AwsBatchWaitersHook' docstring (#15839)``
* ``MongoToS3Operator failed when running with a single query (not aggregate pipeline) (#15680)``
* ``fix: AwsGlueJobOperator change order of args for load_file (#16216)``
* ``Fix S3ToFTPOperator (#13796)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Check synctatic correctness for code-snippets (#16005)``
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
   * ``Docs: Replace 'airflow' to 'apache-airflow' to install extra (#15628)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add Connection Documentation for the Hive Provider (#15704)``
   * ``Update Docstrings of Modules with Missing Params (#15391)``
   * ``Fix spelling (#15699)``
   * ``Add Connection Documentation for Providers (#15499)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.4.0
.....

Features
~~~~~~~~

* ``S3Hook.load_file should accept Path object in addition to str (#15232)``

Bug fixes
~~~~~~~~~

* ``Fix 'logging.exception' redundancy (#14823)``
* ``Fix AthenaSensor calling AthenaHook incorrectly (#15427)``
* ``Add links to new modules for deprecated modules (#15316)``
* ``Fixes doc for SQSSensor (#15323)``

1.3.0
.....

Features
~~~~~~~~

* ``A bunch of template_fields_renderers additions (#15130)``
* ``Send region_name into parent class of AwsGlueJobHook (#14251)``
* ``Added retry to ECS Operator (#14263)``
* ``Make script_args templated in AwsGlueJobOperator (#14925)``
* ``Add FTPToS3Operator (#13707)``
* ``Implemented S3 Bucket Tagging (#14402)``
* ``S3DataSource is not required (#14220)``

Bug fixes
~~~~~~~~~

* ``AWS: Do not log info when SSM & SecretsManager secret not found (#15120)``
* ``Cache Hook when initializing 'CloudFormationCreateStackSensor' (#14638)``

1.2.0
.....

Features
~~~~~~~~

* ``Avoid using threads in S3 remote logging upload (#14414)``
* ``Allow AWS Operator RedshiftToS3Transfer To Run a Custom Query (#14177)``
* ``includes the STS token if STS credentials are used (#11227)``

1.1.0
.....

Features
~~~~~~~~

* ``Adding support to put extra arguments for Glue Job. (#14027)``
* ``Add aws ses email backend for use with EmailOperator. (#13986)``
* ``Add bucket_name to template fileds in S3 operators (#13973)``
* ``Add ExasolToS3Operator (#13847)``
* ``AWS Glue Crawler Integration (#13072)``
* ``Add acl_policy to S3CopyObjectOperator (#13773)``
* ``AllowDiskUse parameter and docs in MongotoS3Operator (#12033)``
* ``Add S3ToFTPOperator (#11747)``
* ``add xcom push for ECSOperator (#12096)``
* ``[AIRFLOW-3723] Add Gzip capability to mongo_to_S3 operator (#13187)``
* ``Add S3KeySizeSensor (#13049)``
* ``Add 'mongo_collection' to template_fields in MongoToS3Operator (#13361)``
* ``Allow Tags on AWS Batch Job Submission (#13396)``

Bug fixes
~~~~~~~~~

* ``Fix bug in GCSToS3Operator (#13718)``
* ``Fix S3KeysUnchangedSensor so that template_fields work (#13490)``


1.0.0
.....


Initial version of the provider.
