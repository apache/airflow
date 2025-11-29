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

9.18.0
......

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Move out some exceptions to TaskSDK (#54505)``
* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``
* ``Remove the limitation for sagemaker for Python 3.13 (#58388)``
* ``Remove SDK reference for NOTSET in Airflow Core (#58258)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``
   * ``Check for cluster stability just before the transfer (#58690)``
   * ``Increase wait for redshift clusters (#58645)``
   * ``Remove global from lineage.hook (#58285)``
   * ``Increase waiter delay for ecs run tasks in system tests (#58338)``

9.17.0
......

Features
~~~~~~~~

* ``Add flatten_structure parameter to GCSToS3Operator (#56134) (#57713)``
* ``Add missing failure retry case for Bedrock (#57777)``
* ``Add support for Airflow 3 in MWAA operators/sensors/triggers (#57443)``
* ``Add SsmGetCommandInvocationOperator and enhance SSM components (#56936)``
* ``Add missing 'bucket_name' to 'get_file_metadata' in 'S3Hook'``

Bug Fixes
~~~~~~~~~

* ``Fix: S3KeySensor deferrable mode ignores metadata_keys, returns only key names, and doesn't pass context to check_fn (#56910)``
* ``Fix 'MwaaTaskCompletedTrigger' (#57490)``
* ``Fix DAG bundle retrieval from S3 (#57178)``

Misc
~~~~

* ``Exclude sagemaker for Python 3.13 due to pydanamodb pinning old sqlean (#58262)``
* ``Remove unnecessary list (#58141)``
* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

Doc-only
~~~~~~~~

* ``Update AWS auth manager documentation to fix login callback URL (#57974)``
* ``[Doc] Fixing some typos and spelling errors (#57225)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Skip the team config test until new modular config supports it (#58233)``
   * ``Remove deprecation warning in common test utils (#58152)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Enable PT006 rule to 23 files in providers (amazon -> hooks, links, log, queues) (#58003)``
   * ``Enable PT006 rule to 23 files in providers (all remaining files related to amazon) (#58005)``
   * ``Decrease the batch inference size for example_bedrock_batch_inference (#57912)``
   * ``Enable PT006 rule to 17 files in providers (operatorsproviders/amazon/tests/unit/amazon/aws/operators/) (#57903)``
   * ``EKS sensors before delete operations (#57655)``
   * ``Fix mypy static errors in main (#57755)``
   * ``Enable ruff PLW1510 rule (#57660)``
   * ``Enable ruff PLW1508 rule (#57653)``
   * ``Fix code formatting via ruff preview (#57641)``
   * ``Enable ruff PLW0129 rule (#57516)``
   * ``Enable ruff PLW0120 rule (#57456)``
   * ``Enable PT011 rule to prvoider tests (#56929)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``
   * ``Revert "Fix main. Fix 'test_athena_sql.py' (#56974)" (#57098)``
   * ``Fixing some typos and spelling errors (#57186)``
   * ``Add missing test for amazon/aws/executors/ecs/test_utils.py (#58139)``

9.16.0
......

Features
~~~~~~~~

* ``Separate Firehose and Kinesis hooks (#56276)``

Bug Fixes
~~~~~~~~~

* ``Fixed incorrect path in EMR notebook waiter (#56584)``
* ``Add poke_mode_only to version_compat to fix the incorrect deprecation warning (#56435)``
* ``Refactor: deprecate wait_policy in EmrCreateJobFlowOperator in favor of wait_for_completion (#56158)``

Misc
~~~~

* ``Migrate amazon provider to ''common.compat'' (#56994)``
* ``Fix mypy errors for sqla2 in aws hooks (#56751)``
* ``Update authentication to handle JWT token in backend (#56633)``

Doc-only
~~~~~~~~

* ``Correct 'Dag' to 'DAG' for code snippets in provider docs (#56727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix main. Fix 'test_athena_sql.py' (#56974)``
   * ``Update example dms system tests for sqla 2 (#56744)``
   * ``Enable PT011 rule to prvoider tests (#56698)``
   * ``Enable PT011 rule to prvoider tests (#56642)``
   * ``Enable PT011 rule to prvoider tests (#56608)``
   * ``[AWS System Tests] Add task retries to deletion of EKS resources (#56308)``

9.15.0
......

Features
~~~~~~~~

* ``Add async support for Amazon SNS Notifier (#56133)``
* ``Add async support for Amazon SQS Notifier (#56159)``
* ``Add 'SesNotifier' - Amazon Simple Email Service Notifier (#56106)``
* ``Implement 'filter_authorized_connections', 'filter_authorized_pools' and 'filter_authorized_variables' in AWS auth manager (#55687)``

Bug Fixes
~~~~~~~~~

* ``Fix wrong import of 'AIRFLOW_V_3_0_PLUS' in 'AwsLambdaExecutor' (#56280)``
* ``Only defer 'EmrCreateJobFlowOperator' when 'wait_policy' is set (#56077)``
* ``Reducing memory footprint for synchronous 'S3KeySensor' (#55070)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove placeholder Release Date in changelog and index files (#56056)``

9.14.0
......


Features
~~~~~~~~

* ``Implement 'batch_is_authorized_' methods in AWS auth manager (#55307)``
* ``Add configurable confirm parameter to 'S3ToSFTPOperator' (#55569)``

Bug Fixes
~~~~~~~~~

* ``[OSSTaskHandler, CloudwatchTaskHandler, S3TaskHandler, HdfsTaskHandler, ElasticsearchTaskHandler, GCSTaskHandler, OpensearchTaskHandler, RedisTaskHandler, WasbTaskHandler] supports log file size handling (#55455)``
* ``Catch 404/401 issues for Bedrock Operators (#55445)``
* ``EcsRunTaskOperator fails when no containers are provided in the response (#51692)``
* ``AWS BatchOperator does not fetch log entries for deferred jobs (#55703)``

Misc
~~~~

* ``List only connections, pools and variables the user has access to (#55298)``
* ``Switch all airflow logging to structlog (#52651)``
* ``AIP-67 - Multi-team: Per team executor config (env var only) (#55003)``
* ``Allow SSM operators and sensors to run in deferrable mode (#55649)``
* ``Update EOL AWS Redshift cluster node types (#55741)``
* ``improve logging in SqsSensorTrigger (#55705)``

Doc-only
~~~~~~~~

* ``Add stable note to BatchExecutor (#55286)``
* ``Add quotas section in lambda executor docs (#55740)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate off Xen-based ECS instances (#55527)``
   * ``Add missing test for aws batch utils (#55407)``
   * ``README optional dependencies template (#55280)``

9.13.0
......


.. note::
  * ``The experimental BatchExecutor added in 8.20.0 is now stable``

Features
~~~~~~~~

* ``feature: Add OpenLineage support for transfer operators between GCS and S3 (#54269)``
* ``Update HiveToDynamoDBOperator to support Polars (#54221)``
* ``Update 'SqlToS3Operator' to support Polars and deprecate 'read_pd_kwargs' (#54195)``

Bug Fixes
~~~~~~~~~

* ``Fix SqlToS3Operator _partition_dataframe for proper polars support (#54588)``
* ``fixing file extension issue on SqlToS3Operator (#54187)``
* ``Fix connection management for EKS token generation (#55195)``
* ``Retry on more edge cases for bedrock (#55201)``

Misc
~~~~

* ``Refactor Common Queue Interface (#54651)``
* ``Introduce 'LIST' logic in AWS auth manager (#54987)``
* ``Move secrets_masker over to airflow_shared distribution (#54449)``
* ``Handle task-sdk connection retrieval import error in BaseHook (#54692)``
* ``Remove end_from_trigger attribute in trigger (#54567)``
* ``Remove redundant exception handling in AWS hook (#54485)``
* ``Remove end_from_trigger attribute in trigger (#54531)``
* ``Unify error handling when connection is not found in aws hook (#54299)``
* ``Fix a small typo and improve logging in bedrock error handling (#55271)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``
* ``Mark Batch Executor as stable (#54924)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove airflow.models.DAG (#54383)``
   * ``Add missing test for amazon/aws/executors/ecs/boto_schema.py (#54930)``
   * ``Move trigger_rule utils from 'airflow/utils'  to 'airflow.task'and integrate with Execution API spec (#53389)``
   * ``Make AWS notifier tests db independent (#54668)``
   * ``Replace API server's direct Connection access workaround in BaseHook (#54083)``
   * ``Switch pre-commit to prek (#54258)``
   * ``Mock AWS during Athena tests (#54576)``
   * ``make bundle_name not nullable (#47592)``
   * ``Add CI support for SQLAlchemy 2.0 (#52233)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Update models used in bedrock system tests (#55229)``

9.12.0
......

Features
~~~~~~~~

* ``Add MwaaTaskSensor to Amazon Provider Package (#51719)``
* ``Support executor_config on Lambda Executor (#53994)``

Bug Fixes
~~~~~~~~~

* ``Fix AWS Lambda executor error handling for DLQ vs task failures (#53990)``

Misc
~~~~

* ``Set minimum version for common.messaging to 1.0.3 (#54160)``
* ``Use timezone from new TaskSDK public API where possible (#53949)``
* ``Use timezone from new TaskSDK public API where possible (second part) (#53986)``
* ``Refactor bundle view_url to not instantiate bundle on server components (#52876)``

Doc-only
~~~~~~~~

* ``Add missing PR number in amazon changelog (#53880)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``SageMaker Unified Studios System Tests Update (#54038)``
   * ``Do not run export operations in 'example_dynamodb_to_s3' (#54158)``
   * ``Add system test for Lambda executor Dead Letter Queue (DLQ) processing (#54042)``
   * ``Fix 'importskip' statements in tests (#54135)``
   * ``Increase timeout to delete tables in 'example_s3_to_dynamodb' (#54096)``

9.11.0
......

Features
~~~~~~~~

* ``Add full support for AWS SSM Run Command in Airflow (#52769)``
* ``Enhancement: AWS Provider sql to s3 operator pd.read_sql kwargs (#53399)``
* ``Support HA schedulers for the Lambda Executor (#53396)``

Bug Fixes
~~~~~~~~~

* ``Fix variable name in EKS command for token expiration timestamp (#53720)``
* ``Fix EMR operator parameter documentation and naming (#53446)``
* ``remove ECS Operator retry mechanism on task failed to start (#53083)``
* ``Resolve OOM When Reading Large Logs in Webserver (#49470)``

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Fix unreachable code mypy warnings in amazon provider (#53414)``
* ``Refactoring get con part dbapihook in providers (#53335)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Cleanup type ignores in amazon provider where possible (#53239)``
* ``Use standard library ''typing'' imports for Python 3.10+ (#53158)``
* ``Always build xmlsec and lxml packages from sources in our images (#53137)``
* ``Improve mypy typing for RedshiftHook (#53099)``
* ``Make amazon provider compatible with mypy 1.16.1 (#53088)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Change import in provider example dag (#53772)``
   * ``Deprecate decorators from Core (#53629)``
   * ``Replace 'mock.patch("utcnow")' with time_machine. (#53642)``
   * ``Fix typos 'aiflow' -> 'airflow' (#53603)``
   * ``Add support to example_emr_eks for LambdaExecutor environment (#53394)``
   * ``Cleanup mypy ignore in eks_test_utils (#53325)``
   * ``Handle ruff PT028 changes (#53235)``
   * ``Create connection with API instead of directly through Session (#53161)``
   * ``Make dag_version_id in TI non-nullable (#50825)``
   * ``Changing import path to 'airflow.sdk' in Amazon provider package (#50659)``

9.10.0
......

Features
~~~~~~~~

* ``Add support for S3 dag bundle (#46621)``

Bug Fixes
~~~~~~~~~

* ``Fix GlueJobOperator deferred waiting (#52314)``
* ``Handle exceptions when fetching status in GlueJobHook (#52262)``
* ``Handle 'S3KeySensor' in 'deferrable' mode splits 'bucket_key' into individual chars (#52983)``
* ``Pass the region_name from the GlueJobOperator / GlueJobSensor to the Trigger (#52904)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Replace 'models.BaseOperator' to Task SDK one for Amazon Provider (#52667)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Upgrade ruff to latest version (0.12.1) (#52562)``
* ``Replace usage of 'set_extra' with 'extra' for athena sql hook (#52340)``
* ``Drop support for Python 3.9 (#52072)``
* ``Replace 'models.BaseOperator' to Task SDK one for Standard Provider (#52292)``
* ``Replace occurences of 'get_password' with 'password' to ease migration (#52333)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``
* ``Use base AWS classes in Glue Trigger / Sensor and implement custom waiter (#52243)``
* ``Add Airflow 3.0+ Task SDK support to AWS Batch Executor (#52121)``
* ``Refactor operator_extra_links property in BatchOperator (#51385)``
* ``Remove unused batch methods from auth manager (#52883)``
* ``Add debug logging for endpoint_url in AWS Connection (#52856)``
* ``Remove 'MENU' from 'ResourceMethod' in auth manager (#52731)``
* ``Remove 'MAX_XCOM_SIZE' hardcoded constant from Airflow core  (#52978)``
* ``More robust handling of 'BaseHook.get_connection''s 'CONNECTION_NOT_FOUND' Task SDK exception (#52838)``
* ``Move all BaseHook usages to version_compat in Amazon (#52796)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

Doc-only
~~~~~~~~

* ``Clean some leftovers of Python 3.9 removal - All the rest (#52432)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make sure all test version imports come from test_common (#52425)``
   * ``Added additional steps to QuickSights test prerequisites (#52198)``
   * ``examples dags: Update redshift node version dc2.large is deprecated (#52120)``
   * ``Updating AWS systest to do connection setup using ENV (#52073)``
   * ``Remove pytest.mark.db_test: airbyte and amazon providers where possible (#52017)``
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``
   * ``Switch the Supervisor/task process from line-based to length-prefixed (#51699)``
   * ``Mocked time.sleep to avoid actual sleep time (#51752)``
   * ``Prepare release for July 2025 1st provider wave (#52727)``

9.9.0
.....

Features
~~~~~~~~

* ``Add 'MessageDeduplicationId' support to 'SnsPublishOperator' (#51383)``
* ``Add support for RequestPay=requester option in Amazon S3's Operators, Sensors and Triggers (#51098)``
* ``Add AWS Lambda Executor (#50516)``

Bug Fixes
~~~~~~~~~

* ``Removed unnecessary 'aws_conn_id' param from operators constructors (#51236)``
* ``Fix EcsRunTaskOperator reattach (#51412)``
* ``Fix EKS token generation (#51333)``
* ``Fix 'EksPodOperator' in deferrable mode (#51255)``
* ``Rds Operator pass custom conn_id to superclass (#51196)``
* ``Fix remote logging CloudWatch handler initialization and stream name assignment (#51022)``
* ``Check 'is_mapped' to prevent 'operator_extra_links' property failing for Airflow 3. #50932``
* ``Fix aws_conn_id defaulting after dag.test was updated to use TaskSDK. (#50515)``
* ``AWS ECS Executor. Assign public ip defaults false (#50713)``

Misc
~~~~

* ``Remove unused entries from 'DagAccessEntity' (#51174)``
* ``Update Redshift cluster operator and sensor to inherit AwsBaseOperator (#51129)``
* ``Remove Airflow 2 code path in executors (#51009)``
* ``Move AWS auth dependencies to python3-saml extra (#50449)``
* ``Bump some provider dependencies for faster resolution (#51727)``

Doc-only
~~~~~~~~

* ``docs: Add missing 'param' for waiter_max_attempts in EMR operator docstring (#51676)``
* ``Update comment in CloudWatchRemoteLogIO (#51092)``
* ``Use explicit directives instead of implicit syntax (#50870)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix 'example_bedrock_batch_inference' (#51413)``
   * ``Fixed cross-merged tests that fail for Pytest 8.4.0 (#51366)``
   * ``Allow test migration to pytest 8.4.0 (#51349)``
   * ``Fix system test 'test_aws_auth_manager' (#51241)``
   * ``Fix 'StopIteration' error in AWS System Test 'variable_fetcher' when using remote executor (#51127)``

9.8.0
.....

Features
~~~~~~~~

* ``Add in-memory buffer and gzip support in SqlToS3Operator (#50287)``

Bug Fixes
~~~~~~~~~

* ``Add Key to default meta keys in S3KeySensor (#50122)``

Misc
~~~~

* ``Update Sagemaker Operators and Sensors to inherit Base AWS classes (#50321)``
* ``Use non-deprecated context in tests for Airflow 3 (#50391)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Handle exception when building amazon documentation (#50417)``
   * ``AWS System Test Context: Don't set env_id at parse time (#50571)``

9.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.10+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Move SQS messaging from common.messaging to Amazon provider (#50057)``

Bug Fixes
~~~~~~~~~

* ``Fix typo in emr sensor _hook_parameters (#49597)``
* ``Update 'prune_logs' in Amazon provider package to support 'RuntimeTaskInstance' (#49363)``

Misc
~~~~

* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``Update GlueJobOperator to inherit AWS base class (#49750)``
* ``Amazon EMR Sensors/Operators inherit AWS Base Classes (#49486)``
* ``Introduce lower bind to lxml as 5.4.0 (#49612)``
* ``Remove Marshmallow from Core (#49388)``
* ``Migrate 'HiveToDynamoDBOperator' and 'SqlToS3Operator' to use 'get_df' (#50126)``
* ``add root parent information to OpenLineage events (#49237)``
* ``Remove limits for aiobotocore limiting boto3 (#50285)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Limit boto3 (#49939)" (#49948)``
   * ``Refactor connection creation in system tests to use REST API instead (#49804)``
   * ``Min provider version=2.10; use running_state freely (#49924)``
   * ``Also limit botocore when upgrading to latest (#49962)``
   * ``Add AWS auth manager to AWS provider.yaml (#49944)``
   * ``Limit boto3 (#49939)``
   * ``Avoid committing history for providers (#49907)``
   * ``Replace chicken-egg providers with automated use of unreleased packages (#49799)``
   * ``Fix AWS system test names (#49791)``
   * ``Update 'secure' in AWS auth manager (#49751)``
   * ``use NonNegativeInt for backfill_id (#49691)``
   * ``Fix AWS auth manager (#49588)``
   * ``Fix AWS auth manager system test (#49561)``
   * ``Make AWS auth manager compatible with AF3 (#49419)``
   * ``capitalize the term airflow (#49450)``
   * ``Use Label class from task sdk in providers (#49398)``
   * ``Add Stop AutoML Job to the Sagemaker system test to clean up. (#49325)``
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Prepare ad hoc release for providers May 2025 (#50166)``

9.6.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix 'EksClusterStateSensor'. Save 'region' as attribute (#49138)``
* ``Decrease default value of 'waiter_max_attempts' in 'MwaaTriggerDagRunOperator' (#49136)``
* ``Increase default value of 'waiter_max_attempts' in 'BedrockBatchInferenceOperator' (#49090)``

Misc
~~~~

* ``Use contextlib.suppress(exception) instead of try-except-pass and add SIM105 ruff rule (#49251)``
* ``Add base_url fallback for aws auth_manager (#49305)``
* ``remove superfluous else block (#49199)``
* ``AWS Batch Operators/Sensors inherit AWS Base classes (#49172)``
* ``Help pip to find appropriate boto for aiobotocore (#49166)``
* ``Update EKS Operators and Sensors to inherit AWS base classes (#48192)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Unpause DAG in AWS MWAA system test (#49145)``
   * ``Fix AWS auth manager system test (#49072)``

9.6.0
.....

Features
~~~~~~~~

* ``Add Bedrock Batch Inference Operator and accompanying parts (#48468)``
* ``Update ECS executor to support Task SDK (#48513)``

Bug Fixes
~~~~~~~~~

* ``Handle NoCredentialsError in waiter_with_logging.py (#48946)``
* ``Bedrock Batch Inference - Trying to stop a completed job is a successful result (#48964)``
* ``S3Hook: remove error return on inactivity period check (#48782)``

Misc
~~~~

* ``Rename list_jobs method to describe_jobs in GlueJobHook (#48904)``
* ``Fix typo in docstring for MwaaHook (#48980)``
* ``Update Amazon RDS Operators and Sensors to inherit AWS Base classes (#48872)``
* ``Change provider-specific dependencies to refer to providers (#48843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor AWS system tests to not use @task.branch (#48973)``
   * ``Fix botocore version in Amazon provider docs to match 'pyproject.toml' (#48981)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``
   * ``Fix default base value (#49013)``

9.5.0
.....

Features
~~~~~~~~

* ``Add a backup implementation in AWS MwaaHook for calling the MWAA API (#47035)``
* ``Add AWS SageMaker Unified Studio Workflow Operator (#45726)``
* ``Add error statuses check in RdsExportTaskExistenceSensor  (#46917)``
* ``Common Message Queue (#46694)``
* ``add startTime to paginator.paginate when fetching logs in GlueJobHook (#46950)``
* ``Add MwaaDagRunSensor to Amazon Provider Package (#46945)``
* ``Add wait/defer support - MwaaTriggerDagRunOperator (#47528)``
* ``Add deferrable support for MwaaDagRunSensor (#47527)``

Bug Fixes
~~~~~~~~~

* ``Fix aws trigger tests, use get_async_conn for mock object (#47515)``
* ``fix: don't use blocking property access for async purposes (#47326)``
* ``Fix and simplify 'get_permitted_dag_ids' in auth manager (#47458)``
* ``Log state for EMR Containers sensor on failure (#47125)``
* ``Don't expect default conns in S3ToRedshiftOperator (#48363)``
* ``Don't expect default connections to be present in RedshiftToS3Operator (#47968)``
* ``fix PosixPath not working with file create_asset in download_file of S3Hook (#47880)``
* ``Fix Cloudwatch remote logging (#48774)``
* ``Fix 'conf.get_boolean("api", "ssl_cert")' (#48465)``
* ``Fix signature of 'BatchWaitersHook.get_waiter' not matching parent class (#48581)``

Misc
~~~~

* ``Relocate airflow.auth to airflow.api_fastapi.auth (#47492)``
* ``AIP-72: Moving BaseOperatorLink to task sdk (#47008)``
* ``Add some typing and require kwargs for auth manager (#47455)``
* ``AIP-84 - Add Auth for Assets (#47136)``
* ``Base AWS classes - S3 (#47321)``
* ``Remove unused methods from auth managers (#47316)``
* ``Move api-server to port 8080 (#47310)``
* ``Render structured logs in the new UI rather than showing raw JSON (#46827)``
* ``Remove old UI and webserver (#46942)``
* ``Don't remove log groups from example_glue.py (#47128)``
* ``Move 'fastapi-api' command to 'api-server' (#47076)``
* ``Remove '/webapp' prefix from new UI (#47041)``
* ``Restricting moto 5.1.0 to fix ci (#47005)``
* ``Bump minimum boto3 version to 1.37.0 (#48238)``
* ``Move BaseNotifier to Task SDK (#48008)``
* ``Updating EC2 Operators and Sensors with AWS Base classes (#47931)``
* ``Bump mypy-boto3-appflow>=1.37.0 (#47912)``
* ``Lower bind xmlsec dependency version (#47696)``
* ``Clarify the Redshift delete cluster operator messaging. (#48652)``
* ``Rework remote task log handling for the structlog era. (#48491)``
* ``Move 'BaseSensorOperator' to TaskSDK definitions (#48244)``
* ``Cookies in non TLS mode (#48453)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add skipimport check for aiobotocore module in aws trigger tests (#47512)``
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Fix codespell issues detected by new codespell (#47259)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Replace 'external_trigger' check with DagRunType (#45961)``
   * ``Remove extra whitespace in provider readme template (#46975)``
   * ``Fix TestRdsCopyDbSnapshotOperator tests (#47006)``
   * ``Fix new UI when running outside of breeze (#46991)``
   * ``Upgrade flit to 3.11.0 (#46938)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``(Re)move old dependencies from the old FAB UI (#48007)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Update example_s3 system test (#47974)``
   * ``Set 'wait_for_completion' to True in example_mwaa system test (#47877)``
   * ``Fix AWS auth manager system test (#47876)``
   * ``AIP-72: Handle Custom XCom Backend on Task SDK (#47339)``
   * ``Remove links to x/twitter.com (#47801)``
   * ``Test 'MwaaHook''s IAM fallback in system test (#47759)``
   * ``Update Dockerfile in aws execs docs (#47799)``
   * ``Update AWS auth manager system test to handle new way of passing JWT token (#47794)``
   * ``Rename 'get_permitted_dag_ids' and 'filter_permitted_dag_ids' to 'get_authorized_dag_ids' and 'filter_authorized_dag_ids' (#47640)``
   * ``Set JWT token to localStorage from cookies (#47432)``
   * ``Re-work JWT Validation and Generation to use public/private key and official claims (#46981)``
   * ``AIP-84 Add Auth for DAG Versioning (#47553)``
   * ``Introduce 'filter_authorized_menu_items' to filter menu items based on permissions (#47681)``
   * ``AIP-84 Add Auth for backfill (#47482)``
   * ``test(aws): Fix aws trigger tests, use get_async_conn for mock object (#47667)``
   * ``Adding xmlsec pin in amazon provider (#47656)``
   * ``Add 'get_additional_menu_items' in auth manager interface to extend the menu (#47468)``
   * ``Use a single http tag to report the server's location to front end, not two (#47572)``
   * ``AIP 84 - Add auth for asset alias (#47241)``
   * ``Prepare docs for Mar 1st wave of providers (#47545)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Fix usage of mock_cmd in ECS executor unit tests (#48593)``
   * ``Fix failing eks tests with new moto 5.1.2 (#48556)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``

9.4.0
.....

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.

9.3.0
.....

.. warning::
  * ``The experimental AWS auth manager is no longer compatible with Airflow 2``

Features
~~~~~~~~

* ``Add MwaaTriggerDagRunOperator and MwaaHook to Amazon Provider Package (#46579)``
* ``Adding extra links for EC2 (#46340)``
* ``Allow to pass container_name parameter to EcsRunTaskOperator (#46152)``
* ``Adding DataSync links (#46292)``
* ``Added extra links for Comprehend operators (#46031)``
* ``Add support for timeout to BatchOperator (#45660)``
* ``Adding SageMaker Transform extra link (#45677)``
* ``Add MessageDeduplicationId support to AWS SqsPublishOperator (#45051)``

Bug Fixes
~~~~~~~~~

* ``Rework the TriggererJobRunner to run triggers in a process without DB access (#46677)``
* ``Fix schema path in AWS auth manager system test due to restructure (#46625)``
* ``Increase retries in 'EmrContainerHook.create_emr_on_eks_cluster' (#46562)``
* ``Update 'create_emr_on_eks_cluster' method to try when "cluster is not reachable as its connection is currently being updated" (#46497)``
* ``Generate partition aware STS endpoints for EKS Hook (#45725)``
* ``Sagemaker Operator Character limit fix  (#45551)``
* ``Fix 'fetch_access_token_for_cluster' in EKS hook (#45469)``
* ``The DMS waiter replication_terminal_status has been extended to proceed on 2 additional states: "created" and "deprovisioned" (#46684)``

Misc
~~~~

* ``AIP-72: Improving Operator Links Interface to Prevent User Code Execution in Webserver (#46613)``
* ``Update 'example_sqs' to not use 'logical_date' (#46696)``
* ``Change improper AirflowProviderDeprecationWarning ignore to DeprecationWarning ignore for 3.12 tests (#46612)``
* ``Update AWS auth manager to use Fastapi instead of Flask (#46381)``
* ``AIP-72: Move Secrets Masker to task SDK (#46375)``
* ``Swap CeleryExecutor over to use TaskSDK for execution. (#46265)``
* ``Make parameter 'user' mandatory for all methods in the auth manager interface (#45986)``
* ``Add 'run_job_kwargs' as templated field in 'GlueJobOperator' (#45973)``
* ``Use Protocol for 'OutletEventAccessor' (#45762)``
* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``
* ``Remove classes from 'typing_compat' that can be imported directly (#45589)``
* ``Move Literal alias into TYPE_CHECKING block (#45345)``
* ``Remove marshmallow version restriction; update deprecated usages (#45499)``
* ``Remove obsolete pandas specfication for pre-python 3.9 (#45399)``
* ``Add option in auth manager interface to define FastAPI api (#45009)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Migrate Amazon provider package (#46590)``
   * ``move standard, alibaba and common.sql provider to the new structure (#45964)``
   * ``Revert "Fix fetch_access_token_for_cluster in EKS hook" (#45526)``
   * ``Fix the way to get STS endpoint in EKS hook (#45520)``
   * ``update outdated hyperlinks referencing provider package files (#45332)``

9.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add DMS Serverless Operators (#43988)``
* ``Add fail_on_file_not_exist option to SFTPToS3Operator (#44320)``
* ``Add 'wait_policy' option to 'EmrCreateJobFlowOperator' (#44055)``
* ``Add meta_data_directive to 'S3CopyObjectOperator' (#44160)``

Misc
~~~~

* ``Remove references to AIRFLOW_V_2_9_PLUS (#44987)``
* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Consistent way of checking Airflow version in providers (#44686)``
* ``Remove unnecessary compatibility code in S3 asset import (#44714)``
* ``Remove AIP-44 from taskinstance (#44540)``
* ``Add do_xcom_push documentation in EcsRunTaskOperator (#44440)``
* ``Move Asset user facing components to task_sdk (#43773)``
* ``Set up JWT token authentication in Fast APIs (#42634)``
* ``Bump to mypy-boto3-appflow and pass without '# type: ignore[arg-type]' (#44115)``
* ``Update DAG example links in multiple providers documents (#44034)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Fix deferrable RedshiftClusterSensor (#45098)``
   * ``Update path of example dags in docs (#45069)``

9.1.0
.....

Features
~~~~~~~~

* ``feat: add OpenLineage support for RedshiftToS3Operator (#41632)``
* ``Add 'SageMakerProcessingSensor' (#43144)``
* ``Make 'RedshiftDataOperator'  handle multiple queries (#42900)``

Bug Fixes
~~~~~~~~~

* ``fix(providers/amazon): alias is_authorized_dataset to is_authorized_asset (#43470)``
* ``Remove returns in final clause of athena hooks (#43426)``
* ``fix: replace \s with space in EksHook (#43849)``
* ``Fix 'HttpToS3Operator' throws exception if s3_bucket parameter is not passed (#43828)``
* ``Add 'container_name' and update 'awslogs_stream_prefix' pattern (#43138)``
* ``Check if awslogs_stream_prefix already ends with container_name (#43724)``
* ``bugfix description should be optional for openlineage integration with 'AthenaOperator' (#43576)``
* ``(bugfix): 'EcsRunTaskOperator' decouple 'volume_configurations' from 'capacity_provider_strategy' (#43047)``
* ``GlueJobOperator: add option to wait for cleanup before returning job status (#43688)``
* ``Resolve 'GlueJobTrigger' serialization bug causing verbose to always be True (#43622)``
* ``Remove returns in final clause of S3ToDynamoDBOperator (#43456)``

Misc
~~~~

* ``Remove sqlalchemy-redshift dependency (#43271)``
* ``feat(providers/amazon): Use asset in common provider (#43110)``
* ``Restrict looker-sdk version 24.18.0 and microsoft-kiota-http 1.3.4 (#42954)``
* ``Limit mypy-boto3-appflow (#43436)``
* ``Move PythonOperator to Standard provider (#42081)``
* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``
* ``Remove deprecations from cncf.kubernetes provider (#43689)``
* ``Fix docstring for AthenaTrigger (#43616)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove TaskContextLogger (#43183)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``
   * ``Start porting DAG definition code to the Task SDK (#43076)``
   * ``Prepare docs for Oct 2nd wave of providers (#43409)``
   * ``Prepare docs for Oct 2nd wave of providers RC2 (#43540)``
   * ``Prepare docs for Oct 2nd wave of providers rc3 (#43613)``

9.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In order to support session reuse in RedshiftData operators, the following breaking changes were introduced:

  The ``database`` argument is now optional and as a result was moved after the ``sql`` argument which is a positional
  one. Update your DAGs accordingly if they rely on argument order. Applies to:

  * ``RedshiftDataHook``'s ``execute_query`` method
  * ``RedshiftDataOperator``

  ``RedshiftDataHook``'s ``execute_query`` method now returns a ``QueryExecutionOutput`` object instead of just the
  statement ID as a string.

  ``RedshiftDataHook``'s ``parse_statement_resposne`` method was renamed to ``parse_statement_response``.

  ``S3ToRedshiftOperator``'s ``schema`` argument is now optional and was moved after the ``s3_key`` positional argument.
  Update your DAGs accordingly if they rely on argument order.


.. warning::
  All deprecated classes, parameters and features have been removed from the Amazon provider package.
  The following breaking changes were introduced:

  * Hooks

    * Removed ``sleep_time`` parameter from ``AthenaHook``. Use ``poll_query_status`` instead
    * Removed ``BaseAsyncSessionFactory``
    * Removed ``AwsBaseAsyncHook``
    * Removed ``start_from_head`` parameter from ``AwsLogsHook.get_log_events`` method
    * Removed ``sts_hook`` property from ``QuickSightHook``
    * Removed ``RedshiftAsyncHook``
    * Removed S3 connection type. Please use ``aws`` as ``conn_type`` instead, and specify ``bucket_name`` in ``service_config.s3`` within ``extras``
    * Removed ``wait_for_completion``, ``check_interval`` and ``verbose`` parameters from ``SageMakerHook.start_pipeline`` method
    * Removed ``wait_for_completion``, ``check_interval`` and ``verbose`` parameters from ``SageMakerHook.stop_pipeline`` method

  * Operators

    * Removed ``source`` parameter from ``AppflowRunOperator``
    * Removed ``overrides`` parameter from ``BatchOperator``. Use ``container_overrides`` instead
    * Removed ``status_retries`` parameter from ``BatchCreateComputeEnvironmentOperator``
    * Removed ``get_hook`` method from ``DataSyncOperator``. Use ``hook`` property instead
    * Removed ``wait_for_completion``, ``waiter_delay`` and ``waiter_max_attempts`` parameters from ``EcsDeregisterTaskDefinitionOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``wait_for_completion``, ``waiter_delay`` and ``waiter_max_attempts`` parameters from ``EcsRegisterTaskDefinitionOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``eks_hook`` property from ``EksCreateClusterOperator``. Use ``hook`` property instead
    * Removed ``pod_context``, ``pod_username`` and ``is_delete_operator_pod`` parameters from ``EksPodOperator``
    * Removed ``waiter_countdown`` and ``waiter_check_interval_seconds`` parameters from ``EmrStartNotebookExecutionOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``waiter_countdown`` and ``waiter_check_interval_seconds`` parameters from ``EmrStopNotebookExecutionOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``max_tries`` parameter from ``EmrContainerOperator``. Use ``max_polling_attempts`` instead
    * Removed ``waiter_countdown`` and ``waiter_check_interval_seconds`` parameters from ``EmrCreateJobFlowOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``waiter_countdown`` and ``waiter_check_interval_seconds`` parameters from ``EmrServerlessCreateApplicationOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``waiter_countdown`` and ``waiter_check_interval_seconds`` parameters from ``EmrServerlessStartJobOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``waiter_countdown`` and ``waiter_check_interval_seconds`` parameters from ``EmrServerlessStopApplicationOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``waiter_countdown`` and ``waiter_check_interval_seconds`` parameters from ``EmrServerlessDeleteApplicationOperator``. Please use ``waiter_max_attempts`` and ``waiter_delay`` instead
    * Removed ``delay`` parameter from ``GlueDataBrewStartJobOperator``. Use ``waiter_delay`` instead
    * Removed ``hook_params`` parameter from ``RdsBaseOperator``
    * Removed ``increment`` as possible value from ``action_if_job_exists`` parameter from ``SageMakerProcessingOperator``
    * Removed ``increment`` as possible value from ``action_if_job_exists`` parameter from ``SageMakerTransformOperator``
    * Removed ``increment`` as possible value from ``action_if_job_exists`` parameter from ``SageMakerTrainingOperator``

  * Secrets

    * Removed from ``full_url_mode`` and ``are_secret_values_urlencoded`` as possible key in ``kwargs`` from ``SecretsManagerBackend``

  * Sensors

    * Removed ``get_hook`` method from ``BatchSensor``. Use ``hook`` property instead
    * Removed ``get_hook`` method from ``DmsTaskBaseSensor``. Use ``hook`` property instead
    * Removed ``get_hook`` method from ``EmrBaseSensor``. Use ``hook`` property instead
    * Removed ``get_hook`` method from ``GlueCatalogPartitionSensor``. Use ``hook`` property instead
    * Removed ``get_hook`` method from ``GlueCrawlerSensor``. Use ``hook`` property instead
    * Removed ``quicksight_hook`` property from ``QuickSightSensor``. Use ``QuickSightSensor.hook`` instead
    * Removed ``sts_hook`` property from ``QuickSightSensor``
    * Removed ``get_hook`` method from ``RedshiftClusterSensor``. Use ``hook`` property instead
    * Removed ``get_hook`` method from ``S3KeySensor``. Use ``hook`` property instead
    * Removed ``get_hook`` method from ``SageMakerBaseSensor``. Use ``hook`` property instead
    * Removed ``get_hook`` method from ``SqsSensor``. Use ``hook`` property instead
    * Removed ``get_hook`` method from ``StepFunctionExecutionSensor``. Use ``hook`` property instead

  * Transfers

    * Removed ``aws_conn_id`` parameter from ``AwsToAwsBaseOperator``. Use ``source_aws_conn_id`` instead
    * Removed ``bucket`` and ``delimiter`` parameters from ``GCSToS3Operator``. Use ``gcs_bucket`` instead of ``bucket``

  * Triggers

    * Removed ``BatchOperatorTrigger``. Use ``BatchJobTrigger`` instead
    * Removed ``BatchSensorTrigger``. Use ``BatchJobTrigger`` instead
    * Removed ``region`` parameter from ``EksCreateFargateProfileTrigger``. Use ``region_name`` instead
    * Removed ``region`` parameter from ``EksDeleteFargateProfileTrigger``. Use ``region_name`` instead
    * Removed ``poll_interval`` and ``max_attempts`` parameters from ``EmrCreateJobFlowTrigger``. Use ``waiter_delay`` and ``waiter_max_attempts`` instead
    * Removed ``poll_interval`` and ``max_attempts`` parameters from ``EmrTerminateJobFlowTrigger``. Use ``waiter_delay`` and ``waiter_max_attempts`` instead
    * Removed ``poll_interval`` parameter from ``EmrContainerTrigger``. Use ``waiter_delay`` instead
    * Removed ``poll_interval`` parameter from ``GlueCrawlerCompleteTrigger``. Use ``waiter_delay`` instead
    * Removed ``delay`` and ``max_attempts`` parameters from ``GlueDataBrewJobCompleteTrigger``. Use ``waiter_delay`` and ``waiter_max_attempts`` instead
    * Removed ``RdsDbInstanceTrigger``. Use the other RDS triggers such as ``RdsDbDeletedTrigger``, ``RdsDbStoppedTrigger`` or ``RdsDbAvailableTrigger``
    * Removed ``poll_interval`` and ``max_attempts`` parameters from ``RedshiftCreateClusterTrigger``. Use ``waiter_delay`` and ``waiter_max_attempts`` instead
    * Removed ``poll_interval`` and ``max_attempts`` parameters from ``RedshiftPauseClusterTrigger``. Use ``waiter_delay`` and ``waiter_max_attempts`` instead
    * Removed ``poll_interval`` and ``max_attempts`` parameters from ``RedshiftCreateClusterSnapshotTrigger``. Use ``waiter_delay`` and ``waiter_max_attempts`` instead
    * Removed ``poll_interval`` and ``max_attempts`` parameters from ``RedshiftResumeClusterTrigger``. Use ``waiter_delay`` and ``waiter_max_attempts`` instead
    * Removed ``poll_interval`` and ``max_attempts`` parameters from ``RedshiftDeleteClusterTrigger``. Use ``waiter_delay`` and ``waiter_max_attempts`` instead
    * Removed ``SageMakerTrainingPrintLogTrigger``. Use ``SageMakerTrigger`` instead

  * Utils

    * Removed ``test_endpoint_url`` as possible key in ``extra_config`` from ``AwsConnectionWrapper``. Please set ``endpoint_url`` in ``service_config.sts`` within ``extras``
    * Removed ``s3`` as possible value in ``conn_type`` from ``AwsConnectionWrapper``. Please update your connection to have ``conn_type='aws'``
    * Removed ``session_kwargs`` as key in connection extra config. Please specify arguments passed to boto3 session directly
    * Removed ``host`` from AWS connection, please set it in ``extra['endpoint_url']`` instead
    * Removed ``region`` parameter from ``AwsHookParams``. Use ``region_name`` instead

* ``Remove deprecated stuff from Amazon provider package (#42450)``
* ``Support session reuse in 'RedshiftDataOperator' (#42218)``

Features
~~~~~~~~

* ``Add STOPPED to the failure cases for Sagemaker Training Jobs (#42423)``

Bug Fixes
~~~~~~~~~

* ``'S3DeleteObjects' Operator: Handle dates passed as strings (#42464)``
* ``Small fix to AWS AVP cli init script (#42479)``
* ``Make the AWS logging faster by reducing the amount of sleep (#42449)``
* ``Fix logout in AWS auth manager (#42447)``
* ``fix(providers/amazon): handle ClientError raised after key is missing during table.get_item (#42408)``

Misc
~~~~

* ``Drop python3.8 support core and providers (#42766)``
* ``Removed conditional check for task context logging in airflow version 2.8.0 and above (#42764)``
* ``Rename dataset related python variable names to asset (#41348)``
* ``Remove identity center auth manager cli (#42481)``
* ``Refactor AWS Auth manager user output (#42454)``
* ``Remove 'sqlalchemy-redshift' dependency from Amazon provider (#42830)``
* ``Revert "Remove 'sqlalchemy-redshift' dependency from Amazon provider" (#42864)``

8.29.0
......

Features
~~~~~~~~

* ``Adding support for volume configurations in ECSRunTaskOperator (#42087)``
* ``Openlineage s3 to redshift operator integration (#41575)``

Bug Fixes
~~~~~~~~~

* ``ECSExecutor: Drop params that aren't compatible with EC2 (#42228)``
* ``Fix 'GlueDataBrewStartJobOperator' template fields (#42073)``
* ``validate aws service exceptions in waiters (#41941)``
* ``Fix treatment of "#" in S3Hook.parse_s3_url() (#41796)``
* ``fix: remove part of openlineage extraction from S3ToRedshiftOperator (#41631)``
* ``filename template arg in providers file task handlers backward compitability support (#41633)``
* ``fix: select_query should have precedence over default query in RedshiftToS3Operator (#41634)``

Misc
~~~~

* ``Actually move saml to amazon provider (mistakenly added in papermill) (#42148)``
* ``Use base aws classes in AWS Glue DataBrew Operators/Triggers (#41848)``
* ``Move 'register_views' to auth manager interface (#41777)``
* ``airflow.models.taskinstance deprecations removed (#41784)``
* ``remove deprecated soft_fail from providers (#41710)``
* ``remove deprecated soft_fail from providers part2 (#41727)``
* ``Limit watchtower as depenendcy as 3.3.0 breaks moin. (#41612)``
* ``Remove deprecated log handler argument filename_template (#41552)``

8.28.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. warning:: When deferrable mode was introduced for ``RedshiftDataOperator``, in version 8.17.0, tasks configured with
  ``deferrable=True`` and ``wait_for_completion=True`` would not enter the deferred state. Instead, the task would occupy
  an executor slot until the statement was completed. A workaround may have been to set ``wait_for_completion=False``.
  In this version, tasks set up with ``wait_for_completion=False`` will not wait anymore, regardless of the value of
  ``deferrable``.

Features
~~~~~~~~

* ``Add incremental export and cross account export functionality in 'DynamoDBToS3Operator' (#41304)``
* ``EKS Overrides for AWS Batch submit_job (#40718)``

Bug Fixes
~~~~~~~~~

* ``Fix 'AwsTaskLogFetcher' missing logs (#41515)``
* ``Fix the Exception name and unpin dependency in 'RdsHook' (#41256)``
* ``Fix RedshiftDataOperator not running in deferred mode as expected (#41206)``

Misc
~~~~

* ``Partial fix for example_dynamodb_to_s3.py (#41517)``
* ``Remove deprecated code is AWS provider (#41407)``
* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``
* ``Limit moto temporarily - 5.0.12 is breaking our tests (#41244)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``typo (#41381)``

8.27.0
......

Features
~~~~~~~~

* ``Add RedriveExecution support to 'StepFunctionStartExecutionOperator' (#40976)``
* ``openlineage: add support for hook lineage for S3Hook (#40819)``
* ``Introduce Amazon Kinesis Analytics V2 (Managed Service for Apache Flink application)  (#40765)``

Bug Fixes
~~~~~~~~~

* ``Make EMR Container Trigger max attempts retries match the Operator (#41008)``
* ``Fix 'RdsStopDbOperator' operator in deferrable mode (#41059)``
* ``Fix 'RedshiftCreateClusterOperator' to always specify 'PubliclyAccessible' (#40872)``
* ``Fix Redshift cluster operators and sensors using deferrable mode (#41191)``
* ``Fix 'EmrServerlessStartJobOperator' with deferrable mode (#41103)``

Misc
~~~~

* ``Update 'example_redshift' and 'example_redshift_s3_transfers' to use 'RedshiftDataHook' instead of 'RedshiftSQLHook' (#40970)``
* ``openlineage: migrate OpenLineage provider to V2 facets. (#39530)``
* ``[AIP-62] Translate AIP-60 URI to OpenLineage (#40173)``
* ``Move AWS Managed Service for Apache Flink sensor states to Hook (#40896)``
* ``Replace usages of task context logger with the log table (#40867)``
* ``Deprecate 'SageMakerTrainingPrintLogTrigger' (#41158)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare Providers docs ad hoc release (#41074)``

8.26.0
......

.. note::
  Reduce memory footprint of S3KeyTrigger
  Decorator ``provide_bucket_name_async`` is removed.
  Async does not require a separated decorator.
  The old one is removed and users can use ``provide_bucket_name`` for coroutine functions, async iterators, and normal synchronous functions.
  Hook method ``get_file_metadata_async`` is now an async iterator
  Previously, the metadata objects were accumulated in a list.  Now the objects are yielded as we page through the results.  To get a list you may use ``async for`` in a list comprehension.
  S3KeyTrigger avoids loading all positive matches into memory in some circumstances

.. note::
  This release contains significant resources utilization improvements for async sessions

Features
~~~~~~~~

* ``Do not dynamically determine op links for emr serverless (#40627)``
* ``Be able to remove ACL in S3 hook's copy_object (#40518)``
* ``feat(aws): provide the context to check_fn in S3 sensor (#40686)``

Bug Fixes
~~~~~~~~~

* ``fix OpenLineage extraction for AthenaOperator (#40545)``
* ``Reduce memory footprint of s3 key trigger (#40473)``
* ``Adding cluster to ecs trigger event to avoid defer error (#40482)``
* ``Fix deferrable AWS SageMaker operators (#40706)``
* ``Make 'AwsAuthManager' compatible with only Airflow >= 2.9 (#40690)``
* ``Add serialization opt to s3 operator (#40659)``

Misc
~~~~

* ``Use base aws classes in AWS Glue Data Catalog Sensors (#40492)``
* ``Use base aws classes in AWS Glue Crawlers Operators/Sensors/Triggers (#40504)``
* ``Share data loader to across asyncio boto sessions (#40658)``
* ``Send executor logs to task logs in 'EcsExecutor' (#40468)``
* ``Send executor logs to task logs in 'AwsBatchExecutor' (#40698)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix docs build re aws changelog (#40488)``
   * ``Remove todo re bucket_name decorator in s3 hook (#40485)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``
   * ``Prepare docs 1st wave July 2024 (#40644)``

8.25.0
......

Features
~~~~~~~~

* ``Add Amazon Comprehend Document Classifier (#40287)``

Bug Fixes
~~~~~~~~~

* ``Fix 'importlib_metadata' import in aws utils (#40134)``
* ``openlineage, redshift: do not call DB for schemas below Airflow 2.10 (#40197)``
* ``Lazy match escaped quotes in 'RedshiftToS3Operator' (#40206)``
* ``Use stdlib 'importlib.metadata' for retrieve 'botocore' package version (#40137)``

Misc
~~~~

* ``Update pandas minimum requirement for Python 3.12 (#40272)``

8.24.0
......

Features
~~~~~~~~

* ``ECS Overrides for AWS Batch submit_job (#39903)``
* ``Add transfer operator S3ToDynamoDBOperator (#39654)``
* ``Adding Glue Data Quality Rule Recommendation Run  (#40014)``
* ``Allow user-specified object attributes to be used in check_fn for S3KeySensor (#39950)``
* ``Adding Amazon Glue Data Quality Service (#39923)``

Bug Fixes
~~~~~~~~~

* ``Deduplicate model name in SageMakerTransformOperator (#39956)``
* ``Fix: remove process_func from templated_fields (#39948)``
* ``Fix aws assume role session creation when deferrable (#40051)``

Misc
~~~~

* ``Resolving ECS fargate deprecated warnings (#39834)``
* ``Resolving EMR notebook deprecated warnings (#39829)``
* ``Bump boto min versions (#40052)``
* ``docs: mention minimum boto3 1.34.52 for AWS provider when using Batch 'ecs_properties_override' (#39983)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Implement per-provider tests with lowest-direct dependency resolution (#39946)``
   * ``Resolve aws emr deprecations in tests (#40020)``
   * ``Prepare docs 4th wave May 2024 (#39934)``

8.23.0
......

Features
~~~~~~~~

* ``Amazon Bedrock - Retrieve and RetrieveAndGenerate (#39500)``
* ``Introduce Amazon Comprehend Service (#39592)``

Bug Fixes
~~~~~~~~~

* ``fix: empty openlineage dataset name for AthenaExtractor (#39677)``
* ``Fix default value for aws batch operator retry strategy (#39608)``
* ``Sagemaker trigger: pass the job name as part of the event (#39671)``
* ``Handle task adoption for batch executor (#39590)``
* ``bugfix: handle invalid cluster states in NeptuneStopDbClusterOperator (#38287)``
* ``Fix automatic termination issue in 'EmrOperator' by ensuring 'waiter_max_attempts' is set for deferrable triggers (#38658)``

Misc
~~~~

* ``Resolving EMR deprecated warnings (#39743)``
* ``misc: add comment about remove unused code (#39748)``

8.22.0
......

Features
~~~~~~~~

* ``'S3DeleteObjectsOperator' Added ability to filter keys by last modified time (#39151)``
* ``Amazon Bedrock - Add Knowledge Bases and Data Sources integration (#39245)``

Bug Fixes
~~~~~~~~~

* ``EcsExcecutor Scheduler to handle incrementing of try_number (#39336)``
* ``ECS Executor: Set tasks to RUNNING state once active (#39212)``

Misc
~~~~

* ``Add 'jmespath' as an explicit dependency (#39350)``
* ``Drop 'xmlsec' dependency (#39534)``
* ``Reapply templates for all providers (#39554)``
* ``Faster 'airflow_version' imports (#39552)``
* ``enh(amazon_hook): raise not found exception instead of general exception when download file (#39509)``
* ``Simplify 'airflow_version' imports (#39497)``

8.21.0
......

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Added logging_config,snapstart,ephemeral_storage parameters to aws lambdacreatefunctionoperator (#39300)``

Bug Fixes
~~~~~~~~~

* ``Fix bug in GlueJobOperator where consecutive runs fail when a local script file is used (#38960)``
* ``Update 'is_authorized_custom_view' from auth manager to handle custom actions (#39167)``
* ``Update logic to allow retries in AWS Batch Client hook to be effective (#38998)``
* ``Amazon Bedrock - Model Throughput Provisioning (#38850)``

Misc
~~~~

* ``Adding MSGraphOperator in Microsoft Azure provider (#38111)``
* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``Allow importing the aws executors with a shorter path (#39093)``
* ``Remove flag from AWS auth manager to use it (#39033)``
* ``Limit xmlsec<1.3.14  (#39104)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Rename "try_number" increments that are unrelated to the airflow concept (#39317)``
   * ``Activate RUF015 that checks for unnecessary iterable allocation for first element (#38949)``
   * ``Add tests for 'EmrServerlessJobSensor' and 'EmrServerlessApplicationSensor' (#39099)``

8.20.0
......

Features
~~~~~~~~

* ``AWS Batch Executor (#37618)``
* ``Add fallback 'region_name' value to AWS Executors (#38704)``
* ``Introduce Amazon Bedrock service (#38602)``
* ``Amazon Bedrock - Model Customization Jobs (#38693)``
* ``ECS Executor - add support to adopt orphaned tasks. (#37786)``
* ``Update AWS auth manager CLI command to not disable AVP schema validation (#38301)``

Bug Fixes
~~~~~~~~~

* ``Reduce 's3hook' memory usage (#37886)``
* ``Add check in AWS auth manager to check if the Amazon Verified Permissions schema is up to date (#38333)``
* ``fix: EmrServerlessStartJobOperator not serializing DAGs correctly when partial/expand is used. (#38022)``
* ``fix(amazon): add return statement to yield within a while loop in triggers (#38396)``
* ``Fix set deprecated amazon operators arguments in 'MappedOperator' (#38346)``
* ``'ECSExecutor' API Retry bug fix (#38118)``
* ``Fix 'region' argument in 'MappedOperator' based on 'AwsBaseOperator' / 'AwsBaseSensor' (#38178)``
* ``Fix bug for ECS Executor where tasks were being skipped if one task failed. (#37979)``
* ``Fix init checks for aws redshift to s3 operator (#37861)``

Misc
~~~~

* ``Make the method 'BaseAuthManager.is_authorized_custom_view' abstract (#37915)``
* ``Replace "Role" by "Group" in AWS auth manager (#38078)``
* ``Avoid use of 'assert' outside of the tests (#37718)``
* ``Use 'AwsLogsHook' when fetching Glue job logs (#38010)``
* ``Implement 'filter_permitted_dag_ids' in AWS auth manager (#37666)``
* ``AWS auth manager CLI: persist the policy store description when doing updates (#37946)``
* ``Change f-string to formatting into the logging messages for Batch Executor (#37929)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``Add back system test for AWS auth manager (#38044)``
   * ``Revert "Add system test to test the AWS auth manager (#37947)" (#38004)``
   * ``Add system test to test the AWS auth manager (#37947)``
   * ``fix: try002 for provider amazon (#38789)``
   * ``Typo fix (#38783)``
   * ``fix: COMMAND string should be raw to avoid SyntaxWarning: invalid escape sequence '\s' (#38734)``
   * ``Revert "fix: COMMAND string should be raw to avoid SyntaxWarning: invalid escape sequence '\s' (#38734)" (#38864)``

8.19.0
......

Features
~~~~~~~~

* ``Implement 'filter_permitted_menu_items' in AWS auth manager (#37627)``
* ``Implement 'batch_is_authorized_*' APIs in AWS auth manager (#37430)``

Bug Fixes
~~~~~~~~~

* ``Fix init checks for aws 'eks' (#37674)``
* ``Fix init checks for aws gcs_to_s3 (#37662)``


Misc
~~~~

* ``Use named loggers instead of root logger (#37801)``
* ``Avoid non-recommended usage of logging (#37792)``
* ``Unify 'aws_conn_id' type to always be 'str | None' (#37768)``
* ``Limit 'pandas' to '<2.2' (#37748)``
* ``Implement AIP-60 Dataset URI formats (#37005)``
* ``Bump min versions of openapi validators (#37691)``
* ``Update action names in AWS auth manager (#37572)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix 'example_emr' system test (#37667)``
   * ``Avoid to use too broad 'noqa' (#37862)``
   * ``Resolve G003: "Logging statement uses +" (#37848)``
   * ``D105 Check on Amazon (#37764)``

8.18.0
......

Features
~~~~~~~~

* ``ECS Executor - Add backoff on failed task retry (#37109)``
* ``SqlToS3Operator: feat/ add max_rows_per_file parameter (#37055)``
* ``Adding Amazon Neptune Hook and Operators (#37000)``
* ``Add retry configuration in 'EmrContainerOperator' (#37426)``
* ``Create CLI commands for AWS auth manager to create AWS Identity Center related resources (#37407)``
* ``Add extra operator links for EMR Serverless (#34225)``

Bug Fixes
~~~~~~~~~

* ``Fix 'log_query' to format SQL statement correctly in 'AthenaOperator' (#36962)``
* ``check sagemaker training job status before deferring 'SageMakerTrainingOperator' (#36685)``

Misc
~~~~

* ``Merge all ECS executor configs following recursive python dict update (#37137)``
* ``Update default value for 'BatchSensor' (#37234)``
* ``remove info log from download_file (#37211)``
* ``S3ToRedshiftOperator templating aws_conn_id (#37195)``
* ``Updates to ECS Docs (#37125)``
* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``
* ``Replace usage of 'datetime.utcnow' and 'datetime.utcfromtimestamp' in providers (#37138)``
* ``add type annotations to Amazon provider "execute_coplete" methods (#36330)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 support in amazon provider (#37275)``
   * ``Upgrade mypy to 1.8.0 (#36428)``
   * ``Make Amazon Provider tests compatible with 'moto>=5' (#37060)``
   * ``Limit moto to version below 5.0.0 (#37054)``
   * ``docs: Add doc page with providers deprecations (#37075)``
   * ``Prepare docs 1st wave of Providers February 2024 (#37326)``

8.17.0
......

Features
~~~~~~~~

* ``add deferrable mode to RedshiftDataOperator (#36586)``
* ``Adds support for capacity providers to ECS Executor (#36722)``
* ``Add use_regex argument for allowing 'S3KeySensor' to check s3 keys with regular expression (#36578)``
* ``Add deferrable mode to RedshiftClusterSensor (#36550)``
* ``AthenaSqlHook implementation (#36171)``
* ``Create CLI commands for AWS auth manager to create Amazon Verified Permissions related resources (#36799)``
* ``Implement 'is_authorized_dag' in AWS auth manager (#36619)``

Bug Fixes
~~~~~~~~~

* ``Fix stacklevel in warnings.warn into the providers (#36831)``
* ``EC2 'CreateInstance': terminate instances in on_kill (#36828)``
* ``Fallback to default value if '[aws] cloudwatch_task_handler_json_serializer' not set (#36851)``
* ``AWS auth manager: raise AirflowOptionalProviderfeature exception for AVP command (#36824)``
* ``check transform job status before deferring SageMakerTransformOperator (#36680)``
* ``check sagemaker processing job status before deferring (#36658)``
* ``check job_status before BatchOperator execute in deferrable mode (#36523)``
* ``Update the redshift hostname check to avoid possible bugs (#36703)``
* ``Refresh credentials in 'AwsEcsExecutor' (#36179)``

Misc
~~~~

* ``Fix docstring for apply_wildcard parameter in 'S3ListOperator'. Changed the order of docstring for fix (#36679)``
* ``Use base aws classes in AWS DMS Operators/Sensors (#36772)``
* ``Use base aws classes in AWS Redshift Data API Operators (#36764)``
* ``Use base aws classes in Amazon EventBridge Operators (#36765)``
* ``Use base aws classes in Amazon QuickSight Operators/Sensors (#36776)``
* ``Use base aws classes in AWS Datasync Operators (#36766)``
* ``Use base aws classes in Amazon DynamoDB Sensors (#36770)``
* ``Use base aws classes in AWS CloudFormation Operators/Sensors (#36771)``
* ``Set min pandas dependency to 1.2.5 for all providers and airflow (#36698)``
* ``Bump min version of amazon-provider related dependencies (#36660)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

8.16.0
......

Features
~~~~~~~~

* ``Add AWS Step Functions links (#36599)``
* ``Add OpenLineage support for Redshift SQL (#35794)``

Bug Fixes
~~~~~~~~~

* ``Fix assignment of template field in '__init__' in 'AwsToAwsBaseOperator' (#36604)``
* ``Fix assignment of template field in '__init__' in 'DataSyncOperator' (#36605)``
* ``Check redshift cluster state before deferring to triggerer (#36416)``

Misc
~~~~

* ``Use base aws classes in Amazon SQS Operators/Sensors/Triggers (#36613)``
* ``Use base aws classes in Amazon SNS Operators (#36615)``
* ``Use base aws classes in AWS Step Functions Operators/Sensors/Triggers (#36468)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Select ruff B006 to detect the usage of mutable values as argument default (#36626)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``


8.15.0
......

Features
~~~~~~~~

* ``Add Amazon Athena query results extra link (#36447)``

Bug Fixes
~~~~~~~~~

* ``fix(providers/amazon): remove event['message'] call in EmrContainerOperator.execute_complete|as the key message no longer exists (#36417)``
* ``handle tzinfo in S3Hook.is_keys_unchanged_async (#36363)``

Misc
~~~~

* ``Use base aws classes in Amazon ECS Operators/Sensors/Triggers (#36393)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.14.0
......

Features
~~~~~~~~

* ``Add 'jsonpath_ng.ext.parse' support for 'SqsSensor' (#36170)``
* ``Increase ConflictException retries to 4 total (#36337)``
* ``Increase width of execution_date input in trigger.html (#36278) (#36304)``
* ``Allow storage options to be passed (#35820)``

Bug Fixes
~~~~~~~~~

* ``Remove 'is_authorized_cluster_activity' from auth manager (#36175)``
* ``Follow BaseHook connection fields method signature in child classes (#36086)``

Misc
~~~~

* ``Add code snippet formatting in docstrings via Ruff (#36262)``
* ``Remove remaining Airflow 2.6 backcompat code from Amazon Provider (#36324)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.13.0
......

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Support IAM authentication for Redshift serverless (#35897)``
* ``Implement 'is_authorized_variable' in AWS auth manager (#35804)``
* ``Enhance 'attribute_value' in 'DynamoDBValueSensor' to accept list (#35831)``

Bug Fixes
~~~~~~~~~

* ``Fix handling of single quotes in 'RedshiftToS3Operator' (#35986)``
* ``Fix a bug in get_iam_token for Redshift Serverless (#36001)``
* ``Fix reraise outside of try block in 'AthenaHook.get_output_location' (#36008)``
* ``Fix a bug with accessing hooks in EKS trigger (#35989)``
* ``Fix a bug in method name used in 'GlacierToGCSOperator' (#35978)``
* ``Fix EC2Hook get_instance for client_type api (#35960)``
* ``Avoid creating the hook in the EmrServerlessCancelJobsTrigger init (#35992)``
* ``Stop getting message from event after migrating 'EmrContainerTrigger' to 'AwsBaseWaiterTrigger' (#35892)``
* ``Fix for 'EksCreateClusterOperator' deferrable mode (#36079)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``
* ``Update 'boto3' and 'botocore' versions notes (#36073)``
* ``Improve typing hints for only_client_type decorator (#35997)``
* ``Refactor some methods in EmrContainerHook (#35999)``
* ``Refactor get_output_location in AthenaHook (#35996)``
* ``Move RDS hook to a cached property in RDS trigger (#35990)``
* ``Replace default empty dict value by None in AzureBlobStorageToS3Operator (#35977)``
* ``Update 'set_context' signature to match superclass one and stop setting the instance attribute in CloudwatchTaskHandler (#35975)``
* ``Use S3 hook instead of AwsGenericHook in AWS S3 FS (#35973)``
* ``AWS auth manager: implement all 'is_authorized_*' methods (but 'is_authorized_dag') (#35928)``
* ``Remove setting a non-existing object param and use local var instead in S3Hook (#35950)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add feature to build "chicken-egg" packages from sources (#35890)``
   * ``Fix AWS system tests (#36091)``

8.12.0
......

Features
~~~~~~~~

* ``Add 'EC2HibernateInstanceOperator' and 'EC2RebootInstanceOperator' (#35790)``
* ``Add OpenLineage support to 'S3FileTransformOperator' (#35819)``
* ``Add OpenLineage support to S3Operators - Copy, Delete and Create Object (#35796)``
* ``Added retry strategy parameter to Amazon AWS provider Batch Operator to allow dynamic Batch retry strategies (#35789)``
* ``Added name field to template_fields in EmrServerlessStartJobOperator (#35648)``
* ``openlineage, aws: Add OpenLineage support for AthenaOperator. (#35090)``
* ``Implement login and logout in AWS auth manager (#35488)``

Bug Fixes
~~~~~~~~~

* ``Fix Batch operator's retry_strategy (#35808)``
* ``Fix and reapply templates for provider documentation (#35686)``
* ``Make EksPodOperator exec config not rely on log level (#35771)``
* ``Fix 'configuration_overrides' parameter in 'EmrServerlessStartJobOperator' (#35787)``

Misc
~~~~

* ``Updated docstring: 'check_key_async' is now in line with description of '_check_key_async' (#35799)``
* ``Check attr on parent not self re TaskContextLogger set_context (#35780)``
* ``Allow a wider range of watchtower versions (#35713)``
* ``Extend task context logging support for remote logging using AWS S3 (#32950)``
* ``Log failure reason for containers if a task fails for ECS Executor (#35496)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use reproducible builds for providers (#35693)``
   * ``Update http to s3 system test (#35711)``

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

2.5.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Contains breaking changes``

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
