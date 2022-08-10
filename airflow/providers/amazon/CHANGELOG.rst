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

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

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
