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
