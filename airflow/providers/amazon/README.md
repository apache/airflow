<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->


# Package apache-airflow-backport-providers-amazon

Release: 2020.6.24

**Table of contents**

- [Backport package](#backport-package)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
        - [New operators](#new-operators)
        - [Moved operators](#moved-operators)
    - [Transfer operators](#transfers)
        - [New transfer operators](#new-transfers)
        - [Moved transfer operators](#moved-transfers)
    - [Sensors](#sensors)
        - [New sensors](#new-sensors)
        - [Moved sensors](#moved-sensors)
    - [Hooks](#hooks)
        - [New hooks](#new-hooks)
        - [Moved hooks](#moved-hooks)
    - [Secrets](#secrets)
        - [Moved secrets](#moved-secrets)
- [Releases](#releases)
    - [Release 2020.6.24](#release-2020624)

## Backport package

This is a backport providers package for `amazon` provider. All classes for this provider package
are in `airflow.providers.amazon` python package.

**Only Python 3.6+ is supported for this backport package.**

While Airflow 1.10.* continues to support Python 2.7+ - you need to upgrade python to 3.6+ if you
want to use this backport package.



## Installation

You can install this package on top of an existing airflow 1.10.* installation via
`pip install apache-airflow-backport-providers-amazon`

## Compatibility

For full compatibility and test status of the backport packages check
[Airflow Backport Package Compatibility](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)

## PIP requirements

| PIP package   | Version required   |
|:--------------|:-------------------|
| boto3         | &gt;=1.12.0,&lt;2.0.0    |
| watchtower    | ~=0.7.3            |

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-backport-providers-amazon[apache.hive]
```

| Dependent package                                                                                                            | Extra       |
|:-----------------------------------------------------------------------------------------------------------------------------|:------------|
| [apache-airflow-backport-providers-apache-hive](https://github.com/apache/airflow/tree/master/airflow/providers/apache/hive) | apache.hive |
| [apache-airflow-backport-providers-google](https://github.com/apache/airflow/tree/master/airflow/providers/google)           | google      |
| [apache-airflow-backport-providers-imap](https://github.com/apache/airflow/tree/master/airflow/providers/imap)               | imap        |
| [apache-airflow-backport-providers-mongo](https://github.com/apache/airflow/tree/master/airflow/providers/mongo)             | mongo       |
| [apache-airflow-backport-providers-mysql](https://github.com/apache/airflow/tree/master/airflow/providers/mysql)             | mysql       |
| [apache-airflow-backport-providers-postgres](https://github.com/apache/airflow/tree/master/airflow/providers/postgres)       | postgres    |
| [apache-airflow-backport-providers-ssh](https://github.com/apache/airflow/tree/master/airflow/providers/ssh)                 | ssh         |

# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `amazon` provider
are in the `airflow.providers.amazon` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators


### New operators

| New Airflow 2.0 operators: `airflow.providers.amazon` package                                                                                                              |
|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.operators.cloud_formation.CloudFormationCreateStackOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/cloud_formation.py) |
| [aws.operators.cloud_formation.CloudFormationDeleteStackOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/cloud_formation.py) |
| [aws.operators.datasync.AWSDataSyncOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/datasync.py)                             |
| [aws.operators.ec2_start_instance.EC2StartInstanceOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/ec2_start_instance.py)    |
| [aws.operators.ec2_stop_instance.EC2StopInstanceOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/ec2_stop_instance.py)       |
| [aws.operators.emr_modify_cluster.EmrModifyClusterOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/emr_modify_cluster.py)    |
| [aws.operators.glue.AwsGlueJobOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/glue.py)                                      |
| [aws.operators.s3_bucket.S3CreateBucketOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/s3_bucket.py)                        |
| [aws.operators.s3_bucket.S3DeleteBucketOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/s3_bucket.py)                        |
| [aws.operators.s3_file_transform.S3FileTransformOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/s3_file_transform.py)       |



### Moved operators

| Airflow 2.0 operators: `airflow.providers.amazon` package                                                                                                                                    | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                                                |
|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.operators.athena.AWSAthenaOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/athena.py)                                                     | [contrib.operators.aws_athena_operator.AWSAthenaOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/aws_athena_operator.py)                                             |
| [aws.operators.batch.AwsBatchOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/batch.py)                                                        | [contrib.operators.awsbatch_operator.AWSBatchOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/awsbatch_operator.py)                                                  |
| [aws.operators.ecs.ECSOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/ecs.py)                                                                 | [contrib.operators.ecs_operator.ECSOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/ecs_operator.py)                                                                 |
| [aws.operators.emr_add_steps.EmrAddStepsOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/emr_add_steps.py)                                     | [contrib.operators.emr_add_steps_operator.EmrAddStepsOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/emr_add_steps_operator.py)                                     |
| [aws.operators.emr_create_job_flow.EmrCreateJobFlowOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/emr_create_job_flow.py)                    | [contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/emr_create_job_flow_operator.py)                    |
| [aws.operators.emr_terminate_job_flow.EmrTerminateJobFlowOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/emr_terminate_job_flow.py)           | [contrib.operators.emr_terminate_job_flow_operator.EmrTerminateJobFlowOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/emr_terminate_job_flow_operator.py)           |
| [aws.operators.s3_copy_object.S3CopyObjectOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/s3_copy_object.py)                                  | [contrib.operators.s3_copy_object_operator.S3CopyObjectOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/s3_copy_object_operator.py)                                  |
| [aws.operators.s3_delete_objects.S3DeleteObjectsOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/s3_delete_objects.py)                         | [contrib.operators.s3_delete_objects_operator.S3DeleteObjectsOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/s3_delete_objects_operator.py)                         |
| [aws.operators.s3_list.S3ListOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/s3_list.py)                                                      | [contrib.operators.s3_list_operator.S3ListOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/s3_list_operator.py)                                                      |
| [aws.operators.sagemaker_base.SageMakerBaseOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sagemaker_base.py)                                 | [contrib.operators.sagemaker_base_operator.SageMakerBaseOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sagemaker_base_operator.py)                                 |
| [aws.operators.sagemaker_endpoint.SageMakerEndpointOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sagemaker_endpoint.py)                     | [contrib.operators.sagemaker_endpoint_operator.SageMakerEndpointOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sagemaker_endpoint_operator.py)                     |
| [aws.operators.sagemaker_endpoint_config.SageMakerEndpointConfigOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sagemaker_endpoint_config.py) | [contrib.operators.sagemaker_endpoint_config_operator.SageMakerEndpointConfigOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sagemaker_endpoint_config_operator.py) |
| [aws.operators.sagemaker_model.SageMakerModelOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sagemaker_model.py)                              | [contrib.operators.sagemaker_model_operator.SageMakerModelOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sagemaker_model_operator.py)                              |
| [aws.operators.sagemaker_training.SageMakerTrainingOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sagemaker_training.py)                     | [contrib.operators.sagemaker_training_operator.SageMakerTrainingOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sagemaker_training_operator.py)                     |
| [aws.operators.sagemaker_transform.SageMakerTransformOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sagemaker_transform.py)                  | [contrib.operators.sagemaker_transform_operator.SageMakerTransformOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sagemaker_transform_operator.py)                  |
| [aws.operators.sagemaker_tuning.SageMakerTuningOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sagemaker_tuning.py)                           | [contrib.operators.sagemaker_tuning_operator.SageMakerTuningOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sagemaker_tuning_operator.py)                           |
| [aws.operators.sns.SnsPublishOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sns.py)                                                          | [contrib.operators.sns_publish_operator.SnsPublishOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sns_publish_operator.py)                                          |
| [aws.operators.sqs.SQSPublishOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/operators/sqs.py)                                                          | [contrib.operators.aws_sqs_publish_operator.SQSPublishOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/aws_sqs_publish_operator.py)                                  |





### New transfer operators

| New Airflow 2.0 transfers: `airflow.providers.amazon` package                                                                                      |
|:---------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.transfers.mysql_to_s3.MySQLToS3Operator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/mysql_to_s3.py) |



### Moved transfer operators

| Airflow 2.0 transfers: `airflow.providers.amazon` package                                                                                                                       | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                                   |
|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.transfers.dynamodb_to_s3.DynamoDBToS3Operator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/dynamodb_to_s3.py)                     | [contrib.operators.dynamodb_to_s3.DynamoDBToS3Operator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/dynamodb_to_s3.py)                                       |
| [aws.transfers.gcs_to_s3.GCSToS3Operator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/gcs_to_s3.py)                                    | [operators.gcs_to_s3.GCSToS3Operator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/gcs_to_s3.py)                                                                      |
| [aws.transfers.google_api_to_s3.GoogleApiToS3Operator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/google_api_to_s3.py)                | [operators.google_api_to_s3_transfer.GoogleApiToS3Transfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/google_api_to_s3_transfer.py)                                |
| [aws.transfers.hive_to_dynamodb.HiveToDynamoDBOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/hive_to_dynamodb.py)               | [contrib.operators.hive_to_dynamodb.HiveToDynamoDBOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/hive_to_dynamodb.py)                                 |
| [aws.transfers.imap_attachment_to_s3.ImapAttachmentToS3Operator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/imap_attachment_to_s3.py) | [contrib.operators.imap_attachment_to_s3_operator.ImapAttachmentToS3Operator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/imap_attachment_to_s3_operator.py) |
| [aws.transfers.mongo_to_s3.MongoToS3Operator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/mongo_to_s3.py)                              | [contrib.operators.mongo_to_s3.MongoToS3Operator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/mongo_to_s3.py)                                                |
| [aws.transfers.redshift_to_s3.RedshiftToS3Operator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/redshift_to_s3.py)                     | [operators.redshift_to_s3_operator.RedshiftToS3Transfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/redshift_to_s3_operator.py)                                     |
| [aws.transfers.s3_to_redshift.S3ToRedshiftOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/s3_to_redshift.py)                     | [operators.s3_to_redshift_operator.S3ToRedshiftTransfer](https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/s3_to_redshift_operator.py)                                     |
| [aws.transfers.s3_to_sftp.S3ToSFTPOperator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/s3_to_sftp.py)                                 | [contrib.operators.s3_to_sftp_operator.S3ToSFTPOperator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/s3_to_sftp_operator.py)                                 |
| [aws.transfers.sftp_to_s3.SFTPToS3Operator](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/sftp_to_s3.py)                                 | [contrib.operators.sftp_to_s3_operator.SFTPToS3Operator](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/operators/sftp_to_s3_operator.py)                                 |




## Sensors


### New sensors

| New Airflow 2.0 sensors: `airflow.providers.amazon` package                                                                                                          |
|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.sensors.cloud_formation.CloudFormationCreateStackSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/cloud_formation.py) |
| [aws.sensors.cloud_formation.CloudFormationDeleteStackSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/cloud_formation.py) |
| [aws.sensors.ec2_instance_state.EC2InstanceStateSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/ec2_instance_state.py)    |
| [aws.sensors.glue.AwsGlueJobSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/glue.py)                                      |
| [aws.sensors.redshift.AwsRedshiftClusterSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/redshift.py)                      |
| [aws.sensors.sagemaker_training.SageMakerTrainingSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/sagemaker_training.py)   |


### Moved sensors

| Airflow 2.0 sensors: `airflow.providers.amazon` package                                                                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                                        |
|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.sensors.athena.AthenaSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/athena.py)                                                  | [contrib.sensors.aws_athena_sensor.AthenaSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/aws_athena_sensor.py)                                                  |
| [aws.sensors.emr_base.EmrBaseSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/emr_base.py)                                             | [contrib.sensors.emr_base_sensor.EmrBaseSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/emr_base_sensor.py)                                                     |
| [aws.sensors.emr_job_flow.EmrJobFlowSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/emr_job_flow.py)                                  | [contrib.sensors.emr_job_flow_sensor.EmrJobFlowSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/emr_job_flow_sensor.py)                                          |
| [aws.sensors.emr_step.EmrStepSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/emr_step.py)                                             | [contrib.sensors.emr_step_sensor.EmrStepSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/emr_step_sensor.py)                                                     |
| [aws.sensors.glue_catalog_partition.AwsGlueCatalogPartitionSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/glue_catalog_partition.py) | [contrib.sensors.aws_glue_catalog_partition_sensor.AwsGlueCatalogPartitionSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/aws_glue_catalog_partition_sensor.py) |
| [aws.sensors.s3_key.S3KeySensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/s3_key.py)                                                   | [sensors.s3_key_sensor.S3KeySensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/s3_key_sensor.py)                                                                           |
| [aws.sensors.s3_prefix.S3PrefixSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/s3_prefix.py)                                          | [sensors.s3_prefix_sensor.S3PrefixSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/s3_prefix_sensor.py)                                                                  |
| [aws.sensors.sagemaker_base.SageMakerBaseSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/sagemaker_base.py)                           | [contrib.sensors.sagemaker_base_sensor.SageMakerBaseSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/sagemaker_base_sensor.py)                                   |
| [aws.sensors.sagemaker_endpoint.SageMakerEndpointSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/sagemaker_endpoint.py)               | [contrib.sensors.sagemaker_endpoint_sensor.SageMakerEndpointSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/sagemaker_endpoint_sensor.py)                       |
| [aws.sensors.sagemaker_transform.SageMakerTransformSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/sagemaker_transform.py)            | [contrib.sensors.sagemaker_transform_sensor.SageMakerTransformSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/sagemaker_transform_sensor.py)                    |
| [aws.sensors.sagemaker_tuning.SageMakerTuningSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/sagemaker_tuning.py)                     | [contrib.sensors.sagemaker_tuning_sensor.SageMakerTuningSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/sagemaker_tuning_sensor.py)                             |
| [aws.sensors.sqs.SQSSensor](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/sensors/sqs.py)                                                           | [contrib.sensors.aws_sqs_sensor.SQSSensor](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/sensors/aws_sqs_sensor.py)                                                           |



## Hooks


### New hooks

| New Airflow 2.0 hooks: `airflow.providers.amazon` package                                                                                              |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.hooks.batch_client.AwsBatchClientHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/batch_client.py)          |
| [aws.hooks.batch_waiters.AwsBatchWaitersHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/batch_waiters.py)       |
| [aws.hooks.cloud_formation.AWSCloudFormationHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/cloud_formation.py) |
| [aws.hooks.ec2.EC2Hook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/ec2.py)                                       |
| [aws.hooks.glue.AwsGlueJobHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/glue.py)                              |
| [aws.hooks.kinesis.AwsFirehoseHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/kinesis.py)                       |
| [aws.hooks.redshift.RedshiftHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/redshift.py)                        |


### Moved hooks

| Airflow 2.0 hooks: `airflow.providers.amazon` package                                                                                          | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                 |
|:-----------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.hooks.athena.AWSAthenaHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/athena.py)                   | [contrib.hooks.aws_athena_hook.AWSAthenaHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_athena_hook.py)                  |
| [aws.hooks.aws_dynamodb.AwsDynamoDBHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/aws_dynamodb.py)     | [contrib.hooks.aws_dynamodb_hook.AwsDynamoDBHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_dynamodb_hook.py)            |
| [aws.hooks.base_aws.AwsBaseHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/base_aws.py)                 | [contrib.hooks.aws_hook.AwsHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_hook.py)                                      |
| [aws.hooks.datasync.AWSDataSyncHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/datasync.py)             | [contrib.hooks.aws_datasync_hook.AWSDataSyncHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_datasync_hook.py)            |
| [aws.hooks.emr.EmrHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/emr.py)                               | [contrib.hooks.emr_hook.EmrHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/emr_hook.py)                                      |
| [aws.hooks.glue_catalog.AwsGlueCatalogHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/glue_catalog.py)  | [contrib.hooks.aws_glue_catalog_hook.AwsGlueCatalogHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_glue_catalog_hook.py) |
| [aws.hooks.lambda_function.AwsLambdaHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/lambda_function.py) | [contrib.hooks.aws_lambda_hook.AwsLambdaHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_lambda_hook.py)                  |
| [aws.hooks.logs.AwsLogsHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/logs.py)                         | [contrib.hooks.aws_logs_hook.AwsLogsHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_logs_hook.py)                        |
| [aws.hooks.s3.S3Hook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/s3.py)                                  | [hooks.S3_hook.S3Hook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/hooks/S3_hook.py)                                                         |
| [aws.hooks.sagemaker.SageMakerHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/sagemaker.py)             | [contrib.hooks.sagemaker_hook.SageMakerHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/sagemaker_hook.py)                    |
| [aws.hooks.sns.AwsSnsHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/sns.py)                            | [contrib.hooks.aws_sns_hook.AwsSnsHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_sns_hook.py)                           |
| [aws.hooks.sqs.SQSHook](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/hooks/sqs.py)                               | [contrib.hooks.aws_sqs_hook.SQSHook](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/hooks/aws_sqs_hook.py)                              |




## Secrets



### Moved secrets

| Airflow 2.0 protocols: `airflow.providers.amazon` package                                                                                                                | Airflow 1.10.* previous location (usually `airflow.contrib`)                                                                                                                  |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [aws.secrets.secrets_manager.SecretsManagerBackend](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/secrets/secrets_manager.py)               | [contrib.secrets.aws_secrets_manager.SecretsManagerBackend](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/secrets/aws_secrets_manager.py)               |
| [aws.secrets.systems_manager.SystemsManagerParameterStoreBackend](https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/secrets/systems_manager.py) | [contrib.secrets.aws_systems_manager.SystemsManagerParameterStoreBackend](https://github.com/apache/airflow/blob/v1-10-stable/airflow/contrib/secrets/aws_systems_manager.py) |




## Releases

### Release 2020.6.24

| Commit                                                                                         | Committed   | Subject                                                                                                                                                            |
|:-----------------------------------------------------------------------------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [12af6a080](https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1) | 2020-06-19  | Final cleanup for 2020.6.23rc1 release preparation (#9404)                                                                                                         |
| [992a18c84](https://github.com/apache/airflow/commit/992a18c84a355d13e821c703e7364f12233c37dc) | 2020-06-19  | Move MySqlToS3Operator to transfers (#9400)                                                                                                                        |
| [a60f589aa](https://github.com/apache/airflow/commit/a60f589aa251cc3df6bec5b306ad4a7f736f539f) | 2020-06-19  | Add MySqlToS3Operator (#9054)                                                                                                                                      |
| [c7e5bce57](https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13) | 2020-06-19  | Prepare backport release candidate for 2020.6.23rc1 (#9370)                                                                                                        |
| [40bf8f28f](https://github.com/apache/airflow/commit/40bf8f28f97f17f40d993d207ea740eba54593ee) | 2020-06-18  | Detect automatically the lack of reference to the guide in the operator descriptions (#9290)                                                                       |
| [f6bd817a3](https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac) | 2020-06-16  | Introduce &#39;transfers&#39; packages (#9320)                                                                                                                             |
| [58a8ec0e4](https://github.com/apache/airflow/commit/58a8ec0e46f624ee0369dd156dd8fb4f81884a21) | 2020-06-16  | AWSBatchOperator &lt;&gt; ClientHook relation changed to composition (#9306)                                                                                             |
| [a80cd25e8](https://github.com/apache/airflow/commit/a80cd25e8eb7f8b5d89af26cdcd62a5bbe44d65c) | 2020-06-15  | Close/Flush byte stream in s3 hook load_string and load_bytes (#9211)                                                                                              |
| [ffb857403](https://github.com/apache/airflow/commit/ffb85740373f7adb70d28ec7d5a8886380170e5e) | 2020-06-14  | Decrypt secrets from SystemsManagerParameterStoreBackend (#9214)                                                                                                   |
| [a69b031f2](https://github.com/apache/airflow/commit/a69b031f20c5a1cd032f9873394374f661811e8f) | 2020-06-10  | Add S3ToRedshift example dag and system test (#8877)                                                                                                               |
| [17adcea83](https://github.com/apache/airflow/commit/17adcea835cb7b0cf2d8da0ac7dda5549cfa3e45) | 2020-06-02  | Fix handling of subprocess error handling in s3_file_transform and gcs (#9106)                                                                                     |
| [357e11e0c](https://github.com/apache/airflow/commit/357e11e0cfb4c02833018e073bc4f5e5b52fae4f) | 2020-05-29  | Add Delete/Create S3 bucket operators (#8895)                                                                                                                      |
| [1ed171bfb](https://github.com/apache/airflow/commit/1ed171bfb265ded8674058bdc425640d25f1f4fc) | 2020-05-28  | Add script_args for S3FileTransformOperator (#9019)                                                                                                                |
| [0b0e4f7a4](https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34) | 2020-05-26  | Preparing for RC3 relase of backports (#9026)                                                                                                                      |
| [00642a46d](https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c) | 2020-05-26  | Fixed name of 20 remaining wrongly named operators. (#8994)                                                                                                        |
| [1d36b0303](https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea) | 2020-05-23  | Fix references in docs (#8984)                                                                                                                                     |
| [f946f96da](https://github.com/apache/airflow/commit/f946f96da45d8e6101805450d8cab7ccb2774ad0) | 2020-05-23  | Old json boto compat removed from dynamodb_to_s3 operator (#8987)                                                                                                  |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                                                                                       |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                                                                                            |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                                                                                       |
| [f4edd90a9](https://github.com/apache/airflow/commit/f4edd90a94b8f91bbefbbbfba367372399559596) | 2020-05-16  | Speed up TestAwsLambdaHook by not actually running a function (#8882)                                                                                              |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                                                                                            |
| [85bbab27d](https://github.com/apache/airflow/commit/85bbab27dbb4f55f6f322b894fe3d54797076c15) | 2020-05-15  | Add EMR operators howto docs (#8863)                                                                                                                               |
| [e61b9bb9b](https://github.com/apache/airflow/commit/e61b9bb9bbe6d8a0621310f3583483b9135c6770) | 2020-05-13  | Add AWS EMR System tests (#8618)                                                                                                                                   |
| [ed3f5131a](https://github.com/apache/airflow/commit/ed3f5131a27e2ef0422f2495a4532630a6204f82) | 2020-05-13  | Correctly pass sleep time from AWSAthenaOperator down to the hook. (#8845)                                                                                         |
| [7236862a1](https://github.com/apache/airflow/commit/7236862a1f5361b5e99c03dd63dae9b966efcd24) | 2020-05-12  | [AIRFLOW-2310] Enable AWS Glue Job Integration (#6007)                                                                                                             |
| [d590e5e76](https://github.com/apache/airflow/commit/d590e5e7679322bebb1472fa8c7ec6d183e4154a) | 2020-05-11  | Add option to propagate tags in ECSOperator (#8811)                                                                                                                |
| [0c3db84c3](https://github.com/apache/airflow/commit/0c3db84c3ce5107f53ed5ecc48edfdfe1b97feff) | 2020-05-11  | [AIRFLOW-7068] Create EC2 Hook, Operator and Sensor (#7731)                                                                                                        |
| [cbebed2b4](https://github.com/apache/airflow/commit/cbebed2b4d0bd1e0984c331c0270e83bf8df8540) | 2020-05-10  | Allow passing backend_kwargs to AWS SSM client (#8802)                                                                                                             |
| [c7788a689](https://github.com/apache/airflow/commit/c7788a6894cb79c22153434dd9b977393b8236be) | 2020-05-10  | Add imap_attachment_to_s3 example dag and system test (#8669)                                                                                                      |
| [ff5b70149](https://github.com/apache/airflow/commit/ff5b70149bf51012156378c8fc8b072c7c280d9d) | 2020-05-07  | Add google_api_to_s3_transfer example dags and system tests (#8581)                                                                                                |
| [4421f011e](https://github.com/apache/airflow/commit/4421f011eeec2d1022a39933e27f530fb9f9c1b1) | 2020-05-01  | Improve template capabilities of EMR job and step operators (#8572)                                                                                                |
| [379a884d6](https://github.com/apache/airflow/commit/379a884d645a4d73db1c81e3450adc82571989ea) | 2020-04-28  | fix: aws hook should work without conn id (#8534)                                                                                                                  |
| [74bc316c5](https://github.com/apache/airflow/commit/74bc316c56192f14677e9406d3878887a836062b) | 2020-04-27  | [AIRFLOW-4438] Add Gzip compression to S3_hook (#8571)                                                                                                             |
| [7ea66a1a9](https://github.com/apache/airflow/commit/7ea66a1a9594704869e82513d3a06fe35b6109b2) | 2020-04-26  | Add example DAG for ECSOperator (#8452)                                                                                                                            |
| [b6434dedf](https://github.com/apache/airflow/commit/b6434dedf974085e5f8891446fa63104836c8fdf) | 2020-04-24  | [AIRFLOW-7111] Add generate_presigned_url method to S3Hook (#8441)                                                                                                 |
| [becedd5af](https://github.com/apache/airflow/commit/becedd5af8df01a0210e0a3fa78e619785f39908) | 2020-04-19  | Remove unrelated EC2 references in ECSOperator (#8451)                                                                                                             |
| [ab1290cb0](https://github.com/apache/airflow/commit/ab1290cb0c5856fa85c8596bfdf780fcdfd99c31) | 2020-04-13  | Make launch_type parameter optional (#8248)                                                                                                                        |
| [87969a350](https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca) | 2020-04-09  | [AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)                                                                                                   |
| [b46d6c060](https://github.com/apache/airflow/commit/b46d6c060280da59193a28cf67e791eb825cb51c) | 2020-04-08  | Add support for AWS Secrets Manager as Secrets Backend (#8186)                                                                                                     |
| [68d1714f2](https://github.com/apache/airflow/commit/68d1714f296989b7aad1a04b75dc033e76afb747) | 2020-04-04  | [AIRFLOW-6822] AWS hooks should cache boto3 client (#7541)                                                                                                         |
| [8a0240257](https://github.com/apache/airflow/commit/8a02402576f83869d5134b4bddef5d73c15a8320) | 2020-03-31  | Rename CloudBaseHook to GoogleBaseHook and move it to google.common (#8011)                                                                                        |
| [7239d9a82](https://github.com/apache/airflow/commit/7239d9a82dbb3b9bdf27b531daa70338af9dd796) | 2020-03-28  | Get Airflow Variables from AWS Systems Manager Parameter Store (#7945)                                                                                             |
| [eb4af4f94](https://github.com/apache/airflow/commit/eb4af4f944c77e67e167bbb6b0a2aaf075a95b50) | 2020-03-28  | Make BaseSecretsBackend.build_path generic (#7948)                                                                                                                 |
| [438da7241](https://github.com/apache/airflow/commit/438da7241eb537e3ef5ae711629446155bf738a3) | 2020-03-28  | [AIRFLOW-5825] SageMakerEndpointOperator is not idempotent (#7891)                                                                                                 |
| [686d7d50b](https://github.com/apache/airflow/commit/686d7d50bd21622724d6818021355bc6885fd3de) | 2020-03-25  | Standardize SecretBackend class names (#7846)                                                                                                                      |
| [eef87b995](https://github.com/apache/airflow/commit/eef87b9953347a65421f315a07dbef37ded9df66) | 2020-03-23  | [AIRFLOW-7105] Unify Secrets Backend method interfaces (#7830)                                                                                                     |
| [5648dfbc3](https://github.com/apache/airflow/commit/5648dfbc300337b10567ef4e07045ea29d33ec06) | 2020-03-23  | Add missing call to Super class in &#39;amazon&#39;, &#39;cloudant &amp; &#39;databricks&#39; providers (#7827)                                                                            |
| [a36002412](https://github.com/apache/airflow/commit/a36002412334c445e4eab41fdbb85ef31b6fd384) | 2020-03-19  | [AIRFLOW-5705] Make AwsSsmSecretsBackend consistent with VaultBackend (#7753)                                                                                      |
| [2a54512d7](https://github.com/apache/airflow/commit/2a54512d785ba603ba71381dc3dfa049e9f74063) | 2020-03-17  | [AIRFLOW-5705] Fix bugs in AWS SSM Secrets Backend (#7745)                                                                                                         |
| [a8b5fc74d](https://github.com/apache/airflow/commit/a8b5fc74d07e50c91bb64cb66ca1a450aa5ce6e1) | 2020-03-16  | [AIRFLOW-4175] S3Hook load_file should support ACL policy paramete (#7733)                                                                                         |
| [e31e9ddd2](https://github.com/apache/airflow/commit/e31e9ddd2332e5d92422baf668acee441646ad68) | 2020-03-14  | [AIRFLOW-5705] Add secrets backend and support for AWS SSM (#6376)                                                                                                 |
| [3bb60afc7](https://github.com/apache/airflow/commit/3bb60afc7b8319996385d681faac342afe2b3bd2) | 2020-03-13  | [AIRFLOW-6975] Base AWSHook AssumeRoleWithSAML (#7619)                                                                                                             |
| [c0c5f11ad](https://github.com/apache/airflow/commit/c0c5f11ad11a5a38e0553c1a36aa75eb83efae51) | 2020-03-12  | [AIRFLOW-6884] Make SageMakerTrainingOperator idempotent (#7598)                                                                                                   |
| [b7cdda1c6](https://github.com/apache/airflow/commit/b7cdda1c64595bc7f85519337029de259e573fce) | 2020-03-10  | [AIRFLOW-4438] Add Gzip compression to S3_hook (#7680)                                                                                                             |
| [42eef3821](https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e) | 2020-03-07  | [AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)                                                                                                   |
| [9a94ab246](https://github.com/apache/airflow/commit/9a94ab246db8c09aa83bb6a6d245b1ca9563bcd9) | 2020-03-01  | [AIRFLOW-6962] Fix compeleted to completed (#7600)                                                                                                                 |
| [1b38f6d9b](https://github.com/apache/airflow/commit/1b38f6d9b6710bd5e25fc16883599f1842ab7cb9) | 2020-02-29  | [AIRFLOW-5908] Add download_file to S3 Hook (#6577)                                                                                                                |
| [3ea3e1a2b](https://github.com/apache/airflow/commit/3ea3e1a2b580b7ed10efe668de0cc37b03673500) | 2020-02-26  | [AIRFLOW-6824] EMRAddStepsOperator problem with multi-step XCom (#7443)                                                                                            |
| [6eaa7e3b1](https://github.com/apache/airflow/commit/6eaa7e3b1845644d5ec65a00a997f4029bec9628) | 2020-02-25  | [AIRFLOW-5924] Automatically unify bucket name and key in S3Hook (#6574)                                                                                           |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)                                                                                   |
| [7d0e7122d](https://github.com/apache/airflow/commit/7d0e7122dd14576d834c6f66fe919a72b100b7f8) | 2020-02-24  | [AIRFLOW-6830] Add Subject/MessageAttributes to SNS hook and operator (#7451)                                                                                      |
| [4d03e33c1](https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc) | 2020-02-22  | [AIRFLOW-6817] remove imports from `airflow/__init__.py`, replaced implicit imports with explicit imports, added entry to `UPDATING.MD` - squashed/rebased (#7456) |
| [47a922b86](https://github.com/apache/airflow/commit/47a922b86426968bfa07cc7892d2eeeca761d884) | 2020-02-21  | [AIRFLOW-6854] Fix missing typing_extensions on python 3.8 (#7474)                                                                                                 |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)                                                                           |
| [58c3542ed](https://github.com/apache/airflow/commit/58c3542ed25061320ce61dbe0adf451a44c738dd) | 2020-02-12  | [AIRFLOW-5231] Fix S3Hook.delete_objects method (#7375)                                                                                                            |
| [b7aa778b3](https://github.com/apache/airflow/commit/b7aa778b38df2f116a1c20031e72fea8b97315bf) | 2020-02-10  | [AIRFLOW-6767] Correct name for default Athena workgroup (#7394)                                                                                                   |
| [9282185e6](https://github.com/apache/airflow/commit/9282185e6624e64bb7f17447f81c1b2d1bb4d56d) | 2020-02-09  | [AIRFLOW-6761] Fix WorkGroup param in AWSAthenaHook (#7386)                                                                                                        |
| [94fccca97](https://github.com/apache/airflow/commit/94fccca97030ee59d89f302a98137b17e7b01a33) | 2020-02-04  | [AIRFLOW-XXXX] Add pre-commit check for utf-8 file encoding (#7347)                                                                                                |
| [f3ad5cf61](https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a) | 2020-02-03  | [AIRFLOW-4681] Make sensors module pylint compatible (#7309)                                                                                                       |
| [88e40c714](https://github.com/apache/airflow/commit/88e40c714d2853aa8966796945b2907c263fed08) | 2020-02-03  | [AIRFLOW-6716] Fix AWS Datasync Example DAG (#7339)                                                                                                                |
| [a311d3d82](https://github.com/apache/airflow/commit/a311d3d82e0c2e32bcb56e29f33c95ed0a2a2ddc) | 2020-02-03  | [AIRFLOW-6718] Fix more occurrences of utils.dates.days_ago (#7341)                                                                                                |
| [cb766b05b](https://github.com/apache/airflow/commit/cb766b05b17b80fd54a5ce6ac3ee35a631115000) | 2020-02-03  | [AIRFLOW-XXXX] Fix Static Checks on CI (#7342)                                                                                                                     |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                                                                                           |
| [7527eddc5](https://github.com/apache/airflow/commit/7527eddc5e9729aa7e732209a07d57985f6c73e4) | 2020-02-02  | [AIRFLOW-4364] Make all code in airflow/providers/amazon pylint compatible (#7336)                                                                                 |
| [cf141506a](https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721) | 2020-02-02  | [AIRFLOW-6708] Set unique logger names (#7330)                                                                                                                     |
| [63aa3db88](https://github.com/apache/airflow/commit/63aa3db88f8824efe79622301efd9f8ba75b991c) | 2020-02-02  | [AIRFLOW-6258] Add CloudFormation operators to AWS providers (#6824)                                                                                               |
| [af4157fde](https://github.com/apache/airflow/commit/af4157fdeffc0c18492b518708c0db44815067ab) | 2020-02-02  | [AIRFLOW-6672] AWS DataSync - better logging of error message (#7288)                                                                                              |
| [373c6aa4a](https://github.com/apache/airflow/commit/373c6aa4a208284b5ff72987e4bd8f4e2ada1a1b) | 2020-01-30  | [AIRFLOW-6682] Move GCP classes to providers package (#7295)                                                                                                       |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                                                                                                 |
| [1988a97e8](https://github.com/apache/airflow/commit/1988a97e8f687e28a5a39b29677fb514e097753c) | 2020-01-28  | [AIRFLOW-6659] Move AWS Transfer operators to providers package (#7274)                                                                                            |
| [ab10443e9](https://github.com/apache/airflow/commit/ab10443e965269efe9c1efaf5fa33bcdbe609f13) | 2020-01-28  | [AIRFLOW-6424] Added a operator to modify EMR cluster (#7213)                                                                                                      |
| [40246132a](https://github.com/apache/airflow/commit/40246132a7ef3b07fe3173c6e7646ed6b53aad6e) | 2020-01-28  | [AIRFLOW-6654] AWS DataSync - bugfix when creating locations (#7270)                                                                                               |
| [82c0e5aff](https://github.com/apache/airflow/commit/82c0e5aff6004f636b98e207c3caec40b403fbbe) | 2020-01-28  | [AIRFLOW-6655] Move AWS classes to providers (#7271)                                                                                                               |
| [599e4791c](https://github.com/apache/airflow/commit/599e4791c91cff411b1bf1c45555db5094c2b420) | 2020-01-18  | [AIRFLOW-6541] Use EmrJobFlowSensor for other states (#7146)                                                                                                       |
| [c319e81ca](https://github.com/apache/airflow/commit/c319e81cae1de31ad1373903252d8608ffce1fba) | 2020-01-17  | [AIRFLOW-6572] Move AWS classes to providers.amazon.aws package (#7178)                                                                                            |
| [941a07057](https://github.com/apache/airflow/commit/941a070578bc7d9410715b89658548167352cc4d) | 2020-01-15  | [AIRFLOW-6570] Add dag tag for all example dag (#7176)                                                                                                             |
| [78d8fe694](https://github.com/apache/airflow/commit/78d8fe6944b689b9b0af99255286e34e06eedec3) | 2020-01-08  | [AIRFLOW-6245] Add custom waiters for AWS batch jobs (#6811)                                                                                                       |
| [e0b022725](https://github.com/apache/airflow/commit/e0b022725749181bd4e30933e4a0ffefb993eede) | 2019-12-28  | [AIRFLOW-6319] Add support for AWS Athena workgroups (#6871)                                                                                                       |
| [57da45685](https://github.com/apache/airflow/commit/57da45685457520d51a0967e2aeb5e5ff162dfa7) | 2019-12-24  | [AIRFLOW-6333] Bump Pylint to 2.4.4 &amp; fix/disable new checks (#6888)                                                                                               |
| [cf647c27e](https://github.com/apache/airflow/commit/cf647c27e0f35bbd1183bfcf87a106cbdb69d3fa) | 2019-12-18  | [AIRFLOW-6038] AWS DataSync reworked (#6773)                                                                                                                       |
| [7502cad28](https://github.com/apache/airflow/commit/7502cad2844139d57e4276d971c0706a361d9dbe) | 2019-12-17  | [AIRFLOW-6206] Move and rename AWS batch operator [AIP-21] (#6764)                                                                                                 |
| [c4c635df6](https://github.com/apache/airflow/commit/c4c635df6906f56e01724573923e19763bb0da62) | 2019-12-17  | [AIRFLOW-6083] Adding ability to pass custom configuration to lambda client. (#6678)                                                                               |
| [4fb498f87](https://github.com/apache/airflow/commit/4fb498f87ef89acc30f2576ebc5090ab0653159e) | 2019-12-09  | [AIRFLOW-6072] aws_hook: Outbound http proxy setting and other enhancements (#6686)                                                                                |
| [a1e2f8635](https://github.com/apache/airflow/commit/a1e2f863526973b17892ec31caf09eded95c1cd2) | 2019-11-20  | [AIRFLOW-6021] Replace list literal with list constructor (#6617)                                                                                                  |
| [baae14084](https://github.com/apache/airflow/commit/baae140847cdf9d84e905fb6d1f119d6950eecf9) | 2019-11-19  | [AIRFLOW-5781] AIP-21 Migrate AWS Kinesis to /providers/amazon/aws (#6588)                                                                                         |
| [504cfbac1](https://github.com/apache/airflow/commit/504cfbac1a4ec2e2fd169523ed357808f63881bb) | 2019-11-18  | [AIRFLOW-5783] AIP-21 Move aws redshift into providers structure (#6539)                                                                                           |
| [992f0e3ac](https://github.com/apache/airflow/commit/992f0e3acf11163294508858515a5f79116e3ad8) | 2019-11-12  | AIRFLOW-5824: AWS DataSync Hook and Operators added (#6512)                                                                                                        |
| [c015eb2f6](https://github.com/apache/airflow/commit/c015eb2f6496b9721afda9e85d5d4af3bbe0696b) | 2019-11-10  | [AIRFLOW-5786] Migrate AWS SNS to /providers/amazon/aws (#6502)                                                                                                    |
| [3d76fb4bf](https://github.com/apache/airflow/commit/3d76fb4bf25e5b7d3d30e0d64867b5999b77f0b0) | 2019-11-09  | [AIRFLOW-5782] Migrate AWS Lambda to /providers/amazon/aws [AIP-21] (#6518)                                                                                        |
