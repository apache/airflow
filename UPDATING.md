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
# Updating Airflow

This file documents any backwards-incompatible changes in Airflow and
assists users migrating to a new version.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Airflow Master](#airflow-master)
- [Airflow 1.10.9](#airflow-1109)
- [Airflow 1.10.8](#airflow-1108)
- [Airflow 1.10.7](#airflow-1107)
- [Airflow 1.10.6](#airflow-1106)
- [Airflow 1.10.5](#airflow-1105)
- [Airflow 1.10.4](#airflow-1104)
- [Airflow 1.10.3](#airflow-1103)
- [Airflow 1.10.2](#airflow-1102)
- [Airflow 1.10.1](#airflow-1101)
- [Airflow 1.10](#airflow-110)
- [Airflow 1.9](#airflow-19)
- [Airflow 1.8.1](#airflow-181)
- [Airflow 1.8](#airflow-18)
- [Airflow 1.7.1.2](#airflow-1712)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Airflow Master

<!--

I'm glad you want to write a new note. Remember that this note is intended for users.
Make sure it contains the following information:

- [ ] Previous behaviors
- [ ] New behaviors
- [ ] If possible, a simple example of how to migrate. This may include a simple code example.
- [ ] If possible, the benefit for the user after migration e.g. "we want to make these changes to unify class names."
- [ ] If possible, the reason for the change, which adds more context to that interested, e.g. reference for Airflow Improvement Proposal.

More tips can be found in the guide:
https://developers.google.com/style/inclusive-documentation

-->

### Removed sub-package imports from `airflow/__init__.py`

The imports `LoggingMixin`, `conf`, `AirflowException`, and `DAG` have been removed from `airflow/__init__.py`.
All implicit references of these objects will no longer be valid. To migrate, all usages of each old path must be
replaced with its corresponding new path.

| Old Path (Implicit Import)   | New Path (Explicit Import)                       |
|------------------------------|--------------------------------------------------|
| ``airflow.LoggingMixin``     | ``airflow.utils.log.logging_mixin.LoggingMixin`` |
| ``airflow.conf``             | ``airflow.configuration.conf``                   |
| ``airflow.AirflowException`` | ``airflow.exceptions.AirflowException``          |
| ``airflow.DAG``              | ``airflow.models.dag.DAG``                  |

### Success Callback will be called when a task in marked as success from UI

When a task is marked as success by a used from Airflow UI - on_success_callback will be called

### Added `airflow dags test` CLI command

A new command was added to the CLI for executing one full run of a DAG for a given execution date, similar to
`airflow tasks test`. Example usage:

```
airflow dags test [dag_id] [execution_date]
airflow dags test example_branch_operator 2018-01-01
```

### Drop plugin support for stat_name_handler

In previous version, you could use plugins mechanism to configure ``stat_name_handler``. You should now use the `stat_name_handler`
option in `[scheduler]` section to achieve the same effect.

If your plugin looked like this and was available through the `test_plugin` path:
```python
def my_stat_name_handler(stat):
    return stat

class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    stat_name_handler = my_stat_name_handler
```
then your `airflow.cfg` file should look like this:
```ini
[scheduler]
stat_name_handler=test_plugin.my_stat_name_handler
```

This change is intended to simplify the statsd configuration.

### Move methods from BiqQueryBaseCursor to BigQueryHook

To simplify BigQuery operators (no need of `Cursor`) and standardize usage of hooks within all GCP integration methods from `BiqQueryBaseCursor`
were moved to `BigQueryHook`. Using them by from `Cursor` object is still possible due to preserved backward compatibility but they will raise `DeprecationWarning`.
The following methods were moved:

| Old path                                                                                       | New path                                                                                 |
|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.cancel_query                  | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.cancel_query                  |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_dataset          | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_dataset          |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_table            | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_table            |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_external_table         | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_external_table         |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.delete_dataset                | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.delete_dataset                |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset                   | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset                   |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset_tables            | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables            |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset_tables_list       | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables_list       |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_datasets_list             | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_datasets_list             |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_schema                    | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_schema                    |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_tabledata                 | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_tabledata                 |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.insert_all                    | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_all                    |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.patch_dataset                 | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_dataset                 |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.patch_table                   | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_table                   |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.poll_job_complete             | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete             |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_copy                      | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_copy                      |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_extract                   | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_extract                   |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_grant_dataset_view_access | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_grant_dataset_view_access |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_load                      | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_load                      |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_query                     | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_query                     |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_delete              | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_delete              |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_upsert              | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_upsert              |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_with_configuration        | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration        |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.update_dataset                | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset                |

### Standardize handling http exception in BigQuery

Since BigQuery is the part of the GCP it was possible to simplify the code by handling the exceptions
by usage of the `airflow.providers.google.cloud.hooks.base.CloudBaseHook.catch_http_exception` decorator however it changes
exceptions raised by the following methods:
* `airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_delete` raises `AirflowException` instead of `Exception`.
* `airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_dataset` raises `AirflowException` instead of `ValueError`.
* `airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset` raises `AirflowException` instead of `ValueError`.

### Remove airflow.utils.file.TemporaryDirectory

Since Airflow dropped support for Python < 3.5 there's no need to have this custom
implementation of `TemporaryDirectory` because the same functionality is provided by
`tempfile.TemporaryDirectory`.

Now users instead of `import from airflow.utils.files import TemporaryDirectory` should
do `from tempfile import TemporaryDirectory`. Both context managers provide the same
interface, thus no additional changes should be required.

### Chain and cross_downstream moved from helpers to BaseOperator

The `chain` and `cross_downstream` methods are now moved to airflow.models.baseoperator module from
`airflow.utils.helpers` module.

The baseoperator module seems to be a better choice to keep
closely coupled methods together. Helpers module is supposed to contain standalone helper methods
that can be imported by all classes.

The `chain` method and `cross_downstream` method both use BaseOperator. If any other package imports
any classes or functions from helpers module, then it automatically has an
implicit dependency to BaseOperator. That can often lead to cyclic dependencies.

More information in [AIFLOW-6392](https://issues.apache.org/jira/browse/AIRFLOW-6392)

In Airflow <2.0 you imported those two methods like this:

```python
from airflow.utils.helpers import chain
from airflow.utils.helpers import cross_downstream
```

In Airflow 2.0 it should be changed to:
```python
from airflow.models.baseoperator import chain
from airflow.models.baseoperator import cross_downstream
```

### Change python3 as Dataflow Hooks/Operators default interpreter

Now the `py_interpreter` argument for DataFlow Hooks/Operators has been changed from python2 to python3.

### Logging configuration has been moved to new section

The following configurations have been moved from `[core]` to the new `[logging]` section.

* `base_log_folder`
* `remote_logging`
* `remote_log_conn_id`
* `remote_base_log_folder`
* `encrypt_s3_logs`
* `logging_level`
* `fab_logging_level`
* `logging_config_class`
* `colored_console_log`
* `colored_log_format`
* `colored_formatter_class`
* `log_format`
* `simple_log_format`
* `task_log_prefix_template`
* `log_filename_template`
* `log_processor_filename_template`
* `dag_processor_manager_log_location`
* `task_log_reader`

### Simplification of CLI commands

Some commands have been grouped to improve UX of CLI. New commands are available according to the following table:

| Old command               | New command                        |
|---------------------------|------------------------------------|
| ``airflow worker``        | ``airflow celery worker``          |
| ``airflow flower``        | ``airflow celery flower``          |

### Remove serve_logs command from CLI

The ``serve_logs`` command has been deleted. This command should be run only by internal application mechanisms
and there is no need for it to be accessible from the CLI interface.

### dag_state CLI command

If the DAGRun was triggered with conf key/values passed in, they will also be printed in the dag_state CLI response
ie. running, {"name": "bob"}
whereas in in prior releases it just printed the state:
ie. running

### Remove gcp_service_account_keys option in airflow.cfg file

This option has been removed because it is no longer supported by the Google Kubernetes Engine. The new
recommended service account keys for the Google Cloud Platform management method is
[Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity).

### BranchPythonOperator has a return value
`BranchPythonOperator` will now return a value equal to the `task_id` of the chosen branch,
where previously it returned None. Since it inherits from BaseOperator it will do an
`xcom_push` of this value if `do_xcom_push=True`. This is useful for downstream decision-making.

### Removal of airflow.AirflowMacroPlugin class

The class was there in airflow package but it has not been used (apparently since 2015).
It has been removed.

### Changes to settings

CONTEXT_MANAGER_DAG was removed from settings. It's role has been taken by `DagContext` in
'airflow.models.dag'. One of the reasons was that settings should be rather static than store
dynamic context from the DAG, but the main one is that moving the context out of settings allowed to
untangle cyclic imports between DAG, BaseOperator, SerializedDAG, SerializedBaseOperator which was
part of AIRFLOW-6010.

#### Change default aws_conn_id in EMR operators

The default value for the [aws_conn_id](https://airflow.apache.org/howto/manage-connections.html#amazon-web-services) was accidently set to 's3_default' instead of 'aws_default' in some of the emr operators in previous
versions. This was leading to EmrStepSensor not being able to find their corresponding emr cluster. With the new
changes in the EmrAddStepsOperator, EmrTerminateJobFlowOperator and EmrCreateJobFlowOperator this issue is
solved.

### Removal of redirect_stdout, redirect_stderr

Function `redirect_stderr` and `redirect_stdout` from `airflow.utils.log.logging_mixin` module has
been deleted because it can be easily replaced by the standard library.
The functions of the standard library are more flexible and can be used in larger cases.

The code below
```python
import logging

from airflow.utils.log.logging_mixin import redirect_stderr, redirect_stdout

logger = logging.getLogger("custom-logger")
with redirect_stdout(logger, logging.INFO), redirect_stderr(logger, logging.WARN):
    print("I love Airflow")
```
can be replaced by the following code:
```python
from contextlib import redirect_stdout, redirect_stderr
import logging

from airflow.utils.log.logging_mixin import StreamLogWriter

logger = logging.getLogger("custom-logger")

with redirect_stdout(StreamLogWriter(logger, logging.INFO)), \
        redirect_stderr(StreamLogWriter(logger, logging.WARN)):
    print("I Love Airflow")
```

### Removal of XCom.get_one()

This one is supersede by `XCom.get_many().first()` which will return the same result.

### Changes to SQLSensor

SQLSensor now consistent with python `bool()` function and the `allow_null` parameter has been removed.

It will resolve after receiving any value  that is casted to `True` with python `bool(value)`. That
changes the previous response receiving `NULL` or `'0'`. Earlier `'0'` has been treated as success
criteria. `NULL` has been treated depending on value of `allow_null`parameter.  But all the previous
behaviour is still achievable setting param `success` to `lambda x: x is None or str(x) not in ('0', '')`.

### Idempotency in BigQuery operators
Idempotency was added to `BigQueryCreateEmptyTableOperator` and `BigQueryCreateEmptyDatasetOperator`.
But to achieve that try / except clause was removed from `create_empty_dataset` and `create_empty_table`
methods of `BigQueryHook`.

### Migration of AWS components

All AWS components (hooks, operators, sensors, example DAGs) will be grouped together as decided in
[AIP-21](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths). Migrated
components remain backwards compatible but raise a `DeprecationWarning` when imported from the old module.
Migrated are:

| Old path                                                     | New path                                                 |
| ------------------------------------------------------------ | -------------------------------------------------------- |
| airflow.hooks.S3_hook.S3Hook                                 | airflow.providers.amazon.aws.hooks.s3.S3Hook                    |
| airflow.contrib.hooks.aws_athena_hook.AWSAthenaHook          | airflow.providers.amazon.aws.hooks.athena.AWSAthenaHook         |
| airflow.contrib.hooks.aws_lambda_hook.AwsLambdaHook          | airflow.providers.amazon.aws.hooks.lambda_function.AwsLambdaHook         |
| airflow.contrib.hooks.aws_sqs_hook.SQSHook                   | airflow.providers.amazon.aws.hooks.sqs.SQSHook        |
| airflow.contrib.hooks.aws_sns_hook.AwsSnsHook                   | airflow.providers.amazon.aws.hooks.sns.AwsSnsHook        |
| airflow.contrib.operators.aws_athena_operator.AWSAthenaOperator | airflow.providers.amazon.aws.operators.athena.AWSAthenaOperator |
| airflow.contrib.operators.awsbatch.AWSBatchOperator | airflow.providers.amazon.aws.operators.batch.AwsBatchOperator |
| airflow.contrib.operators.awsbatch.BatchProtocol | airflow.providers.amazon.aws.hooks.batch_client.AwsBatchProtocol |
| private attrs and methods on AWSBatchOperator | airflow.providers.amazon.aws.hooks.batch_client.AwsBatchClient |
| n/a | airflow.providers.amazon.aws.hooks.batch_waiters.AwsBatchWaiters |
| airflow.contrib.operators.aws_sqs_publish_operator.SQSPublishOperator | airflow.providers.amazon.aws.operators.sqs.SQSPublishOperator |
| airflow.contrib.operators.aws_sns_publish_operator.SnsPublishOperator | airflow.providers.amazon.aws.operators.sns.SnsPublishOperator |
| airflow.contrib.sensors.aws_athena_sensor.AthenaSensor       | airflow.providers.amazon.aws.sensors.athena.AthenaSensor        |
| airflow.contrib.sensors.aws_sqs_sensor.SQSSensor             | airflow.providers.amazon.aws.sensors.sqs.SQSSensor        |

### AWS Batch Operator

The `AwsBatchOperator` was refactored to extract an `AwsBatchClient` (and inherit from it).  The
changes are mostly backwards compatible and clarify the public API for these classes; some
private methods on `AwsBatchOperator` for polling a job status were relocated and renamed
to surface new public methods on `AwsBatchClient` (and via inheritance on `AwsBatchOperator`).  A
couple of job attributes are renamed on an instance of `AwsBatchOperator`; these were mostly
used like private attributes but they were surfaced in the public API, so any use of them needs
to be updated as follows:
- `AwsBatchOperator().jobId` -> `AwsBatchOperator().job_id`
- `AwsBatchOperator().jobName` -> `AwsBatchOperator().job_name`

The `AwsBatchOperator` gets a new option to define a custom model for waiting on job status changes.
The `AwsBatchOperator` can use a new `waiters` parameter, an instance of `AwsBatchWaiters`, to
specify that custom job waiters will be used to monitor a batch job.  See the latest API
documentation for details.

### Additional arguments passed to BaseOperator cause an exception

Previous versions of Airflow took additional arguments and displayed a message on the console. When the
message was not noticed by users, it caused very difficult to detect errors.

In order to restore the previous behavior, you must set an ``True`` in  the ``allow_illegal_arguments``
option of section ``[operators]`` in the ``airflow.cfg`` file. In the future it is possible to completely
delete this option.

### Simplification of the TriggerDagRunOperator

The TriggerDagRunOperator now takes a `conf` argument to which a dict can be provided as conf for the DagRun.
As a result, the `python_callable` argument was removed. PR: https://github.com/apache/airflow/pull/6317.

### Changes in Google Cloud Platform related hooks

The change in GCP operators implies that GCP Hooks for those operators require now keyword parameters rather
than positional ones in all methods where `project_id` is used. The methods throw an explanatory exception
in case they are called using positional parameters.

Hooks involved:

  * DataflowHook
  * MLEngineHook
  * PubSubHook

Other GCP hooks are unaffected.

### Fernet is enabled by default

The fernet mechanism is enabled by default to increase the security of the default installation.  In order to
restore the previous behavior, the user must consciously set an empty key in the ``fernet_key`` option of
section ``[core]`` in the ``airflow.cfg`` file.

At the same time, this means that the `apache-airflow[crypto]` extra-packages are always installed.
However, this requires that your operating system has ``libffi-dev`` installed.

### Changes to Google PubSub Operators, Hook and Sensor
In the `PubSubPublishOperator` and `PubSubHook.publsh` method the data field in a message should be bytestring (utf-8 encoded) rather than base64 encoded string.

Due to the normalization of the parameters within GCP operators and hooks a parameters like `project` or `topic_project`
are deprecated and will be substituted by parameter `project_id`.
In `PubSubHook.create_subscription` hook method in the parameter `subscription_project` is replaced by `subscription_project_id`.
Template fields are updated accordingly and old ones may not work.

It is required now to pass key-word only arguments to `PubSub` hook.

These changes are not backward compatible.

Affected components:
 * airflow.providers.google.cloud.hooks.pubsub.PubSubHook
 * airflow.providers.google.cloud.operators.pubsub.PubSubTopicCreateOperator
 * airflow.providers.google.cloud.operators.pubsub.PubSubSubscriptionCreateOperator
 * airflow.providers.google.cloud.operators.pubsub.PubSubTopicDeleteOperator
 * airflow.providers.google.cloud.operators.pubsub.PubSubSubscriptionDeleteOperator
 * airflow.providers.google.cloud.operators.pubsub.PubSubPublishOperator
 * airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor

### Removed Hipchat integration

Hipchat has reached end of life and is no longer available.

For more information please see
https://community.atlassian.com/t5/Stride-articles/Stride-and-Hipchat-Cloud-have-reached-End-of-Life-updated/ba-p/940248

### The gcp_conn_id parameter in GKEPodOperator is required

In previous versions, it was possible to pass the `None` value to the `gcp_conn_id` in the GKEPodOperator
operator, which resulted in credentials being determined according to the
[Application Default Credentials](https://cloud.google.com/docs/authentication/production) strategy.

Now this parameter requires a value. To restore the previous behavior, configure the connection without
specifying the service account.

Detailed information about connection management is available:
[Google Cloud Platform Connection](https://airflow.apache.org/howto/connection/gcp.html).

### Normalize gcp_conn_id for Google Cloud Platform

Previously not all hooks and operators related to Google Cloud Platform use
`gcp_conn_id` as parameter for GCP connection. There is currently one parameter
which apply to most services. Parameters like ``datastore_conn_id``, ``bigquery_conn_id``,
``google_cloud_storage_conn_id`` and similar have been deprecated. Operators that require two connections are not changed.

Following components were affected by normalization:
  * airflow.providers.google.cloud.hooks.datastore.DatastoreHook
  * airflow.providers.google.cloud.hooks.bigquery.BigQueryHook
  * airflow.providers.google.cloud.hooks.gcs.GoogleCloudStorageHook
  * airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryTableDeleteOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageCreateBucketOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageListOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageDownloadOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageDeleteOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageBucketCreateAclEntryOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageObjectCreateAclEntryOperator
  * airflow.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator
  * airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator
  * airflow.operators.gcs_to_s3.GoogleCloudStorageToS3Operator
  * airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator
  * airflow.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator
  * airflow.operators.local_to_gcs.FileToGoogleCloudStorageOperator
  * airflow.operators.cassandra_to_gcs.CassandraToGoogleCloudStorageOperator
  * airflow.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator

### Changes to propagating Kubernetes worker annotations

`kubernetes_annotations` configuration section has been removed.
A new key `worker_annotations` has been added to existing `kubernetes` section instead.
That is to remove restriction on the character set for k8s annotation keys.
All key/value pairs from `kubernetes_annotations` should now go to `worker_annotations` as a json. I.e. instead of e.g.
```
[kubernetes_annotations]
annotation_key = annotation_value
annotation_key2 = annotation_value2
```
it should be rewritten to
```
[kubernetes]
worker_annotations = { "annotation_key" : "annotation_value", "annotation_key2" : "annotation_value2" }
```

### Changes to import paths and names of GCP operators and hooks

According to [AIP-21](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths)
operators related to Google Cloud Platform has been moved from contrib to core.
The following table shows changes in import paths.

|                                                     Old path                                                     |                                                 New path                                                                     |
|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
|airflow.contrib.hooks.bigquery_hook.BigQueryHook                                                                  |airflow.providers.google.cloud.hooks.bigquery.BigQueryHook                                                                    |
|airflow.contrib.hooks.datastore_hook.DatastoreHook                                                                |airflow.providers.google.cloud.hooks.datastore.DatastoreHook                                                                  |
|airflow.contrib.hooks.gcp_bigtable_hook.BigtableHook                                                              |airflow.providers.google.cloud.hooks.bigtable.BigtableHook                                                                    |
|airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook                                                         |airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook                                                               |
|airflow.contrib.hooks.gcp_container_hook.GKEClusterHook                                                           |airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook                                                                |
|airflow.contrib.hooks.gcp_compute_hook.GceHook                                                                    |airflow.providers.google.cloud.hooks.compute.ComputeEngineHook                                                                |
|airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook                                                              |airflow.providers.google.cloud.hooks.dataflow.DataflowHook                                                                    |
|airflow.contrib.hooks.gcp_dataproc_hook.DataProcHook                                                              |airflow.providers.google.cloud.hooks.dataproc.DataprocHook                                                                    |
|airflow.contrib.hooks.gcp_dlp_hook.CloudDLPHook                                                                   |airflow.providers.google.cloud.hooks.dlp.CloudDLPHook                                                                         |
|airflow.contrib.hooks.gcp_function_hook.GcfHook                                                                   |airflow.providers.google.cloud.hooks.functions.CloudFunctionsHook                                                             |
|airflow.contrib.hooks.gcp_kms_hook.GoogleCloudKMSHook                                                             |airflow.providers.google.cloud.hooks.kms.CloudKMSHook                                                                         |
|airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook                                                              |airflow.providers.google.cloud.hooks.mlengine.MLEngineHook                                                                    |
|airflow.contrib.hooks.gcp_natural_language_hook.CloudNaturalLanguageHook                                          |airflow.providers.google.cloud.hooks.natural_language.CloudNaturalLanguageHook                                                |
|airflow.contrib.hooks.gcp_pubsub_hook.PubSubHook                                                                  |airflow.providers.google.cloud.hooks.pubsub.PubSubHook                                                                        |
|airflow.contrib.hooks.gcp_speech_to_text_hook.GCPSpeechToTextHook                                                 |airflow.providers.google.cloud.hooks.speech_to_text.CloudSpeechToTextHook                                                     |
|airflow.contrib.hooks.gcp_spanner_hook.CloudSpannerHook                                                           |airflow.providers.google.cloud.hooks.spanner.SpannerHook                                                                      |
|airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook                                                           |airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook                                                           |
|airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook                                                                   |airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook                                                                   |
|airflow.contrib.hooks.gcp_tasks_hook.CloudTasksHook                                                               |airflow.providers.google.cloud.hooks.tasks.CloudTasksHook                                                                     |
|airflow.contrib.hooks.gcp_text_to_speech_hook.GCPTextToSpeechHook                                                 |airflow.providers.google.cloud.hooks.text_to_speech.CloudTextToSpeechHook                                                     |
|airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook                                                    |airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.CloudDataTransferServiceHook                              |
|airflow.contrib.hooks.gcp_translate_hook.CloudTranslateHook                                                       |airflow.providers.google.cloud.hooks.translate.CloudTranslateHook                                                             |
|airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook                                      |airflow.providers.google.cloud.hooks.video_intelligence.CloudVideoIntelligenceHook                                            |
|airflow.contrib.hooks.gcp_vision_hook.CloudVisionHook                                                             |airflow.providers.google.cloud.hooks.vision.CloudVisionHook                                                                   |
|airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook                                                             |airflow.providers.google.cloud.hooks.gcs.GCSHook                                                                              |
|airflow.contrib.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator                                            |airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator                                                                |
|airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator                                           |airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator                                                       |
|airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator                                   |airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator                                               |
|airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator                                      |airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator                                                  |
|airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator                                               |airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator                                                     |
|airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyDatasetOperator                                    |airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator                                          |
|airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyTableOperator                                      |airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator                                            |
|airflow.contrib.operators.bigquery_operator.BigQueryCreateExternalTableOperator                                   |airflow.providers.google.cloud.operators.bigquery.BigQueryCreateExternalTableOperator                                         |
|airflow.contrib.operators.bigquery_operator.BigQueryDeleteDatasetOperator                                         |airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator                                               |
|airflow.contrib.operators.bigquery_operator.BigQueryOperator                                                      |airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator                                                |
|airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator                              |airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator                                                 |
|airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator                                         |airflow.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator                                                             |
|airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator                                          |airflow.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator                                                              |
|airflow.contrib.operators.bigquery_to_mysql_operator.BigQueryToMySqlOperator                                      |airflow.operators.bigquery_to_mysql.BigQueryToMySqlOperator                                                                   |
|airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator                                                  |airflow.providers.google.cloud.operators.dataflow.DataFlowJavaOperator                                                        |
|airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator                                                |airflow.providers.google.cloud.operators.dataflow.DataFlowPythonOperator                                                      |
|airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator                                              |airflow.providers.google.cloud.operators.dataflow.DataflowTemplateOperator                                                    |
|airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator                                                |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator                                             |
|airflow.contrib.operators.dataproc_operator.DataProcHiveOperator                                                  |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator                                               |
|airflow.contrib.operators.dataproc_operator.DataProcJobBaseOperator                                               |airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator                                                     |
|airflow.contrib.operators.dataproc_operator.DataProcPigOperator                                                   |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator                                                |
|airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator                                               |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator                                            |
|airflow.contrib.operators.dataproc_operator.DataProcSparkOperator                                                 |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator                                              |
|airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator                                              |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator                                           |
|airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator                                         |airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator                                               |
|airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator                                         |airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator                                               |
|airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator                                          |airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator                                                |
|airflow.contrib.operators.dataproc_operator.DataprocOperationBaseOperator                                         |airflow.providers.google.cloud.operators.dataproc.DataprocOperationBaseOperator                                               |
|airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateInlineOperator                     |airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateInlineWorkflowTemplateOperator                           |
|airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateOperator                           |airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateWorkflowTemplateOperator                                 |
|airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator                                       |airflow.providers.google.cloud.operators.datastore.DatastoreExportOperator                                                    |
|airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator                                       |airflow.providers.google.cloud.operators.datastore.DatastoreImportOperator                                                    |
|airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator                                            |airflow.providers.google.cloud.operators.local_to_gcs.FileToGoogleCloudStorageOperator                                        |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableClusterUpdateOperator                                     |airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator                                               |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceCreateOperator                                    |airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator                                              |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceDeleteOperator                                    |airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator                                              |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableTableCreateOperator                                       |airflow.providers.google.cloud.operators.bigtable.BigtableCreateTableOperator                                                 |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableTableDeleteOperator                                       |airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator                                                 |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableTableWaitForReplicationSensor                             |airflow.providers.google.cloud.sensors.bigtable.BigtableTableReplicationCompletedSensor                                       |
|airflow.contrib.operators.gcp_cloud_build_operator.CloudBuildCreateBuildOperator                                  |airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateOperator                                                 |
|airflow.contrib.operators.gcp_compute_operator.GceBaseOperator                                                    |airflow.providers.google.cloud.operators.compute.GceBaseOperator                                                              |
|airflow.contrib.operators.gcp_compute_operator.GceInstanceGroupManagerUpdateTemplateOperator                      |airflow.providers.google.cloud.operators.compute.GceInstanceGroupManagerUpdateTemplateOperator                                |
|airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator                                           |airflow.providers.google.cloud.operators.compute.GceInstanceStartOperator                                                     |
|airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator                                            |airflow.providers.google.cloud.operators.compute.GceInstanceStopOperator                                                      |
|airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator                                    |airflow.providers.google.cloud.operators.compute.GceInstanceTemplateCopyOperator                                              |
|airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator                                          |airflow.providers.google.cloud.operators.compute.GceSetMachineTypeOperator                                                    |
|airflow.contrib.operators.gcp_container_operator.GKEClusterCreateOperator                                         |airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator                                           |
|airflow.contrib.operators.gcp_container_operator.GKEClusterDeleteOperator                                         |airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator                                           |
|airflow.contrib.operators.gcp_container_operator.GKEPodOperator                                                   |airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator                                                |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCancelDLPJobOperator                                           |airflow.providers.google.cloud.operators.dlp.CloudDLPCancelDLPJobOperator                                                     |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDLPJobOperator                                           |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDLPJobOperator                                                     |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDeidentifyTemplateOperator                               |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDeidentifyTemplateOperator                                         |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateInspectTemplateOperator                                  |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateInspectTemplateOperator                                            |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateJobTriggerOperator                                       |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateJobTriggerOperator                                                 |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateStoredInfoTypeOperator                                   |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateStoredInfoTypeOperator                                             |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeidentifyContentOperator                                      |airflow.providers.google.cloud.operators.dlp.CloudDLPDeidentifyContentOperator                                                |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDeidentifyTemplateOperator                               |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator                                         |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDlpJobOperator                                           |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator                                                     |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteInspectTemplateOperator                                  |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteInspectTemplateOperator                                            |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteJobTriggerOperator                                       |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteJobTriggerOperator                                                 |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteStoredInfoTypeOperator                                   |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator                                             |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDeidentifyTemplateOperator                                  |airflow.providers.google.cloud.operators.dlp.CloudDLPGetDeidentifyTemplateOperator                                            |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDlpJobOperator                                              |airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator                                                        |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetInspectTemplateOperator                                     |airflow.providers.google.cloud.operators.dlp.CloudDLPGetInspectTemplateOperator                                               |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetJobTripperOperator                                          |airflow.providers.google.cloud.operators.dlp.CloudDLPGetJobTriggerOperator                                                    |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetStoredInfoTypeOperator                                      |airflow.providers.google.cloud.operators.dlp.CloudDLPGetStoredInfoTypeOperator                                                |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPInspectContentOperator                                         |airflow.providers.google.cloud.operators.dlp.CloudDLPInspectContentOperator                                                   |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDeidentifyTemplatesOperator                                |airflow.providers.google.cloud.operators.dlp.CloudDLPListDeidentifyTemplatesOperator                                          |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDlpJobsOperator                                            |airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator                                                      |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInfoTypesOperator                                          |airflow.providers.google.cloud.operators.dlp.CloudDLPListInfoTypesOperator                                                    |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInspectTemplatesOperator                                   |airflow.providers.google.cloud.operators.dlp.CloudDLPListInspectTemplatesOperator                                             |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListJobTriggersOperator                                        |airflow.providers.google.cloud.operators.dlp.CloudDLPListJobTriggersOperator                                                  |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListStoredInfoTypesOperator                                    |airflow.providers.google.cloud.operators.dlp.CloudDLPListStoredInfoTypesOperator                                              |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPRedactImageOperator                                            |airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator                                                      |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPReidentifyContentOperator                                      |airflow.providers.google.cloud.operators.dlp.CloudDLPReidentifyContentOperator                                                |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateDeidentifyTemplateOperator                               |airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateDeidentifyTemplateOperator                                         |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateInspectTemplateOperator                                  |airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateInspectTemplateOperator                                            |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateJobTriggerOperator                                       |airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateJobTriggerOperator                                                 |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateStoredInfoTypeOperator                                   |airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateStoredInfoTypeOperator                                             |
|airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator                                         |airflow.providers.google.cloud.operators.functions.GcfFunctionDeleteOperator                                                  |
|airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator                                         |airflow.providers.google.cloud.operators.functions.GcfFunctionDeployOperator                                                  |
|airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeEntitiesOperator               |airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator                         |
|airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeEntitySentimentOperator        |airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitySentimentOperator                  |
|airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeSentimentOperator              |airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeSentimentOperator                        |
|airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageClassifyTextOperator                  |airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageClassifyTextOperator                            |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator                         |airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator                                        |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator                         |airflow.providers.google.cloud.operators.spanner.SpannerDeployDatabaseInstanceOperator                                        |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator                          |airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator                                         |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator                         |airflow.providers.google.cloud.operators.spanner.SpannerUpdateDatabaseInstanceOperator                                        |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator                                 |airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator                                                |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeployOperator                                 |airflow.providers.google.cloud.operators.spanner.SpannerDeployInstanceOperator                                                |
|airflow.contrib.operators.gcp_speech_to_text_operator.GcpSpeechToTextRecognizeSpeechOperator                      |airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextRecognizeSpeechOperator                              |
|airflow.contrib.operators.gcp_text_to_speech_operator.GcpTextToSpeechSynthesizeOperator                           |airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechSynthesizeOperator                                   |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobCreateOperator                               |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator             |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobDeleteOperator                               |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceDeleteJobOperator             |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobUpdateOperator                               |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceUpdateJobOperator             |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationCancelOperator                         |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCancelOperationOperator       |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationGetOperator                            |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGetOperationOperator          |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationPauseOperator                          |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServicePauseOperationOperator        |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationResumeOperator                         |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceResumeOperationOperator       |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationsListOperator                          |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceListOperationsOperator        |
|airflow.contrib.operators.gcp_transfer_operator.GoogleCloudStorageToGoogleCloudStorageTransferOperator            |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGCSToGCSOperator              |
|airflow.contrib.operators.gcp_translate_operator.CloudTranslateTextOperator                                       |airflow.providers.google.cloud.operators.translate.CloudTranslateTextOperator                                                 |
|airflow.contrib.operators.gcp_translate_speech_operator.GcpTranslateSpeechOperator                                |airflow.providers.google.cloud.operators.translate_speech.GcpTranslateSpeechOperator                                          |
|airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoExplicitContentOperator|airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoExplicitContentOperator          |
|airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoLabelsOperator         |airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoLabelsOperator                   |
|airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoShotsOperator          |airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoShotsOperator                    |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionAddProductToProductSetOperator                           |airflow.providers.google.cloud.operators.vision.CloudVisionAddProductToProductSetOperator                                     |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionAnnotateImageOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectDocumentTextOperator                               |airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator                                                 |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageLabelsOperator                                |airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageLabelsOperator                                          |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageSafeSearchOperator                            |airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageSafeSearchOperator                                      |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectTextOperator                                       |airflow.providers.google.cloud.operators.vision.CloudVisionDetectTextOperator                                                 |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductCreateOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductDeleteOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductGetOperator                                       |airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator                                                 |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetCreateOperator                                 |airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator                                           |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetDeleteOperator                                 |airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator                                           |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetGetOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetUpdateOperator                                 |airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator                                           |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductUpdateOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionReferenceImageCreateOperator                             |airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator                                       |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionRemoveProductFromProductSetOperator                      |airflow.providers.google.cloud.operators.vision.CloudVisionRemoveProductFromProductSetOperator                                |
|airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator                         |airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator                                                  |
|airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator                         |airflow.providers.google.cloud.operators.gcs.GCSObjectCreateAclEntryOperator                                                  |
|airflow.contrib.operators.gcs_delete_operator.GoogleCloudStorageDeleteOperator                                    |airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator                                                         |
|airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator                                |airflow.providers.google.cloud.operators.gcs.GCSToLocalOperator                                                               |
|airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageListOperator                                        |airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator                                                           |
|airflow.contrib.operators.gcs_operator.GoogleCloudStorageCreateBucketOperator                                     |airflow.providers.google.cloud.operators.gcs.GCSCreateBucketOperator                                                          |
|airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator                                          |airflow.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator                                                              |
|airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator                               |airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator                                                   |
|airflow.contrib.operators.gcs_to_s3.GoogleCloudStorageToS3Operator                                                |airflow.operators.gcs_to_s3.GCSToS3Operator                                                                                   |
|airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator                                       |airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator                                     |
|airflow.contrib.operators.mlengine_operator.MLEngineModelOperator                                                 |airflow.providers.google.cloud.operators.mlengine.MLEngineManageModelOperator                                                 |
|airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator                                              |airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator                                            |
|airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator                                               |airflow.providers.google.cloud.operators.mlengine.MLEngineManageVersionOperator                                               |
|airflow.contrib.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator                                          |airflow.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator                                                              |
|airflow.contrib.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator                                          |airflow.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator                                                              |
|airflow.contrib.operators.postgres_to_gcs_operator.PostgresToGoogleCloudStorageOperator                           |airflow.operators.postgres_to_gcs.PostgresToGoogleCloudStorageOperator                                                        |
|airflow.contrib.operators.pubsub_operator.PubSubPublishOperator                                                   |airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator                                                  |
|airflow.contrib.operators.pubsub_operator.PubSubSubscriptionCreateOperator                                        |airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator                                              |
|airflow.contrib.operators.pubsub_operator.PubSubSubscriptionDeleteOperator                                        |airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator                                              |
|airflow.contrib.operators.pubsub_operator.PubSubTopicCreateOperator                                               |airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator                                                     |
|airflow.contrib.operators.pubsub_operator.PubSubTopicDeleteOperator                                               |airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator                                                     |
|airflow.contrib.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator                                          |airflow.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator                                                              |
|airflow.contrib.sensors.bigquery_sensor.BigQueryTableSensor                                                       |airflow.providers.google.cloud.sensors.bigquery.BigQueryTableExistenceSensor                                                  |
|airflow.contrib.sensors.gcp_transfer_sensor.GCPTransferServiceWaitForJobStatusSensor                              |airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.DataTransferServiceJobStatusSensor                      |
|airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectSensor                                                 |airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor                                                           |
|airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectUpdatedSensor                                          |airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor                                                              |
|airflow.contrib.sensors.gcs_sensor.GoogleCloudStoragePrefixSensor                                                 |airflow.providers.google.cloud.sensors.gcs.GCSObjectsWtihPrefixExistenceSensor                                                |
|airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageUploadSessionCompleteSensor                                  |airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor                                                     |
|airflow.contrib.sensors.pubsub_sensor.PubSubPullSensor                                                            |airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor                                                                |


### Remove provide_context

`provide_context` argument on the PythonOperator was removed. The signature of the callable passed to the PythonOperator is now inferred and argument values are always automatically provided. There is no need to explicitly provide or not provide the context anymore. For example:

```python
def myfunc(execution_date):
    print(execution_date)

python_operator = PythonOperator(task_id='mytask', python_callable=myfunc, dag=dag)
```

Notice you don't have to set provide_context=True, variables from the task context are now automatically detected and provided.

All context variables can still be provided with a double-asterisk argument:

```python
def myfunc(**context):
    print(context)  # all variables will be provided to context

python_operator = PythonOperator(task_id='mytask', python_callable=myfunc)
```

The task context variable names are reserved names in the callable function, hence a clash with `op_args` and `op_kwargs` results in an exception:

```python
def myfunc(dag):
    # raises a ValueError because "dag" is a reserved name
    # valid signature example: myfunc(mydag)

python_operator = PythonOperator(
    task_id='mytask',
    op_args=[1],
    python_callable=myfunc,
)
```

The change is backwards compatible, setting `provide_context` will add the `provide_context` variable to the `kwargs` (but won't do anything).

PR: [#5990](https://github.com/apache/airflow/pull/5990)

### Changes to FileSensor

FileSensor is now takes a glob pattern, not just a filename. If the filename you are looking for has `*`, `?`, or `[` in it then you should replace these with `[*]`, `[?]`, and `[[]`.

### Change dag loading duration metric name
Change DAG file loading duration metric from
`dag.loading-duration.<dag_id>` to `dag.loading-duration.<dag_file>`. This is to
better handle the case when a DAG file has multiple DAGs.

### Changes to ImapHook, ImapAttachmentSensor and ImapAttachmentToS3Operator

ImapHook:
* The order of arguments has changed for `has_mail_attachment`,
`retrieve_mail_attachments` and `download_mail_attachments`.
* A new `mail_filter` argument has been added to each of those.

ImapAttachmentSensor:
* The order of arguments has changed for `__init__`.
* A new `mail_filter` argument has been added to `__init__`.

ImapAttachmentToS3Operator:
* The order of arguments has changed for `__init__`.
* A new `imap_mail_filter` argument has been added to `__init__`.

### Changes to `SubDagOperator`

`SubDagOperator` is changed to use Airflow scheduler instead of backfill
to schedule tasks in the subdag. User no longer need to specify the executor
in `SubDagOperator`.

### Variables removed from the task instance context

The following variables were removed from the task instance context:
- end_date
- latest_date
- tables

### Moved provide_gcp_credential_file decorator to GoogleCloudBaseHook

To simplify the code, the decorator has been moved from the inner-class.

Instead of `@GoogleCloudBaseHook._Decorators.provide_gcp_credential_file`,
you should write `@GoogleCloudBaseHook.provide_gcp_credential_file`

### Changes to S3Hook

Note: The order of arguments has changed for `check_for_prefix`.
The `bucket_name` is now optional. It falls back to the `connection schema` attribute.
The `delete_objects` now returns `None` instead of a response, since the method now makes multiple api requests when the keys list length is > 1000.

### Changes to Google Transfer Operator
To obtain pylint compatibility the `filter ` argument in `GcpTransferServiceOperationsListOperator`
has been renamed to `request_filter`.

### Changes in  Google Cloud Transfer Hook
 To obtain pylint compatibility the `filter` argument in `GCPTransferServiceHook.list_transfer_job` and
 `GCPTransferServiceHook.list_transfer_operations` has been renamed to `request_filter`.

### CLI reorganization

The Airflow CLI has been organized so that related commands are grouped
together as subcommands. The `airflow list_dags` command is now `airflow
dags list`, `airflow pause` is `airflow dags pause`, etc. For a complete
list of updated CLI commands, see https://airflow.apache.org/cli.html.

### Removal of Mesos Executor

The Mesos Executor is removed from the code base as it was not widely used and not maintained. [Mailing List Discussion on deleting it](https://lists.apache.org/thread.html/daa9500026b820c6aaadeffd66166eae558282778091ebbc68819fb7@%3Cdev.airflow.apache.org%3E).

### Increase standard Dataproc disk sizes

It is highly recommended to have 1TB+ disk size for Dataproc to have sufficient throughput:
https://cloud.google.com/compute/docs/disks/performance

Hence, the default value for `master_disk_size` in DataprocCreateClusterOperator has beeen changes from 500GB to 1TB.

### Changes to SalesforceHook

* renamed `sign_in` function to `get_conn`

### HTTPHook verify default value changed from False to True.

The HTTPHook is now secured by default: `verify=True`.
This can be overwriten by using the extra_options param as `{'verify': False}`.

### Changes to GoogleCloudStorageHook

* The following parameters have been replaced in all the methods in GCSHook:
  * `bucket` is changed to `bucket_name`
  * `object` is changed to `object_name`

* The `maxResults` parameter in `GoogleCloudStorageHook.list` has been renamed to `max_results` for consistency.

### Changes to CloudantHook

* upgraded cloudant version from `>=0.5.9,<2.0` to `>=2.0`
* removed the use of the `schema` attribute in the connection
* removed `db` function since the database object can also be retrieved by calling `cloudant_session['database_name']`

For example:
```python
from airflow.contrib.hooks.cloudant_hook import CloudantHook

with CloudantHook().get_conn() as cloudant_session:
    database = cloudant_session['database_name']
```

See the [docs](https://python-cloudant.readthedocs.io/en/latest/) for more information on how to use the new cloudant version.

### Unify default conn_id for Google Cloud Platform

Previously not all hooks and operators related to Google Cloud Platform use
``google_cloud_default`` as a default conn_id. There is currently one default
variant. Values like ``google_cloud_storage_default``, ``bigquery_default``,
``google_cloud_datastore_default`` have been deprecated. The configuration of
existing relevant connections in the database have been preserved. To use those
deprecated GCP conn_id, you need to explicitly pass their conn_id into
operators/hooks. Otherwise, ``google_cloud_default`` will be used as GCP's conn_id
by default.

### Removed deprecated import mechanism

The deprecated import mechanism has been removed so the import of modules becomes more consistent and explicit.

For example: `from airflow.operators import BashOperator`
becomes `from airflow.operators.bash_operator import BashOperator`

### Changes to sensor imports

Sensors are now accessible via `airflow.sensors` and no longer via `airflow.operators.sensors`.

For example: `from airflow.operators.sensors import BaseSensorOperator`
becomes `from airflow.sensors.base_sensor_operator import BaseSensorOperator`

### Renamed "extra" requirements for cloud providers

Subpackages for specific services have been combined into one variant for
each cloud provider. The name of the subpackage for the Google Cloud Platform
has changed to follow style.

If you want to install integration for Microsoft Azure, then instead of
```
pip install 'apache-airflow[azure_blob_storage,azure_data_lake,azure_cosmos,azure_container_instances]'
```
you should execute `pip install 'apache-airflow[azure]'`

If you want to install integration for Amazon Web Services, then instead of
`pip install 'apache-airflow[s3,emr]'`, you should execute `pip install 'apache-airflow[aws]'`

If you want to install integration for Google Cloud Platform, then instead of
`pip install 'apache-airflow[gcp_api]'`, you should execute `pip install 'apache-airflow[gcp]'`.
The old way will work until the release of Airflow 2.1.

### Deprecate legacy UI in favor of FAB RBAC UI

Previously we were using two versions of UI, which were hard to maintain as we need to implement/update the same feature
in both versions. With this change we've removed the older UI in favor of Flask App Builder RBAC UI. No need to set the
RBAC UI explicitly in the configuration now as this is the only default UI.
Please note that that custom auth backends will need re-writing to target new FAB based UI.

As part of this change, a few configuration items in `[webserver]` section are removed and no longer applicable,
including `authenticate`, `filter_by_owner`, `owner_mode`, and `rbac`.

#### Remove run_duration

We should not use the `run_duration` option anymore. This used to be for restarting the scheduler from time to time, but right now the scheduler is getting more stable and therefore using this setting is considered bad and might cause an inconsistent state.

### CLI Changes

The ability to manipulate users from the command line has been changed. 'airflow create_user' and 'airflow delete_user' and 'airflow list_users' has been grouped to a single command `airflow users` with optional flags `--create`, `--list` and `--delete`.

Example Usage:

To create a new user:
```bash
airflow users --create --username jondoe --lastname doe --firstname jon --email jdoe@apache.org --role Viewer --password test
```

To list users:
```bash
airflow users --list
```

To delete a user:
```bash
airflow users --delete --username jondoe
```

To add a user to a role:
```bash
airflow users --add-role --username jondoe --role Public
```

To remove a user from a role:
```bash
airflow users --remove-role --username jondoe --role Public
```

### Unification of `do_xcom_push` flag
The `do_xcom_push` flag (a switch to push the result of an operator to xcom or not) was appearing in different incarnations in different operators. It's function has been unified under a common name (`do_xcom_push`) on `BaseOperator`. This way it is also easy to globally disable pushing results to xcom.

The following operators were affected:

* DatastoreExportOperator (Backwards compatible)
* DatastoreImportOperator (Backwards compatible)
* KubernetesPodOperator (Not backwards compatible)
* SSHOperator (Not backwards compatible)
* WinRMOperator (Not backwards compatible)
* BashOperator (Not backwards compatible)
* DockerOperator (Not backwards compatible)
* SimpleHttpOperator (Not backwards compatible)

See [AIRFLOW-3249](https://jira.apache.org/jira/browse/AIRFLOW-3249) for details

### Changes to Dataproc related Operators
The 'properties' and 'jars' properties for the Dataproc related operators (`DataprocXXXOperator`) have been renamed from
`dataproc_xxxx_properties` and `dataproc_xxx_jars`  to `dataproc_properties`
and `dataproc_jars`respectively.
Arguments for dataproc_properties dataproc_jars

### Changes to skipping behaviour of LatestOnlyOperator

In previous versions, the `LatestOnlyOperator` forcefully skipped all (direct and undirect) downstream tasks on its own. From this version on the operator will **only skip direct downstream** tasks and the scheduler will handle skipping any further downstream dependencies.

No change is needed if only the default trigger rule `all_success` is being used.

If the DAG relies on tasks with other trigger rules (i.e. `all_done`) being skipped by the `LatestOnlyOperator`, adjustments to the DAG need to be made to commodate the change in behaviour, i.e. with additional edges from the `LatestOnlyOperator`.

The goal of this change is to achieve a more consistent and configurale cascading behaviour based on the `BaseBranchOperator` (see [AIRFLOW-2923](https://jira.apache.org/jira/browse/AIRFLOW-2923) and [AIRFLOW-1784](https://jira.apache.org/jira/browse/AIRFLOW-1784)).

## Airflow 1.10.9

No breaking changes.

## Airflow 1.10.8

### Failure callback will be called when task is marked failed
When task is marked failed by user or task fails due to system failures - on failure call back will be called as part of clean up

See [AIRFLOW-5621](https://jira.apache.org/jira/browse/AIRFLOW-5621) for details

## Airflow 1.10.7

### Changes in experimental API execution_date microseconds replacement

The default behavior was to strip the microseconds (and milliseconds, etc) off of all dag runs triggered by
by the experimental REST API.  The default behavior will change when an explicit execution_date is
passed in the request body.  It will also now be possible to have the execution_date generated, but
keep the microseconds by sending `replace_microseconds=false` in the request body.  The default
behavior can be overridden by sending `replace_microseconds=true` along with an explicit execution_date

### Infinite pool size and pool size query optimisation

Pool size can now be set to -1 to indicate infinite size (it also includes
optimisation of pool query which lead to poor task n^2 performance of task
pool queries in MySQL).

### Viewer won't have edit permissions on DAG view.

### Google Cloud Storage Hook

The `GoogleCloudStorageDownloadOperator` can either write to a supplied `filename` or
return the content of a file via xcom through `store_to_xcom_key` - both options are mutually exclusive.

## Airflow 1.10.6

### BaseOperator::render_template function signature changed

Previous versions of the `BaseOperator::render_template` function required an `attr` argument as the first
positional argument, along with `content` and `context`. This function signature was changed in 1.10.6 and
the `attr` argument is no longer required (or accepted).

In order to use this function in subclasses of the `BaseOperator`, the `attr` argument must be removed:
```python
result = self.render_template('myattr', self.myattr, context)  # Pre-1.10.6 call
...
result = self.render_template(self.myattr, context)  # Post-1.10.6 call
```

### Changes to `aws_default` Connection's default region

The region of Airflow's default connection to AWS (`aws_default`) was previously
set to `us-east-1` during installation.

The region now needs to be set manually, either in the connection screens in
Airflow, via the `~/.aws` config files, or via the `AWS_DEFAULT_REGION` environment
variable.

### Some DAG Processing metrics have been renamed

The following metrics are deprecated and won't be emitted in Airflow 2.0:

- `scheduler.dagbag.errors` and `dagbag_import_errors` -- use `dag_processing.import_errors` instead
- `dag_file_processor_timeouts` -- use `dag_processing.processor_timeouts` instead
- `collect_dags` -- use `dag_processing.total_parse_time` instead
- `dag.loading-duration.<basename>` -- use `dag_processing.last_duration.<basename>` instead
- `dag_processing.last_runtime.<basename>` -- use `dag_processing.last_duration.<basename>` instead

## Airflow 1.10.5

No breaking changes.

## Airflow 1.10.4

### Export MySQL timestamps as UTC

`MySqlToGoogleCloudStorageOperator` now exports TIMESTAMP columns as UTC
by default, rather than using the default timezone of the MySQL server.
This is the correct behavior for use with BigQuery, since BigQuery
assumes that TIMESTAMP columns without time zones are in UTC. To
preserve the previous behavior, set `ensure_utc` to `False.`

### Python 2 support is going away

Airflow 1.10 will be the last release series to support Python 2. Airflow 2.0.0 will only support Python 3.5 and up.

If you have a specific task that still requires Python 2 then you can use the PythonVirtualenvOperator for this.

### Changes to DatastoreHook

* removed argument `version` from `get_conn` function and added it to the hook's `__init__` function instead and renamed it to `api_version`
* renamed the `partialKeys` argument of function `allocate_ids` to `partial_keys`

### Changes to GoogleCloudStorageHook

* the discovery-based api (`googleapiclient.discovery`) used in `GoogleCloudStorageHook` is now replaced by the recommended client based api (`google-cloud-storage`). To know the difference between both the libraries, read https://cloud.google.com/apis/docs/client-libraries-explained. PR: [#5054](https://github.com/apache/airflow/pull/5054)
* as a part of this replacement, the `multipart` & `num_retries` parameters for `GoogleCloudStorageHook.upload` method have been deprecated.

  The client library uses multipart upload automatically if the object/blob size is more than 8 MB - [source code](https://github.com/googleapis/google-cloud-python/blob/11c543ce7dd1d804688163bc7895cf592feb445f/storage/google/cloud/storage/blob.py#L989-L997). The client also handles retries automatically

* the `generation` parameter is deprecated in `GoogleCloudStorageHook.delete` and `GoogleCloudStorageHook.insert_object_acl`.

Updating to `google-cloud-storage >= 1.16` changes the signature of the upstream `client.get_bucket()` method from `get_bucket(bucket_name: str)` to `get_bucket(bucket_or_name: Union[str, Bucket])`. This method is not directly exposed by the airflow hook, but any code accessing the connection directly (`GoogleCloudStorageHook().get_conn().get_bucket(...)` or similar) will need to be updated.

### Changes in writing Logs to Elasticsearch

The `elasticsearch_` prefix has been removed from all config items under the `[elasticsearch]` section. For example `elasticsearch_host` is now just `host`.

### Removal of `non_pooled_task_slot_count` and `non_pooled_backfill_task_slot_count`

`non_pooled_task_slot_count` and `non_pooled_backfill_task_slot_count`
are removed in favor of a real pool, e.g. `default_pool`.

By default tasks are running in `default_pool`.
`default_pool` is initialized with 128 slots and user can change the
number of slots through UI/CLI. `default_pool` cannot be removed.

### `pool` config option in Celery section to support different Celery pool implementation

The new `pool` config option allows users to choose different pool
implementation. Default value is "prefork", while choices include "prefork" (default),
"eventlet", "gevent" or "solo". This may help users achieve better concurrency performance
in different scenarios.

For more details about Celery pool implementation, please refer to:
- https://docs.celeryproject.org/en/latest/userguide/workers.html#concurrency
- https://docs.celeryproject.org/en/latest/userguide/concurrency/eventlet.html


### Change to method signature in `BaseOperator` and `DAG` classes

The signature of the `get_task_instances` method in the `BaseOperator` and `DAG` classes has changed. The change does not change the behavior of the method in either case.

#### For `BaseOperator`

Old signature:

```python
def get_task_instances(self, session, start_date=None, end_date=None):
```

New signature:

```python
@provide_session
def get_task_instances(self, start_date=None, end_date=None, session=None):
```

#### For `DAG`

Old signature:

```python
def get_task_instances(
    self, session, start_date=None, end_date=None, state=None):
```

New signature:

```python
@provide_session
def get_task_instances(
    self, start_date=None, end_date=None, state=None, session=None):
```

In either case, it is necessary to rewrite calls to the `get_task_instances` method that currently provide the `session` positional argument. New calls to this method look like:

```python
# if you can rely on @provide_session
dag.get_task_instances()
# if you need to provide the session
dag.get_task_instances(session=your_session)
```

## Airflow 1.10.3

### New `dag_discovery_safe_mode` config option

If `dag_discovery_safe_mode` is enabled, only check files for DAGs if
they contain the strings "airflow" and "DAG". For backwards
compatibility, this option is enabled by default.

### RedisPy dependency updated to v3 series
If you are using the Redis Sensor or Hook you may have to update your code. See
[redis-py porting instructions] to check if your code might be affected (MSET,
MSETNX, ZADD, and ZINCRBY all were, but read the full doc).

[redis-py porting instructions]: https://github.com/andymccurdy/redis-py/tree/3.2.0#upgrading-from-redis-py-2x-to-30

### SLUGIFY_USES_TEXT_UNIDECODE or AIRFLOW_GPL_UNIDECODE no longer required

It is no longer required to set one of the environment variables to avoid
a GPL dependency. Airflow will now always use text-unidecode if unidecode
was not installed before.

### new `sync_parallelism` config option in celery section

The new `sync_parallelism` config option will control how many processes CeleryExecutor will use to
fetch celery task state in parallel. Default value is max(1, number of cores - 1)

### Rename of BashTaskRunner to StandardTaskRunner

BashTaskRunner has been renamed to StandardTaskRunner. It is the default task runner
so you might need to update your config.

`task_runner = StandardTaskRunner`

### Modification to config file discovery

If the `AIRFLOW_CONFIG` environment variable was not set and the
`~/airflow/airflow.cfg` file existed, airflow previously used
`~/airflow/airflow.cfg` instead of `$AIRFLOW_HOME/airflow.cfg`. Now airflow
will discover its config file using the `$AIRFLOW_CONFIG` and `$AIRFLOW_HOME`
environment variables rather than checking for the presence of a file.

### Changes in Google Cloud Platform related operators

Most GCP-related operators have now optional `PROJECT_ID` parameter. In case you do not specify it,
the project id configured in
[GCP Connection](https://airflow.apache.org/howto/manage-connections.html#connection-type-gcp) is used.
There will be an `AirflowException` thrown in case `PROJECT_ID` parameter is not specified and the
connection used has no project id defined. This change should be  backwards compatible as earlier version
of the operators had `PROJECT_ID` mandatory.

Operators involved:

  * GCP Compute Operators
    * GceInstanceStartOperator
    * GceInstanceStopOperator
    * GceSetMachineTypeOperator
  * GCP Function Operators
    * GcfFunctionDeployOperator
  * GCP Cloud SQL Operators
    * CloudSqlInstanceCreateOperator
    * CloudSqlInstancePatchOperator
    * CloudSqlInstanceDeleteOperator
    * CloudSqlInstanceDatabaseCreateOperator
    * CloudSqlInstanceDatabasePatchOperator
    * CloudSqlInstanceDatabaseDeleteOperator

Other GCP operators are unaffected.

### Changes in Google Cloud Platform related hooks

The change in GCP operators implies that GCP Hooks for those operators require now keyword parameters rather
than positional ones in all methods where `project_id` is used. The methods throw an explanatory exception
in case they are called using positional parameters.

Hooks involved:

  * GceHook
  * GcfHook
  * CloudSqlHook

Other GCP hooks are unaffected.

### Changed behaviour of using default value when accessing variables
It's now possible to use `None` as a default value with the `default_var` parameter when getting a variable, e.g.

```python
foo = Variable.get("foo", default_var=None)
if foo is None:
    handle_missing_foo()
```

(Note: there is already `Variable.setdefault()` which me be helpful in some cases.)

This changes the behaviour if you previously explicitly provided `None` as a default value. If your code expects a `KeyError` to be thrown, then don't pass the `default_var` argument.

### Removal of `airflow_home` config setting

There were previously two ways of specifying the Airflow "home" directory
(`~/airflow` by default): the `AIRFLOW_HOME` environment variable, and the
`airflow_home` config setting in the `[core]` section.

If they had two different values different parts of the code base would end up
with different values. The config setting has been deprecated, and you should
remove the value from the config file and set `AIRFLOW_HOME` environment
variable if you need to use a non default value for this.

(Since this setting is used to calculate what config file to load, it is not
possible to keep just the config option)

### Change of two methods signatures in `GCPTransferServiceHook`

The signature of the `create_transfer_job` method in `GCPTransferServiceHook`
class has changed. The change does not change the behavior of the method.

Old signature:
```python
def create_transfer_job(self, description, schedule, transfer_spec, project_id=None):
```
New signature:
```python
def create_transfer_job(self, body):
```

It is necessary to rewrite calls to method. The new call looks like this:
```python
body = {
  'status': 'ENABLED',
  'projectId': project_id,
  'description': description,
  'transferSpec': transfer_spec,
  'schedule': schedule,
}
gct_hook.create_transfer_job(body)
```
The change results from the unification of all hooks and adjust to
[the official recommendations](https://lists.apache.org/thread.html/e8534d82be611ae7bcb21ba371546a4278aad117d5e50361fd8f14fe@%3Cdev.airflow.apache.org%3E)
for the Google Cloud Platform.

The signature of `wait_for_transfer_job` method in `GCPTransferServiceHook` has changed.

Old signature:
```python
def wait_for_transfer_job(self, job):
```
New signature:
```python
def wait_for_transfer_job(self, job, expected_statuses=(GcpTransferOperationStatus.SUCCESS, )):
```

The behavior of `wait_for_transfer_job` has changed:

Old behavior:

`wait_for_transfer_job` would wait for the SUCCESS status in specified jobs operations.

New behavior:

You can now specify an array of expected statuses. `wait_for_transfer_job` now waits for any of them.

The default value of `expected_statuses` is SUCCESS so that change is backwards compatible.

### Moved two classes to different modules

The class `GoogleCloudStorageToGoogleCloudStorageTransferOperator` has been moved from
`airflow.contrib.operators.gcs_to_gcs_transfer_operator` to `airflow.contrib.operators.gcp_transfer_operator`

the class `S3ToGoogleCloudStorageTransferOperator` has been moved from
`airflow.contrib.operators.s3_to_gcs_transfer_operator` to `airflow.contrib.operators.gcp_transfer_operator`

The change was made to keep all the operators related to GCS Transfer Services in one file.

The previous imports will continue to work until Airflow 2.0

### Fixed typo in --driver-class-path in SparkSubmitHook

The `driver_classapth` argument  to SparkSubmit Hook and Operator was
generating `--driver-classpath` on the spark command line, but this isn't a
valid option to spark.

The argument has been renamed to `driver_class_path`  and  the option it
generates has been fixed.

## Airflow 1.10.2

### New `dag_processor_manager_log_location` config option

The DAG parsing manager log now by default will be log into a file, where its location is
controlled by the new `dag_processor_manager_log_location` config option in core section.

### DAG level Access Control for new RBAC UI

Extend and enhance new Airflow RBAC UI to support DAG level ACL. Each dag now has two permissions(one for write, one for read) associated('can_dag_edit', 'can_dag_read').
The admin will create new role, associate the dag permission with the target dag and assign that role to users. That user can only access / view the certain dags on the UI
that he has permissions on. If a new role wants to access all the dags, the admin could associate dag permissions on an artificial view(``all_dags``) with that role.

We also provide a new cli command(``sync_perm``) to allow admin to auto sync permissions.

### Modification to `ts_nodash` macro
`ts_nodash` previously contained TimeZone information along with execution date. For Example: `20150101T000000+0000`. This is not user-friendly for file or folder names which was a popular use case for `ts_nodash`. Hence this behavior has been changed and using `ts_nodash` will no longer contain TimeZone information, restoring the pre-1.10 behavior of this macro. And a new macro `ts_nodash_with_tz` has been added which can be used to get a string with execution date and timezone info without dashes.

Examples:
  * `ts_nodash`: `20150101T000000`
  * `ts_nodash_with_tz`: `20150101T000000+0000`

### Semantics of next_ds/prev_ds changed for manually triggered runs

next_ds/prev_ds now map to execution_date instead of the next/previous schedule-aligned execution date for DAGs triggered in the UI.

### User model changes
This patch changes the `User.superuser` field from a hardcoded boolean to a `Boolean()` database column. `User.superuser` will default to `False`, which means that this privilege will have to be granted manually to any users that may require it.

For example, open a Python shell and
```python
from airflow import models, settings

session = settings.Session()
users = session.query(models.User).all()  # [admin, regular_user]

users[1].superuser  # False

admin = users[0]
admin.superuser = True
session.add(admin)
session.commit()
```

### Custom auth backends interface change

We have updated the version of flask-login we depend upon, and as a result any
custom auth backends might need a small change: `is_active`,
`is_authenticated`, and `is_anonymous` should now be properties. What this means is if
previously you had this in your user class

    def is_active(self):
      return self.active

then you need to change it like this

    @property
    def is_active(self):
      return self.active

### Support autodetected schemas to GoogleCloudStorageToBigQueryOperator

GoogleCloudStorageToBigQueryOperator is now support schema auto-detection is available when you load data into BigQuery. Unfortunately, changes can be required.

If BigQuery tables are created outside of airflow and the schema is not defined in the task, multiple options are available:

define a schema_fields:

    gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
      ...
      schema_fields={...})

or define a schema_object:

    gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
      ...
      schema_object='path/to/schema/object)

or enabled autodetect of schema:

    gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
      ...
      autodetect=True)

## Airflow 1.10.1

### min_file_parsing_loop_time config option temporarily disabled

The scheduler.min_file_parsing_loop_time config option has been temporarily removed due to
some bugs.

### StatsD Metrics

The `scheduler_heartbeat` metric has been changed from a gauge to a counter. Each loop of the scheduler will increment the counter by 1. This provides a higher degree of visibility and allows for better integration with Prometheus using the [StatsD Exporter](https://github.com/prometheus/statsd_exporter). The scheduler's activity status can be determined by graphing and alerting using a rate of change of the counter. If the scheduler goes down, the rate will drop to 0.

### EMRHook now passes all of connection's extra to CreateJobFlow API

EMRHook.create_job_flow has been changed to pass all keys to the create_job_flow API, rather than
just specific known keys for greater flexibility.

However prior to this release the "emr_default" sample connection that was created had invalid
configuration, so creating EMR clusters might fail until your connection is updated. (Ec2KeyName,
Ec2SubnetId, TerminationProtection and KeepJobFlowAliveWhenNoSteps were all top-level keys when they
should be inside the "Instances" dict)

### LDAP Auth Backend now requires TLS

Connecting to an LDAP server over plain text is not supported anymore. The
certificate presented by the LDAP server must be signed by a trusted
certificate, or you must provide the `cacert` option under `[ldap]` in the
config file.

If you want to use LDAP auth backend without TLS then you will have to create a
custom-auth backend based on
https://github.com/apache/airflow/blob/1.10.0/airflow/contrib/auth/backends/ldap_auth.py

## Airflow 1.10

Installation and upgrading requires setting `SLUGIFY_USES_TEXT_UNIDECODE=yes` in your environment or
`AIRFLOW_GPL_UNIDECODE=yes`. In case of the latter a GPL runtime dependency will be installed due to a
dependency (python-nvd3 -> python-slugify -> unidecode).

### Replace DataProcHook.await calls to DataProcHook.wait

The method name was changed to be compatible with the Python 3.7 async/await keywords

### Setting UTF-8 as default mime_charset in email utils

### Add a configuration variable(default_dag_run_display_number) to control numbers of dag run for display

Add a configuration variable(default_dag_run_display_number) under webserver section to control the number of dag runs to show in UI.

### Default executor for SubDagOperator is changed to SequentialExecutor

### New Webserver UI with Role-Based Access Control

The current webserver UI uses the Flask-Admin extension. The new webserver UI uses the [Flask-AppBuilder (FAB)](https://github.com/dpgaspar/Flask-AppBuilder) extension. FAB has built-in authentication support and Role-Based Access Control (RBAC), which provides configurable roles and permissions for individual users.

To turn on this feature, in your airflow.cfg file (under [webserver]), set the configuration variable `rbac = True`, and then run `airflow` command, which will generate the `webserver_config.py` file in your $AIRFLOW_HOME.

#### Setting up Authentication

FAB has built-in authentication support for DB, OAuth, OpenID, LDAP, and REMOTE_USER. The default auth type is `AUTH_DB`.

For any other authentication type (OAuth, OpenID, LDAP, REMOTE_USER), see the [Authentication section of FAB docs](http://flask-appbuilder.readthedocs.io/en/latest/security.html#authentication-methods) for how to configure variables in webserver_config.py file.

Once you modify your config file, run `airflow db init` to generate new tables for RBAC support (these tables will have the prefix `ab_`).

#### Creating an Admin Account

Once configuration settings have been updated and new tables have been generated, create an admin account with `airflow create_user` command.

#### Using your new UI

Run `airflow webserver` to start the new UI. This will bring up a log in page, enter the recently created admin username and password.

There are five roles created for Airflow by default: Admin, User, Op, Viewer, and Public. To configure roles/permissions, go to the `Security` tab and click `List Roles` in the new UI.

#### Breaking changes

- AWS Batch Operator renamed property queue to job_queue to prevent conflict with the internal queue from CeleryExecutor - AIRFLOW-2542
- Users created and stored in the old users table will not be migrated automatically. FAB's built-in authentication support must be reconfigured.
- Airflow dag home page is now `/home` (instead of `/admin`).
- All ModelViews in Flask-AppBuilder follow a different pattern from Flask-Admin. The `/admin` part of the URL path will no longer exist. For example: `/admin/connection` becomes `/connection/list`, `/admin/connection/new` becomes `/connection/add`, `/admin/connection/edit` becomes `/connection/edit`, etc.
- Due to security concerns, the new webserver will no longer support the features in the `Data Profiling` menu of old UI, including `Ad Hoc Query`, `Charts`, and `Known Events`.
- HiveServer2Hook.get_results() always returns a list of tuples, even when a single column is queried, as per Python API 2.
- **UTC is now the default timezone**: Either reconfigure your workflows scheduling in UTC or set `default_timezone` as explained in https://airflow.apache.org/timezone.html#default-time-zone

### airflow.contrib.sensors.hdfs_sensors renamed to airflow.contrib.sensors.hdfs_sensor

We now rename airflow.contrib.sensors.hdfs_sensors to airflow.contrib.sensors.hdfs_sensor for consistency purpose.

### MySQL setting required

We now rely on more strict ANSI SQL settings for MySQL in order to have sane defaults. Make sure
to have specified `explicit_defaults_for_timestamp=1` in your my.cnf under `[mysqld]`

### Celery config

To make the config of Airflow compatible with Celery, some properties have been renamed:

```
celeryd_concurrency -> worker_concurrency
celery_result_backend -> result_backend
celery_ssl_active -> ssl_active
celery_ssl_cert -> ssl_cert
celery_ssl_key -> ssl_key
```

Resulting in the same config parameters as Celery 4, with more transparency.

### GCP Dataflow Operators

Dataflow job labeling is now supported in Dataflow{Java,Python}Operator with a default
"airflow-version" label, please upgrade your google-cloud-dataflow or apache-beam version
to 2.2.0 or greater.

### BigQuery Hooks and Operator

The `bql` parameter passed to `BigQueryOperator` and `BigQueryBaseCursor.run_query` has been deprecated and renamed to `sql` for consistency purposes. Using `bql` will still work (and raise a `DeprecationWarning`), but is no longer
supported and will be removed entirely in Airflow 2.0

### Redshift to S3 Operator

With Airflow 1.9 or lower, Unload operation always included header row. In order to include header row,
we need to turn off parallel unload. It is preferred to perform unload operation using all nodes so that it is
faster for larger tables. So, parameter called `include_header` is added and default is set to False.
Header row will be added only if this parameter is set True and also in that case parallel will be automatically turned off (`PARALLEL OFF`)

### Google cloud connection string

With Airflow 1.9 or lower, there were two connection strings for the Google Cloud operators, both `google_cloud_storage_default` and `google_cloud_default`. This can be confusing and therefore the `google_cloud_storage_default` connection id has been replaced with `google_cloud_default` to make the connection id consistent across Airflow.

### Logging Configuration

With Airflow 1.9 or lower, `FILENAME_TEMPLATE`, `PROCESSOR_FILENAME_TEMPLATE`, `LOG_ID_TEMPLATE`, `END_OF_LOG_MARK` were configured in `airflow_local_settings.py`. These have been moved into the configuration file, and hence if you were using a custom configuration file the following defaults need to be added.

```
[core]
fab_logging_level = WARN
log_filename_template = {{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{{{{ ts }}}}/{{{{ try_number }}}}.log
log_processor_filename_template = {{{{ filename }}}}.log

[elasticsearch]
elasticsearch_log_id_template = {{dag_id}}-{{task_id}}-{{execution_date}}-{{try_number}}
elasticsearch_end_of_log_mark = end_of_log
```

The previous setting of `log_task_reader` is not needed in many cases now when using the default logging config with remote storages. (Previously it needed to be set to `s3.task` or similar. This is not needed with the default config anymore)

#### Change of per-task log path

With the change to Airflow core to be timezone aware the default log path for task instances will now include timezone information. This will by default mean all previous task logs won't be found. You can get the old behaviour back by setting the following config options:

```
[core]
log_filename_template = {{ ti.dag_id }}/{{ ti.task_id }}/{{ execution_date.strftime("%%Y-%%m-%%dT%%H:%%M:%%S") }}/{{ try_number }}.log
```

## Airflow 1.9

### SSH Hook updates, along with new SSH Operator & SFTP Operator

SSH Hook now uses the Paramiko library to create an ssh client connection, instead of the sub-process based ssh command execution previously (<1.9.0), so this is backward incompatible.

- update SSHHook constructor
- use SSHOperator class in place of SSHExecuteOperator which is removed now. Refer to test_ssh_operator.py for usage info.
- SFTPOperator is added to perform secure file transfer from serverA to serverB. Refer to test_sftp_operator.py for usage info.
- No updates are required if you are using ftpHook, it will continue to work as is.

### S3Hook switched to use Boto3

The airflow.hooks.S3_hook.S3Hook has been switched to use boto3 instead of the older boto (a.k.a. boto2). This results in a few backwards incompatible changes to the following classes: S3Hook:

- the constructors no longer accepts `s3_conn_id`. It is now called `aws_conn_id`.
- the default connection is now "aws_default" instead of "s3_default"
- the return type of objects returned by `get_bucket` is now boto3.s3.Bucket
- the return type of `get_key`, and `get_wildcard_key` is now an boto3.S3.Object.

If you are using any of these in your DAGs and specify a connection ID you will need to update the parameter name for the connection to "aws_conn_id": S3ToHiveTransfer, S3PrefixSensor, S3KeySensor, RedshiftToS3Transfer.

### Logging update

The logging structure of Airflow has been rewritten to make configuration easier and the logging system more transparent.

#### A quick recap about logging

A logger is the entry point into the logging system. Each logger is a named bucket to which messages can be written for processing. A logger is configured to have a log level. This log level describes the severity of the messages that the logger will handle. Python defines the following log levels: DEBUG, INFO, WARNING, ERROR or CRITICAL.

Each message that is written to the logger is a Log Record. Each log record contains a log level indicating the severity of that specific message. A log record can also contain useful metadata that describes the event that is being logged. This can include details such as a stack trace or an error code.

When a message is given to the logger, the log level of the message is compared to the log level of the logger. If the log level of the message meets or exceeds the log level of the logger itself, the message will undergo further processing. If it doesn’t, the message will be ignored.

Once a logger has determined that a message needs to be processed, it is passed to a Handler. This configuration is now more flexible and can be easily be maintained in a single file.

#### Changes in Airflow Logging

Airflow's logging mechanism has been refactored to use Python’s built-in `logging` module to perform logging of the application. By extending classes with the existing `LoggingMixin`, all the logging will go through a central logger. Also the `BaseHook` and `BaseOperator` already extend this class, so it is easily available to do logging.

The main benefit is easier configuration of the logging by setting a single centralized python file. Disclaimer; there is still some inline configuration, but this will be removed eventually. The new logging class is defined by setting the dotted classpath in your `~/airflow/airflow.cfg` file:

```
# Logging class
# Specify the class that will specify the logging configuration
# This class has to be on the python classpath
logging_config_class = my.path.default_local_settings.LOGGING_CONFIG
```

The logging configuration file needs to be on the `PYTHONPATH`, for example `$AIRFLOW_HOME/config`. This directory is loaded by default. Any directory may be added to the `PYTHONPATH`, this might be handy when the config is in another directory or a volume is mounted in case of Docker.

The config can be taken from `airflow/config_templates/airflow_local_settings.py` as a starting point. Copy the contents to `${AIRFLOW_HOME}/config/airflow_local_settings.py`,  and alter the config as is preferred.

```
#
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

import os

from airflow import configuration as conf

# TODO: Logging format and level should be configured
# in this file instead of from airflow.cfg. Currently
# there are other log format and level configurations in
# settings.py and cli.py. Please see AIRFLOW-1455.

LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
LOG_FORMAT = conf.get('core', 'log_format')

BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'child_process_log_directory')

FILENAME_TEMPLATE = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log'
PROCESSOR_FILENAME_TEMPLATE = '{{ filename }}.log'

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': LOG_FORMAT,
        },
        'airflow.processor': {
            'format': LOG_FORMAT,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
        'file.task': {
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'filename_template': FILENAME_TEMPLATE,
        },
        'file.processor': {
            'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
            'formatter': 'airflow.processor',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
        }
        # When using s3 or gcs, provide a customized LOGGING_CONFIG
        # in airflow_local_settings within your PYTHONPATH, see UPDATING.md
        # for details
        # 's3.task': {
        #     'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
        #     'formatter': 'airflow.task',
        #     'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
        #     's3_log_folder': S3_LOG_FOLDER,
        #     'filename_template': FILENAME_TEMPLATE,
        # },
        # 'gcs.task': {
        #     'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
        #     'formatter': 'airflow.task',
        #     'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
        #     'gcs_log_folder': GCS_LOG_FOLDER,
        #     'filename_template': FILENAME_TEMPLATE,
        # },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': LOG_LEVEL
        },
        'airflow': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.processor': {
            'handlers': ['file.processor'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
        'airflow.task': {
            'handlers': ['file.task'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.task_runner': {
            'handlers': ['file.task'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
    }
}
```

To customize the logging (for example, use logging rotate), define one or more of the logging handles that [Python has to offer](https://docs.python.org/3/library/logging.handlers.html). For more details about the Python logging, please refer to the [official logging documentation](https://docs.python.org/3/library/logging.html).

Furthermore, this change also simplifies logging within the DAG itself:

```
root@ae1bc863e815:/airflow# python
Python 3.6.2 (default, Sep 13 2017, 14:26:54)
[GCC 4.9.2] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from airflow.settings import *
>>>
>>> from datetime import datetime
>>> from airflow.models.dag import DAG
>>> from airflow.operators.dummy_operator import DummyOperator
>>>
>>> dag = DAG('simple_dag', start_date=datetime(2017, 9, 1))
>>>
>>> task = DummyOperator(task_id='task_1', dag=dag)
>>>
>>> task.log.error('I want to say something..')
[2017-09-25 20:17:04,927] {<stdin>:1} ERROR - I want to say something..
```

#### Template path of the file_task_handler

The `file_task_handler` logger has been made more flexible. The default format can be changed, `{dag_id}/{task_id}/{execution_date}/{try_number}.log` by supplying Jinja templating in the `FILENAME_TEMPLATE` configuration variable. See the `file_task_handler` for more information.

#### I'm using S3Log or GCSLogs, what do I do!?

If you are logging to Google cloud storage, please see the [Google cloud platform documentation](https://airflow.apache.org/integration.html#gcp-google-cloud-platform) for logging instructions.

If you are using S3, the instructions should be largely the same as the Google cloud platform instructions above. You will need a custom logging config. The `REMOTE_BASE_LOG_FOLDER` configuration key in your airflow config has been removed, therefore you will need to take the following steps:

- Copy the logging configuration from [`airflow/config_templates/airflow_logging_settings.py`](https://github.com/apache/airflow/blob/master/airflow/config_templates/airflow_local_settings.py).
- Place it in a directory inside the Python import path `PYTHONPATH`. If you are using Python 2.7, ensuring that any `__init__.py` files exist so that it is importable.
- Update the config by setting the path of `REMOTE_BASE_LOG_FOLDER` explicitly in the config. The `REMOTE_BASE_LOG_FOLDER` key is not used anymore.
- Set the `logging_config_class` to the filename and dict. For example, if you place `custom_logging_config.py` on the base of your `PYTHONPATH`, you will need to set `logging_config_class = custom_logging_config.LOGGING_CONFIG` in your config as Airflow 1.8.

### New Features

#### Dask Executor

A new DaskExecutor allows Airflow tasks to be run in Dask Distributed clusters.

### Deprecated Features

These features are marked for deprecation. They may still work (and raise a `DeprecationWarning`), but are no longer
supported and will be removed entirely in Airflow 2.0

- If you're using the `google_cloud_conn_id` or `dataproc_cluster` argument names explicitly in `contrib.operators.Dataproc{*}Operator`(s), be sure to rename them to `gcp_conn_id` or `cluster_name`, respectively. We've renamed these arguments for consistency. (AIRFLOW-1323)

- `post_execute()` hooks now take two arguments, `context` and `result`
  (AIRFLOW-886)

  Previously, post_execute() only took one argument, `context`.

- `contrib.hooks.gcp_dataflow_hook.DataFlowHook` starts to use `--runner=DataflowRunner` instead of `DataflowPipelineRunner`, which is removed from the package `google-cloud-dataflow-0.6.0`.

- The pickle type for XCom messages has been replaced by json to prevent RCE attacks.
  Note that JSON serialization is stricter than pickling, so if you want to e.g. pass
  raw bytes through XCom you must encode them using an encoding like base64.
  By default pickling is still enabled until Airflow 2.0. To disable it
  set enable_xcom_pickling = False in your Airflow config.

## Airflow 1.8.1

The Airflow package name was changed from `airflow` to `apache-airflow` during this release. You must uninstall
a previously installed version of Airflow before installing 1.8.1.

## Airflow 1.8

### Database

The database schema needs to be upgraded. Make sure to shutdown Airflow and make a backup of your database. To
upgrade the schema issue `airflow upgradedb`.

### Upgrade systemd unit files

Systemd unit files have been updated. If you use systemd please make sure to update these.

> Please note that the webserver does not detach properly, this will be fixed in a future version.

### Tasks not starting although dependencies are met due to stricter pool checking

Airflow 1.7.1 has issues with being able to over subscribe to a pool, ie. more slots could be used than were
available. This is fixed in Airflow 1.8.0, but due to past issue jobs may fail to start although their
dependencies are met after an upgrade. To workaround either temporarily increase the amount of slots above
the amount of queued tasks or use a new pool.

### Less forgiving scheduler on dynamic start_date

Using a dynamic start_date (e.g. `start_date = datetime.now()`) is not considered a best practice. The 1.8.0 scheduler
is less forgiving in this area. If you encounter DAGs not being scheduled you can try using a fixed start_date and
renaming your DAG. The last step is required to make sure you start with a clean slate, otherwise the old schedule can
interfere.

### New and updated scheduler options

Please read through the new scheduler options, defaults have changed since 1.7.1.

#### child_process_log_directory

In order to increase the robustness of the scheduler, DAGS are now processed in their own process. Therefore each
DAG has its own log file for the scheduler. These log files are placed in `child_process_log_directory` which defaults to
`<AIRFLOW_HOME>/scheduler/latest`. You will need to make sure these log files are removed.

> DAG logs or processor logs ignore and command line settings for log file locations.

#### run_duration

Previously the command line option `num_runs` was used to let the scheduler terminate after a certain amount of
loops. This is now time bound and defaults to `-1`, which means run continuously. See also num_runs.

#### num_runs

Previously `num_runs` was used to let the scheduler terminate after a certain amount of loops. Now num_runs specifies
the number of times to try to schedule each DAG file within `run_duration` time. Defaults to `-1`, which means try
indefinitely. This is only available on the command line.

#### min_file_process_interval

After how much time should an updated DAG be picked up from the filesystem.

#### min_file_parsing_loop_time
CURRENTLY DISABLED DUE TO A BUG
How many seconds to wait between file-parsing loops to prevent the logs from being spammed.

#### dag_dir_list_interval

The frequency with which the scheduler should relist the contents of the DAG directory. If while developing +dags, they are not being picked up, have a look at this number and decrease it when necessary.

#### catchup_by_default

By default the scheduler will fill any missing interval DAG Runs between the last execution date and the current date.
This setting changes that behavior to only execute the latest interval. This can also be specified per DAG as
`catchup = False / True`. Command line backfills will still work.

### Faulty DAGs do not show an error in the Web UI

Due to changes in the way Airflow processes DAGs the Web UI does not show an error when processing a faulty DAG. To
find processing errors go the `child_process_log_directory` which defaults to `<AIRFLOW_HOME>/scheduler/latest`.

### New DAGs are paused by default

Previously, new DAGs would be scheduled immediately. To retain the old behavior, add this to airflow.cfg:

```
[core]
dags_are_paused_at_creation = False
```

### Airflow Context variable are passed to Hive config if conf is specified

If you specify a hive conf to the run_cli command of the HiveHook, Airflow add some
convenience variables to the config. In case you run a secure Hadoop setup it might be
required to whitelist these variables by adding the following to your configuration:

```
<property>
     <name>hive.security.authorization.sqlstd.confwhitelist.append</name>
     <value>airflow\.ctx\..*</value>
</property>
```

### Google Cloud Operator and Hook alignment

All Google Cloud Operators and Hooks are aligned and use the same client library. Now you have a single connection
type for all kinds of Google Cloud Operators.

If you experience problems connecting with your operator make sure you set the connection type "Google Cloud Platform".

Also the old P12 key file type is not supported anymore and only the new JSON key files are supported as a service
account.

### Deprecated Features

These features are marked for deprecation. They may still work (and raise a `DeprecationWarning`), but are no longer
supported and will be removed entirely in Airflow 2.0

- Hooks and operators must be imported from their respective submodules

  `airflow.operators.PigOperator` is no longer supported; `from airflow.operators.pig_operator import PigOperator` is.
  (AIRFLOW-31, AIRFLOW-200)

- Operators no longer accept arbitrary arguments

  Previously, `Operator.__init__()` accepted any arguments (either positional `*args` or keyword `**kwargs`) without
  complaint. Now, invalid arguments will be rejected. (https://github.com/apache/airflow/pull/1285)

- The config value secure_mode will default to True which will disable some insecure endpoints/features

### Known Issues

There is a report that the default of "-1" for num_runs creates an issue where errors are reported while parsing tasks.
It was not confirmed, but a workaround was found by changing the default back to `None`.

To do this edit `cli.py`, find the following:

```
        'num_runs': Arg(
            ("-n", "--num_runs"),
            default=-1, type=int,
            help="Set the number of runs to execute before exiting"),
```

and change `default=-1` to `default=None`. If you have this issue please report it on the mailing list.

## Airflow 1.7.1.2

### Changes to Configuration

#### Email configuration change

To continue using the default smtp email backend, change the email_backend line in your config file from:

```
[email]
email_backend = airflow.utils.send_email_smtp
```

to:

```
[email]
email_backend = airflow.utils.email.send_email_smtp
```

#### S3 configuration change

To continue using S3 logging, update your config file so:

```
s3_log_folder = s3://my-airflow-log-bucket/logs
```

becomes:

```
remote_base_log_folder = s3://my-airflow-log-bucket/logs
remote_log_conn_id = <your desired s3 connection>
```
