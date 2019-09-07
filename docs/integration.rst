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

Integration
===========

- :ref:`Azure`
- :ref:`AWS`
- :ref:`Databricks`
- :ref:`GCP`
- :ref:`Qubole`

.. _Azure:

Azure: Microsoft Azure
----------------------

Airflow has limited support for Microsoft Azure: interfaces exist only for Azure Blob
Storage and Azure Data Lake. Hook, Sensor and Operator for Blob Storage and
Azure Data Lake Hook are in contrib section.

Logging
'''''''

Airflow can be configured to read and write task logs in Azure Blob Storage.
See :ref:`write-logs-azure`.


Azure Blob Storage
''''''''''''''''''

All classes communicate via the Window Azure Storage Blob protocol. Make sure that a
Airflow connection of type `wasb` exists. Authorization can be done by supplying a
login (=Storage account name) and password (=KEY), or login and SAS token in the extra
field (see connection `wasb_default` for an example).

:class:`airflow.contrib.hooks.wasb_hook.WasbHook`
    Interface with Azure Blob Storage.

:class:`airflow.contrib.sensors.wasb_sensor.WasbBlobSensor`
    Checks if a blob is present on Azure Blob storage.

:class:`airflow.contrib.operators.wasb_delete_blob_operator.WasbDeleteBlobOperator`
    Deletes blob(s) on Azure Blob Storage.

:class:`airflow.contrib.sensors.wasb_sensor.WasbPrefixSensor`
    Checks if blobs matching a prefix are present on Azure Blob storage.

:class:`airflow.contrib.operators.file_to_wasb.FileToWasbOperator`
    Uploads a local file to a container as a blob.


Azure File Share
''''''''''''''''

Cloud variant of a SMB file share. Make sure that a Airflow connection of
type `wasb` exists. Authorization can be done by supplying a login (=Storage account name)
and password (=Storage account key), or login and SAS token in the extra field
(see connection `wasb_default` for an example).

:class:`airflow.contrib.hooks.azure_fileshare_hook.AzureFileShareHook`:
    Interface with Azure File Share.

Azure CosmosDB
''''''''''''''

AzureCosmosDBHook communicates via the Azure Cosmos library. Make sure that a
Airflow connection of type `azure_cosmos` exists. Authorization can be done by supplying a
login (=Endpoint uri), password (=secret key) and extra fields database_name and collection_name to specify the
default database and collection to use (see connection `azure_cosmos_default` for an example).

:class:`airflow.contrib.hooks.azure_cosmos_hook.AzureCosmosDBHook`
    Interface with Azure CosmosDB.

:class:`airflow.contrib.operators.azure_cosmos_operator.AzureCosmosInsertDocumentOperator`
    Simple operator to insert document into CosmosDB.

:class:`airflow.contrib.sensors.azure_cosmos_sensor.AzureCosmosDocumentSensor`
    Simple sensor to detect document existence in CosmosDB.


Azure Data Lake
'''''''''''''''

AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make sure that a
Airflow connection of type `azure_data_lake` exists. Authorization can be done by supplying a
login (=Client ID), password (=Client Secret) and extra fields tenant (Tenant) and account_name (Account Name)
(see connection `azure_data_lake_default` for an example).

:class:`airflow.contrib.hooks.azure_data_lake_hook.AzureDataLakeHook`
    Interface with Azure Data Lake.

:class:`airflow.contrib.operators.adls_list_operator.AzureDataLakeStorageListOperator`
    Lists the files located in a specified Azure Data Lake path.

:class:`airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator`
    Copies files from an Azure Data Lake path to a Google Cloud Storage bucket.


Azure Container Instances
'''''''''''''''''''''''''

Azure Container Instances provides a method to run a docker container without having to worry
about managing infrastructure. The AzureContainerInstanceHook requires a service principal. The
credentials for this principal can either be defined in the extra field ``key_path``, as an
environment variable named ``AZURE_AUTH_LOCATION``,
or by providing a login/password and tenantId in extras.

The AzureContainerRegistryHook requires a host/login/password to be defined in the connection.

:class:`airflow.contrib.hooks.azure_container_volume_hook.AzureContainerVolumeHook`
    Interface with Azure Container Volumes

:class:`airflow.contrib.operators.azure_container_instances_operator.AzureContainerInstancesOperator`
    Start/Monitor a new ACI.

:class:`airflow.contrib.hooks.azure_container_instance_hook.AzureContainerInstanceHook`
    Wrapper around a single ACI.

:class:`airflow.contrib.hooks.azure_container_registry_hook.AzureContainerRegistryHook`
    Interface with ACR



.. _AWS:

AWS: Amazon Web Services
------------------------

Airflow has extensive support for Amazon Web Services. But note that the Hooks, Sensors and
Operators are in the contrib section.

AWS EMR
'''''''

:class:`airflow.contrib.hooks.emr_hook.EmrHook`
    Interface with AWS EMR.

:class:`airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator`
    Adds steps to an existing EMR JobFlow.

:class:`airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator`
    Creates an EMR JobFlow, reading the config from the EMR connection.

:class:`airflow.contrib.operators.emr_terminate_job_flow_operator.EmrTerminateJobFlowOperator`
    Terminates an EMR JobFlow.


AWS S3
''''''

:class:`airflow.hooks.S3_hook.S3Hook`
    Interface with AWS S3.

:class:`airflow.operators.s3_file_transform_operator.S3FileTransformOperator`
    Copies data from a source S3 location to a temporary location on the local filesystem.

:class:`airflow.contrib.operators.s3_list_operator.S3ListOperator`
    Lists the files matching a key prefix from a S3 location.

:class:`airflow.contrib.operators.s3_to_gcs_operator.S3ToGoogleCloudStorageOperator`
    Syncs an S3 location with a Google Cloud Storage bucket.

:class:`airflow.contrib.operators.s3_to_gcs_transfer_operator.S3ToGoogleCloudStorageTransferOperator`
    Syncs an S3 bucket with a Google Cloud Storage bucket using the GCP Storage Transfer Service.

:class:`airflow.operators.s3_to_hive_operator.S3ToHiveTransfer`
    Moves data from S3 to Hive. The operator downloads a file from S3, stores the file locally before loading it into a Hive table.


AWS Batch Service
'''''''''''''''''

:class:`airflow.contrib.operators.awsbatch_operator.AWSBatchOperator`
    Execute a task on AWS Batch Service.


AWS RedShift
''''''''''''

:class:`airflow.contrib.sensors.aws_redshift_cluster_sensor.AwsRedshiftClusterSensor`
    Waits for a Redshift cluster to reach a specific status.

:class:`airflow.contrib.hooks.redshift_hook.RedshiftHook`
    Interact with AWS Redshift, using the boto3 library.

:class:`airflow.operators.redshift_to_s3_operator.RedshiftToS3Transfer`
    Executes an unload command to S3 as CSV with or without headers.

:class:`airflow.operators.s3_to_redshift_operator.S3ToRedshiftTransfer`
    Executes an copy command from S3 as CSV with or without headers.



AWS DynamoDB
''''''''''''

:class:`airflow.contrib.operators.hive_to_dynamodb.HiveToDynamoDBTransferOperator`
     Moves data from Hive to DynamoDB.

:class:`airflow.contrib.hooks.aws_dynamodb_hook.AwsDynamoDBHook`
    Interface with AWS DynamoDB.


AWS Lambda
''''''''''

:class:`airflow.contrib.hooks.aws_lambda_hook.AwsLambdaHook`
    Interface with AWS Lambda.


AWS Kinesis
'''''''''''

:class:`airflow.contrib.hooks.aws_firehose_hook.AwsFirehoseHook`
    Interface with AWS Kinesis Firehose.


Amazon SageMaker
''''''''''''''''

For more instructions on using Amazon SageMaker in Airflow, please see `the SageMaker Python SDK README`_.

.. _the SageMaker Python SDK README: https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/README.rst

:class:`airflow.contrib.hooks.sagemaker_hook.SageMakerHook`
    Interface with Amazon SageMaker.

:class:`airflow.contrib.operators.sagemaker_training_operator.SageMakerTrainingOperator`
    Create a SageMaker training job.

:class:`airflow.contrib.operators.sagemaker_tuning_operator.SageMakerTuningOperator`
    Create a SageMaker tuning job.

:class:`airflow.contrib.operators.sagemaker_model_operator.SageMakerModelOperator`
    Create a SageMaker model.

:class:`airflow.contrib.operators.sagemaker_transform_operator.SageMakerTransformOperator`
    Create a SageMaker transform job.

:class:`airflow.contrib.operators.sagemaker_endpoint_config_operator.SageMakerEndpointConfigOperator`
    Create a SageMaker endpoint config.

:class:`airflow.contrib.operators.sagemaker_endpoint_operator.SageMakerEndpointOperator`
    Create a SageMaker endpoint.


.. _Databricks:

Databricks
----------

With contributions from `Databricks <https://databricks.com/>`__, Airflow has several operators
which enable the submitting and running of jobs to the Databricks platform. Internally the
operators talk to the ``api/2.0/jobs/runs/submit`` `endpoint <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_.


:class:`airflow.contrib.operators.databricks_operator.DatabricksSubmitRunOperator`
    Submits a Spark job run to Databricks using the
    `api/2.0/jobs/runs/submit
    <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_
    API endpoint.

:class:`airflow.contrib.operators.databricks_operator.DatabricksRunNowOperator`
    Runs an existing Spark job in Databricks using the
        `api/2.0/jobs/run-now
        <https://docs.databricks.com/api/latest/jobs.html#run-now>`_
        API endpoint.


.. _GCP:

GCP: Google Cloud Platform
--------------------------

Airflow has extensive support for the Google Cloud Platform. But note that most Hooks and
Operators are in the contrib section. Meaning that they have a *beta* status, meaning that
they can have breaking changes between minor releases.

See the :doc:`GCP connection type <howto/connection/gcp>` documentation to
configure connections to GCP.

Logging
'''''''

Airflow can be configured to read and write task logs in Google Cloud Storage.
See :ref:`write-logs-gcp`.


GoogleCloudBaseHook
'''''''''''''''''''

All hooks is based on :class:`airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook`.


BigQuery
''''''''

:class:`airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator`
    Performs checks against a SQL query that will return a single row with different values.

:class:`airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator`
    Checks that the values of metrics given as SQL expressions are within a certain tolerance of the ones from days_back before.

:class:`airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator`
    Performs a simple value check using SQL code.

:class:`airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator`
    Fetches the data from a BigQuery table and returns data in a python list

:class:`airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyDatasetOperator`
    Creates an empty BigQuery dataset.

:class:`airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyTableOperator`
    Creates a new, empty table in the specified BigQuery dataset optionally with schema.

:class:`airflow.contrib.operators.bigquery_operator.BigQueryCreateExternalTableOperator`
    Creates a new, external table in the dataset with the data in Google Cloud Storage.

:class:`airflow.contrib.operators.bigquery_operator.BigQueryDeleteDatasetOperator`
    Deletes an existing BigQuery dataset.

:class:`airflow.contrib.operators.bigquery_operator.BigQueryGetDatasetOperator`
    This operator is used to return the dataset specified by dataset_id.

:class:`airflow.contrib.operators.bigquery_operator.BigQueryUpdateDatasetOperator`
    This operator is used to update dataset for your Project in BigQuery.
    The update method replaces the entire dataset resource, whereas the patch
    method only replaces fields that are provided in the submitted dataset resource.

:class:`airflow.contrib.operators.bigquery_operator.BigQueryPatchDatasetOperator`
    This operator is used to patch dataset for your Project in BigQuery.
    It only replaces fields that are provided in the submitted dataset resource.

:class:`airflow.contrib.operators.bigquery_operator.BigQueryOperator`
    Executes BigQuery SQL queries in a specific BigQuery database.

:class:`airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator`
    Deletes an existing BigQuery table.

:class:`airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator`
    Copy a BigQuery table to another BigQuery table.

:class:`airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator`
    Transfers a BigQuery table to a Google Cloud Storage bucket


They also use :class:`airflow.contrib.hooks.bigquery_hook.BigQueryHook` to communicate with Google Cloud Platform.

BigQuery Data Transfer Service
''''''''''''''''''''''''''''''

:class:`airflow.gcp.operators.bigquery_dts.BigQueryCreateDataTransferOperator`
    Creates a new data transfer configuration.

:class:`airflow.gcp.operators.bigquery_dts.BigQueryDeleteDataTransferConfigOperator`
    Deletes transfer configuration.

:class:`airflow.gcp.sensors.bigquery_dts.BigQueryDataTransferServiceTransferRunSensor`
    Waits for Data Transfer Service run to complete.

They also use :class:`airflow.gcp.hooks.bigquery_dts.BiqQueryDataTransferServiceHook` to communicate with Google Cloud Platform.


Cloud Spanner
'''''''''''''

:class:`airflow.gcp.operators.spanner.CloudSpannerInstanceDatabaseDeleteOperator`
    deletes an existing database from a Google Cloud Spanner instance or returns success if the database is missing.

:class:`airflow.gcp.operators.spanner.CloudSpannerInstanceDatabaseDeployOperator`
    creates a new database in a Google Cloud instance or returns success if the database already exists.

:class:`airflow.gcp.operators.spanner.CloudSpannerInstanceDatabaseQueryOperator`
    executes an arbitrary DML query (INSERT, UPDATE, DELETE).

:class:`airflow.gcp.operators.spanner.CloudSpannerInstanceDatabaseUpdateOperator`
    updates the structure of a Google Cloud Spanner database.

:class:`airflow.gcp.operators.spanner.CloudSpannerInstanceDeleteOperator`
    deletes a Google Cloud Spanner instance.

:class:`airflow.gcp.operators.spanner.CloudSpannerInstanceDeployOperator`
    creates a new Google Cloud Spanner instance, or if an instance with the same name exists, updates the instance.


They also use :class:`airflow.gcp.hooks.spanner.CloudSpannerHook` to communicate with Google Cloud Platform.


Cloud SQL
'''''''''

:class:`airflow.gcp.operators.cloud_sql.CloudSqlInstanceCreateOperator`
    create a new Cloud SQL instance.

:class:`airflow.gcp.operators.cloud_sql.CloudSqlInstanceDatabaseCreateOperator`
    creates a new database inside a Cloud SQL instance.

:class:`airflow.gcp.operators.cloud_sql.CloudSqlInstanceDatabaseDeleteOperator`
    deletes a database from a Cloud SQL instance.

:class:`airflow.gcp.operators.cloud_sql.CloudSqlInstanceDatabasePatchOperator`
    updates a database inside a Cloud SQL instance.

:class:`airflow.gcp.operators.cloud_sql.CloudSqlInstanceDeleteOperator`
    delete a Cloud SQL instance.

:class:`airflow.gcp.operators.cloud_sql.CloudSqlInstanceExportOperator`
    exports data from a Cloud SQL instance.

:class:`airflow.gcp.operators.cloud_sql.CloudSqlInstanceImportOperator`
    imports data into a Cloud SQL instance.

:class:`airflow.gcp.operators.cloud_sql.CloudSqlInstancePatchOperator`
    patch a Cloud SQL instance.

:class:`airflow.gcp.operators.cloud_sql.CloudSqlQueryOperator`
    run query in a Cloud SQL instance.


They also use :class:`airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook` and :class:`airflow.gcp.hooks.cloud_sql.CloudSqlHook` to communicate with Google Cloud Platform.


Cloud Bigtable
''''''''''''''

:class:`airflow.gcp.operators.bigtable.BigtableClusterUpdateOperator`
    updates the number of nodes in a Google Cloud Bigtable cluster.

:class:`airflow.gcp.operators.bigtable.BigtableInstanceCreateOperator`
    creates a Cloud Bigtable instance.

:class:`airflow.gcp.operators.bigtable.BigtableInstanceDeleteOperator`
    deletes a Google Cloud Bigtable instance.

:class:`airflow.gcp.operators.bigtable.BigtableTableCreateOperator`
    creates a table in a Google Cloud Bigtable instance.

:class:`airflow.gcp.operators.bigtable.BigtableTableDeleteOperator`
    deletes a table in a Google Cloud Bigtable instance.

:class:`airflow.gcp.sensors.bigtable.BigtableTableWaitForReplicationSensor`
    (sensor) waits for a table to be fully replicated.


They also use :class:`airflow.gcp.hooks.bigtable.BigtableHook` to communicate with Google Cloud Platform.

Cloud Build
'''''''''''

:class:`airflow.gcp.operators.cloud_build.CloudBuildCreateBuildOperator`
     Starts a build with the specified configuration.


They also use :class:`airflow.gcp.hooks.cloud_build.CloudBuildHook` to communicate with Google Cloud Platform.


Compute Engine
''''''''''''''

:class:`airflow.gcp.operators.compute.GceInstanceStartOperator`
    start an existing Google Compute Engine instance.

:class:`airflow.gcp.operators.compute.GceInstanceStopOperator`
    stop an existing Google Compute Engine instance.

:class:`airflow.gcp.operators.compute.GceSetMachineTypeOperator`
    change the machine type for a stopped instance.

:class:`airflow.gcp.operators.compute.GceInstanceTemplateCopyOperator`
    copy the Instance Template, applying specified changes.

:class:`airflow.gcp.operators.compute.GceInstanceGroupManagerUpdateTemplateOperator`
    patch the Instance Group Manager, replacing source Instance Template URL with the destination one.


The operators have the common base operator :class:`airflow.gcp.operators.compute.GceBaseOperator`

They also use :class:`airflow.gcp.hooks.compute.GceHook` to communicate with Google Cloud Platform.


Cloud Functions
'''''''''''''''

:class:`airflow.gcp.operators.functions.GcfFunctionDeployOperator`
    deploy Google Cloud Function to Google Cloud Platform

:class:`airflow.gcp.operators.functions.GcfFunctionDeleteOperator`
    delete Google Cloud Function in Google Cloud Platform

:class:`airflow.gcp.operators.functions.GcfFunctionInvokeOperator`
    invoke Google Cloud Function in Google Cloud Platform


They also use :class:`airflow.gcp.hooks.functions.GcfHook` to communicate with Google Cloud Platform.


Cloud DataFlow
''''''''''''''

:class:`airflow.gcp.operators.dataflow.DataFlowJavaOperator`
    launching Cloud Dataflow jobs written in Java.

:class:`airflow.gcp.operators.dataflow.DataflowTemplateOperator`
    launching a templated Cloud DataFlow batch job.

:class:`airflow.gcp.operators.dataflow.DataFlowPythonOperator`
    launching Cloud Dataflow jobs written in python.


They also use :class:`airflow.gcp.hooks.dataflow.DataFlowHook` to communicate with Google Cloud Platform.


Cloud DataProc
''''''''''''''

:class:`airflow.gcp.operators.dataproc.DataprocClusterCreateOperator`
    Create a new cluster on Google Cloud Dataproc.

:class:`airflow.gcp.operators.dataproc.DataprocClusterDeleteOperator`
    Delete a cluster on Google Cloud Dataproc.

:class:`airflow.gcp.operators.dataproc.DataprocClusterScaleOperator`
    Scale up or down a cluster on Google Cloud Dataproc.

:class:`airflow.gcp.operators.dataproc.DataProcHadoopOperator`
    Start a Hadoop Job on a Cloud DataProc cluster.

:class:`airflow.gcp.operators.dataproc.DataProcHiveOperator`
    Start a Hive query Job on a Cloud DataProc cluster.

:class:`airflow.gcp.operators.dataproc.DataProcPigOperator`
    Start a Pig query Job on a Cloud DataProc cluster.

:class:`airflow.gcp.operators.dataproc.DataProcPySparkOperator`
    Start a PySpark Job on a Cloud DataProc cluster.

:class:`airflow.gcp.operators.dataproc.DataProcSparkOperator`
    Start a Spark Job on a Cloud DataProc cluster.

:class:`airflow.gcp.operators.dataproc.DataProcSparkSqlOperator`
    Start a Spark SQL query Job on a Cloud DataProc cluster.

:class:`airflow.gcp.operators.dataproc.DataprocWorkflowTemplateInstantiateInlineOperator`
    Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc.

:class:`airflow.gcp.operators.dataproc.DataprocWorkflowTemplateInstantiateOperator`
    Instantiate a WorkflowTemplate on Google Cloud Dataproc.


Cloud Datastore
'''''''''''''''

:class:`airflow.gcp.operators.datastore.DatastoreExportOperator`
    Export entities from Google Cloud Datastore to Cloud Storage.

:class:`airflow.gcp.operators.datastore.DatastoreImportOperator`
    Import entities from Cloud Storage to Google Cloud Datastore.


They also use :class:`airflow.gcp.hooks.datastore.DatastoreHook` to communicate with Google Cloud Platform.


Cloud ML Engine
'''''''''''''''

:class:`airflow.gcp.operators.mlengine.MLEngineBatchPredictionOperator`
    Start a Cloud ML Engine batch prediction job.

:class:`airflow.gcp.operators.mlengine.MLEngineModelOperator`
    Manages a Cloud ML Engine model.

:class:`airflow.gcp.operators.mlengine.MLEngineTrainingOperator`
    Start a Cloud ML Engine training job.

:class:`airflow.gcp.operators.mlengine.MLEngineVersionOperator`
    Manages a Cloud ML Engine model version.


They also use :class:`airflow.gcp.hooks.mlengine.MLEngineHook` to communicate with Google Cloud Platform.


Cloud Storage
'''''''''''''

:class:`airflow.operators.local_to_gcs.FileToGoogleCloudStorageOperator`
    Uploads a file to Google Cloud Storage.

:class:`airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator`
    Creates a new ACL entry on the specified bucket.

:class:`airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator`
    Creates a new ACL entry on the specified object.

:class:`airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator`
    Downloads a file from Google Cloud Storage.

:class:`airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageListOperator`
    List all objects from the bucket with the give string prefix and delimiter in name.

:class:`airflow.contrib.operators.gcs_operator.GoogleCloudStorageCreateBucketOperator`
    Creates a new cloud storage bucket.

:class:`airflow.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator`
    Loads files from Google cloud storage into BigQuery.

:class:`airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator`
    Copies objects from a bucket to another, with renaming if requested.

:class:`airflow.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator`
    Copy data from any MySQL Database to Google cloud storage in JSON format.

:class:`airflow.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator`
    Copy data from any Microsoft SQL Server Database to Google Cloud Storage in JSON format.

:class:`airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectSensor`
    Checks for the existence of a file in Google Cloud Storage.

:class:`airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectUpdatedSensor`
    Checks if an object is updated in Google Cloud Storage.

:class:`airflow.contrib.sensors.gcs_sensor.GoogleCloudStoragePrefixSensor`
    Checks for the existence of a objects at prefix in Google Cloud Storage.

:class:`airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageUploadSessionCompleteSession`
    Checks for changes in the number of objects at prefix in Google Cloud Storage
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects for situations when many objects
    are being uploaded to a bucket with no formal success signal.

:class:`airflow.contrib.operators.gcs_delete_operator.GoogleCloudStorageDeleteOperator`
    Deletes objects from a Google Cloud Storage bucket.


They also use :class:`airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook` to communicate with Google Cloud Platform.


Transfer Service
''''''''''''''''

:class:`airflow.cogcpperators.cloud_storage_transfer_service.GcpTransferServiceJobDeleteOperator`
    Deletes a transfer job.
:class:`airflow.cogcpperators.cloud_storage_transfer_service.GcpTransferServiceJobCreateOperator`
    Creates a transfer job.
:class:`airflow.cogcpperators.cloud_storage_transfer_service.GcpTransferServiceJobUpdateOperator`
    Updates a transfer job.
:class:`airflow.cogcpperators.cloud_storage_transfer_service.GcpTransferServiceOperationCancelOperator`
    Cancels a transfer operation.
:class:`airflow.cogcpperators.cloud_storage_transfer_service.GcpTransferServiceOperationGetOperator`
    Gets a transfer operation.
:class:`airflow.cogcpperators.cloud_storage_transfer_service.GcpTransferServiceOperationPauseOperator`
    Pauses a transfer operation
:class:`airflow.cogcpperators.cloud_storage_transfer_service.GcpTransferServiceOperationResumeOperator`
    Resumes a transfer operation.
:class:`airflow.cogcpperators.cloud_storage_transfer_service.GcpTransferServiceOperationsListOperator`
    Gets a list of transfer operations.
:class:`airflow.cogcpperators.cloud_storage_transfer_service.GoogleCloudStorageToGoogleCloudStorageTransferOperator`
    Copies objects from a Google Cloud Storage bucket to another bucket.
:class:`airflow.cogcpperators.cloud_storage_transfer_service.S3ToGoogleCloudStorageTransferOperator`
    Synchronizes an S3 bucket with a Google Cloud Storage bucket.


:class:`airflow.gcp.sensors.cloud_storage_transfer_service.GCPTransferServiceWaitForJobStatusSensor`
    Waits for at least one operation belonging to the job to have the
    expected status.


They also use :class:`airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook` to communicate with Google Cloud Platform.


Cloud Vision
''''''''''''

:class:`airflow.gcp.operators.vision.CloudVisionAddProductToProductSetOperator`
    Adds a Product to the specified ProductSet.
:class:`airflow.gcp.operators.vision.CloudVisionAnnotateImageOperator`
    Run image detection and annotation for an image.
:class:`airflow.gcp.operators.vision.CloudVisionProductCreateOperator`
    Creates a new Product resource.
:class:`airflow.gcp.operators.vision.CloudVisionProductDeleteOperator`
    Permanently deletes a product and its reference images.
:class:`airflow.gcp.operators.vision.CloudVisionProductGetOperator`
    Gets information associated with a Product.
:class:`airflow.gcp.operators.vision.CloudVisionProductSetCreateOperator`
    Creates a new ProductSet resource.
:class:`airflow.gcp.operators.vision.CloudVisionProductSetDeleteOperator`
    Permanently deletes a ProductSet.
:class:`airflow.gcp.operators.vision.CloudVisionProductSetGetOperator`
    Gets information associated with a ProductSet.
:class:`airflow.gcp.operators.vision.CloudVisionProductSetUpdateOperator`
    Makes changes to a ProductSet resource.
:class:`airflow.gcp.operators.vision.CloudVisionProductUpdateOperator`
    Makes changes to a Product resource.
:class:`airflow.gcp.operators.vision.CloudVisionReferenceImageCreateOperator`
    Creates a new ReferenceImage resource.
:class:`airflow.gcp.operators.vision.CloudVisionRemoveProductFromProductSetOperator`
    Removes a Product from the specified ProductSet.
:class:`airflow.gcp.operators.vision.CloudVisionAnnotateImageOperator`
    Run image detection and annotation for an image.
:class:`airflow.gcp.operators.vision.CloudVisionDetectTextOperator`
    Run text detection for an image
:class:`airflow.gcp.operators.vision.CloudVisionDetectDocumentTextOperator`
    Run document text detection for an image
:class:`airflow.gcp.operators.vision.CloudVisionDetectImageLabelsOperator`
    Run image labels detection for an image
:class:`airflow.gcp.operators.vision.CloudVisionDetectImageSafeSearchOperator`
    Run safe search detection for an image

They also use :class:`airflow.gcp.hooks.vision.CloudVisionHook` to communicate with Google Cloud Platform.

Cloud Text to Speech
''''''''''''''''''''

:class:`airflow.gcp.operators.text_to_speech.GcpTextToSpeechSynthesizeOperator`
    Synthesizes input text into audio file and stores this file to GCS.

They also use :class:`airflow.gcp.hooks.text_to_speech.GCPTextToSpeechHook` to communicate with Google Cloud Platform.

Cloud Speech to Text
''''''''''''''''''''

:class:`airflow.gcp.operators.speech_to_text.GcpSpeechToTextRecognizeSpeechOperator`
    Recognizes speech in audio input and returns text.

They also use :class:`airflow.gcp.hooks.speech_to_text.GCPSpeechToTextHook` to communicate with Google Cloud Platform.

Cloud Speech Translate
''''''''''''''''''''''

:class:`airflow.gcp.operators.translate_speech.GcpTranslateSpeechOperator`
    Recognizes speech in audio input and translates it.

They also use :class:`airflow.gcp.hooks.speech_to_text.GCPSpeechToTextHook` and
    :class:`airflow.gcp.hooks.translate.CloudTranslateHook` to communicate with Google Cloud Platform.

Cloud Translate
'''''''''''''''

:class:`airflow.gcp.operators.translate.CloudTranslateTextOperator`
    Translate a string or list of strings.


Cloud Video Intelligence
''''''''''''''''''''''''

:class:`airflow.gcp.operators.video_intelligence.CloudVideoIntelligenceDetectVideoLabelsOperator`
    Performs video annotation, annotating video labels.
:class:`airflow.gcp.operators.video_intelligence.CloudVideoIntelligenceDetectVideoExplicitContentOperator`
    Performs video annotation, annotating explicit content.
:class:`airflow.gcp.operators.video_intelligence.CloudVideoIntelligenceDetectVideoShotsOperator`
    Performs video annotation, annotating video shots.

They also use :class:`airflow.gcp.hooks.video_intelligence.CloudVideoIntelligenceHook` to communicate with Google Cloud Platform.

Google Kubernetes Engine
''''''''''''''''''''''''

:class:`airflow.gcp.operators.kubernetes_engine.GKEClusterCreateOperator`
    Creates a Kubernetes Cluster in Google Cloud Platform

:class:`airflow.gcp.operators.kubernetes_engine.GKEClusterDeleteOperator`
    Deletes a Kubernetes Cluster in Google Cloud Platform

:class:`airflow.gcp.operators.kubernetes_engine.GKEPodOperator`
    Executes a task in a Kubernetes pod in the specified Google Kubernetes Engine cluster

They also use :class:`airflow.gcp.hooks.kubernetes_engine.GKEClusterHook` to communicate with Google Cloud Platform.


Google Natural Language
'''''''''''''''''''''''

:class:`airflow.gcp.operators.natural_language.CloudLanguageAnalyzeEntities`
    Finds named entities (currently proper names and common nouns) in the text along with entity types,
    salience, mentions for each entity, and other properties.

:class:`airflow.gcp.operators.natural_language.CloudLanguageAnalyzeEntitySentiment`
    Finds entities, similar to AnalyzeEntities in the text and analyzes sentiment associated with each
    entity and its mentions.

:class:`airflow.gcp.operators.natural_language.CloudLanguageAnalyzeSentiment`
    Analyzes the sentiment of the provided text.

:class:`airflow.gcp.operators.natural_language.CloudLanguageClassifyTextOperator`
    Classifies a document into categories.

They also use :class:`airflow.gcp.hooks.natural_language.CloudNaturalLanguageHook` to communicate with Google Cloud Platform.


Google Cloud Data Loss Prevention (DLP)
'''''''''''''''''''''''''''''''''''''''

:class:`airflow.gcp.operators.dlp.CloudDLPCancelDLPJobOperator`
    Starts asynchronous cancellation on a long-running DlpJob.

:class:`airflow.gcp.operators.dlp.CloudDLPCreateDeidentifyTemplateOperator`
    Creates a DeidentifyTemplate for re-using frequently used configuration for
    de-identifying content, images, and storage.

:class:`airflow.gcp.operators.dlp.CloudDLPCreateDLPJobOperator`
    Creates a new job to inspect storage or calculate risk metrics.

:class:`airflow.gcp.operators.dlp.CloudDLPCreateInspectTemplateOperator`
    Creates an InspectTemplate for re-using frequently used configuration for
    inspecting content, images, and storage.

:class:`airflow.gcp.operators.dlp.CloudDLPCreateJobTriggerOperator`
    Creates a job trigger to run DLP actions such as scanning storage for sensitive
    information on a set schedule.

:class:`airflow.gcp.operators.dlp.CloudDLPCreateStoredInfoTypeOperator`
    Creates a pre-built stored infoType to be used for inspection.

:class:`airflow.gcp.operators.dlp.CloudDLPDeidentifyContentOperator`
    De-identifies potentially sensitive info from a ContentItem. This method has limits
    on input size and output size.

:class:`airflow.gcp.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator`
    Deletes a DeidentifyTemplate.

:class:`airflow.gcp.operators.dlp.CloudDLPDeleteDlpJobOperator`
    Deletes a long-running DlpJob. This method indicates that the client is no longer
    interested in the DlpJob result. The job will be cancelled if possible.

:class:`airflow.gcp.operators.dlp.CloudDLPDeleteInspectTemplateOperator`
    Deletes an InspectTemplate.

:class:`airflow.gcp.operators.dlp.CloudDLPDeleteJobTriggerOperator`
    Deletes a job trigger.

:class:`airflow.gcp.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator`
    Deletes a stored infoType.

:class:`airflow.gcp.operators.dlp.CloudDLPGetDeidentifyTemplateOperator`
    Gets a DeidentifyTemplate.

:class:`airflow.gcp.operators.dlp.CloudDLPGetDlpJobOperator`
    Gets the latest state of a long-running DlpJob.

:class:`airflow.gcp.operators.dlp.CloudDLPGetInspectTemplateOperator`
    Gets an InspectTemplate.

:class:`airflow.gcp.operators.dlp.CloudDLPGetJobTripperOperator`
    Gets a job trigger.

:class:`airflow.gcp.operators.dlp.CloudDLPGetStoredInfoTypeOperator`
    Gets a stored infoType.

:class:`airflow.gcp.operators.dlp.CloudDLPInspectContentOperator`
    Finds potentially sensitive info in content. This method has limits on
    input size, processing time, and output size.

:class:`airflow.gcp.operators.dlp.CloudDLPListDeidentifyTemplatesOperator`
    Lists DeidentifyTemplates.

:class:`airflow.gcp.operators.dlp.CloudDLPListDlpJobsOperator`
    Lists DlpJobs that match the specified filter in the request.

:class:`airflow.gcp.operators.dlp.CloudDLPListInfoTypesOperator`
    Returns a list of the sensitive information types that the DLP API supports.

:class:`airflow.gcp.operators.dlp.CloudDLPListInspectTemplatesOperator`
    Lists InspectTemplates.

:class:`airflow.gcp.operators.dlp.CloudDLPListJobTriggersOperator`
    Lists job triggers.

:class:`airflow.gcp.operators.dlp.CloudDLPListStoredInfoTypesOperator`
    Lists stored infoTypes.

:class:`airflow.gcp.operators.dlp.CloudDLPRedactImageOperator`
    Redacts potentially sensitive info from an image. This method has limits on
    input size, processing time, and output size.

:class:`airflow.gcp.operators.dlp.CloudDLPReidentifyContentOperator`
    Re-identifies content that has been de-identified.

:class:`airflow.gcp.operators.dlp.CloudDLPUpdateDeidentifyTemplateOperator`
    Updates the DeidentifyTemplate.

:class:`airflow.gcp.operators.dlp.CloudDLPUpdateInspectTemplateOperator`
    Updates the InspectTemplate.

:class:`airflow.gcp.operators.dlp.CloudDLPUpdateJobTriggerOperator`
    Updates a job trigger.

:class:`airflow.gcp.operators.dlp.CloudDLPUpdateStoredInfoTypeOperator`
    Updates the stored infoType by creating a new version.

They also use :class:`airflow.gcp.hooks.dlp.CloudDLPHook` to communicate with Google Cloud Platform.


Google Cloud Tasks
''''''''''''''''''

:class:`airflow.gcp.operators.tasks.CloudTasksQueueCreateOperator`
    Creates a queue in Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksQueueUpdateOperator`
    Updates a queue in Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksQueueGetOperator`
    Gets a queue from Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksQueuesListOperator`
    Lists queues from Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksQueueDeleteOperator`
    Deletes a queue from Cloud Tasks, even if it has tasks in it.

:class:`airflow.gcp.operators.tasks.CloudTasksQueuePurgeOperator`
    Purges a queue by deleting all of its tasks from Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksQueuePauseOperator`
    Pauses a queue in Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksQueueResumeOperator`
    Resumes a queue in Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksTaskCreateOperator`
    Creates a task in Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksTaskGetOperator`
    Gets a task from Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksTasksListOperator`
    Lists the tasks in Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksTaskDeleteOperator`
    Deletes a task from Cloud Tasks.

:class:`airflow.gcp.operators.tasks.CloudTasksTaskRunOperator`
    Forces to run a task in Cloud Tasks.

They also use :class:`airflow.gcp.hooks.tasks.CloudTasksHook` to communicate with Google Cloud Platform.

Google Natural Language
'''''''''''''''''''''''

:class:`airflow.gcp.operators.automl.AutoMLTrainModelOperator`
    Creates Google Cloud AutoML model.

:class:`airflow.gcp.operators.automl.AutoMLPredictOperator`
    Runs prediction operation on Google Cloud AutoML.

:class:`airflow.gcp.operators.automl.AutoMLBatchPredictOperator`
    Perform a batch prediction on Google Cloud AutoML.

:class:`airflow.gcp.operators.automl.AutoMLCreateDatasetOperator`
    Creates a Google Cloud AutoML dataset.

:class:`airflow.gcp.operators.automl.AutoMLListDatasetOperator`
    Lists AutoML Datasets in project.

:class:`airflow.gcp.operators.automl.AutoMLDeleteDatasetOperator`
    Deletes a dataset and all of its contents.

:class:`airflow.gcp.operators.automl.AutoMLImportDataOperator`
    Imports data to a Google Cloud AutoML dataset.

:class:`airflow.gcp.operators.automl.AutoMLTablesListColumnSpecsOperator`
    Lists column specs in a table.

:class:`airflow.gcp.operators.automl.AutoMLTablesListTableSpecsOperator`
    Lists table specs in a dataset.

:class:`airflow.gcp.operators.automl.AutoMLTablesUpdateDatasetOperator`
    Updates a dataset.

:class:`airflow.gcp.operators.automl.AutoMLGetModelOperator`
    Get Google Cloud AutoML model.

:class:`airflow.gcp.operators.automl.AutoMLDeleteModelOperator`
    Delete Google Cloud AutoML model.

:class:`airflow.gcp.operators.automl.AutoMLDeployModelOperator`
    Deploys a model.

They also use :class:`airflow.gcp.hooks.automl.CloudAutoMLHook` to communicate with Google Cloud Platform.

.. _Qubole:

Qubole
------

Apache Airflow has a native operator and hooks to talk to `Qubole <https://qubole.com/>`__,
which lets you submit your big data jobs directly to Qubole from Apache Airflow.


:class:`airflow.contrib.operators.qubole_operator.QuboleOperator`
    Execute tasks (commands) on QDS (https://qubole.com).

:class:`airflow.contrib.sensors.qubole_sensor.QubolePartitionSensor`
    Wait for a Hive partition to show up in QHS (Qubole Hive Service)
    and check for its presence via QDS APIs

:class:`airflow.contrib.sensors.qubole_sensor.QuboleFileSensor`
    Wait for a file or folder to be present in cloud storage
    and check for its presence via QDS APIs

:class:`airflow.contrib.operators.qubole_check_operator.QuboleCheckOperator`
    Performs checks against Qubole Commands. ``QuboleCheckOperator`` expects
    a command that will be executed on QDS.

:class:`airflow.contrib.operators.qubole_check_operator.QuboleValueCheckOperator`
    Performs a simple value check using Qubole command.
    By default, each value on the first row of this
    Qubole command is compared with a pre-defined value
