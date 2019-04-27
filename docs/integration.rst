..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
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

Logging
'''''''

Airflow can be configured to read and write task logs in Azure Blob Storage.
See :ref:`write-logs-azure`.

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

:class:`airflow.contrib.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator`
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

`Databricks <https://databricks.com/>`__ has contributed an Airflow operator which enables
submitting runs to the Databricks platform. Internally the operator talks to the
``api/2.0/jobs/runs/submit`` `endpoint <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_.


:class:`airflow.contrib.operators.databricks_operator.DatabricksSubmitRunOperator`
    Submits a Spark job run to Databricks using the
    `api/2.0/jobs/runs/submit
    <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_
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

:class:`airflow.contrib.operators.bigquery_operator.BigQueryOperator`
    Executes BigQuery SQL queries in a specific BigQuery database.

:class:`airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator`
    Deletes an existing BigQuery table.

:class:`airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator`
    Copy a BigQuery table to another BigQuery table.

:class:`airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator`
    Transfers a BigQuery table to a Google Cloud Storage bucket


They also use :class:`airflow.contrib.hooks.bigquery_hook.BigQueryHook` to communicate with Google Cloud Platform.


Cloud Spanner
'''''''''''''

:class:`airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator`
    deletes an existing database from a Google Cloud Spanner instance or returns success if the database is missing.

:class:`airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator`
    creates a new database in a Google Cloud instance or returns success if the database already exists.

:class:`airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator`
    executes an arbitrary DML query (INSERT, UPDATE, DELETE).

:class:`airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator`
    updates the structure of a Google Cloud Spanner database.

:class:`airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator`
    deletes a Google Cloud Spanner instance.

:class:`airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeployOperator`
    creates a new Google Cloud Spanner instance, or if an instance with the same name exists, updates the instance.


They also use :class:`airflow.contrib.hooks.gcp_spanner_hook.CloudSpannerHook` to communicate with Google Cloud Platform.


Cloud SQL
'''''''''

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceCreateOperator`
    create a new Cloud SQL instance.

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseCreateOperator`
    creates a new database inside a Cloud SQL instance.

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseDeleteOperator`
    deletes a database from a Cloud SQL instance.

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabasePatchOperator`
    updates a database inside a Cloud SQL instance.

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDeleteOperator`
    delete a Cloud SQL instance.

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceExportOperator`
    exports data from a Cloud SQL instance.

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceImportOperator`
    imports data into a Cloud SQL instance.

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlInstancePatchOperator`
    patch a Cloud SQL instance.

:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlQueryOperator`
    run query in a Cloud SQL instance.


They also use :class:`airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook` and :class:`airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook` to communicate with Google Cloud Platform.


Cloud Bigtable
''''''''''''''

:class:`airflow.contrib.operators.gcp_bigtable_operator.BigtableClusterUpdateOperator`
    updates the number of nodes in a Google Cloud Bigtable cluster.

:class:`airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceCreateOperator`
    creates a Cloud Bigtable instance.

:class:`airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceDeleteOperator`
    deletes a Google Cloud Bigtable instance.

:class:`airflow.contrib.operators.gcp_bigtable_operator.BigtableTableCreateOperator`
    creates a table in a Google Cloud Bigtable instance.

:class:`airflow.contrib.operators.gcp_bigtable_operator.BigtableTableDeleteOperator`
    deletes a table in a Google Cloud Bigtable instance.

:class:`airflow.contrib.operators.gcp_bigtable_operator.BigtableTableWaitForReplicationSensor`
    (sensor) waits for a table to be fully replicated.


They also use :class:`airflow.contrib.hooks.gcp_bigtable_hook.BigtableHook` to communicate with Google Cloud Platform.


Compute Engine
''''''''''''''

:class:`airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator`
    start an existing Google Compute Engine instance.

:class:`airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator`
    stop an existing Google Compute Engine instance.

:class:`airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator`
    change the machine type for a stopped instance.

:class:`airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator`
    copy the Instance Template, applying specified changes.

:class:`airflow.contrib.operators.gcp_compute_operator.GceInstanceGroupManagerUpdateTemplateOperator`
    patch the Instance Group Manager, replacing source Instance Template URL with the destination one.


The operators have the common base operator :class:`airflow.contrib.operators.gcp_compute_operator.GceBaseOperator`

They also use :class:`airflow.contrib.hooks.gcp_compute_hook.GceHook` to communicate with Google Cloud Platform.


Cloud Functions
'''''''''''''''

:class:`airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator`
    deploy Google Cloud Function to Google Cloud Platform

:class:`airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator`
    delete Google Cloud Function in Google Cloud Platform


They also use :class:`airflow.contrib.hooks.gcp_function_hook.GcfHook` to communicate with Google Cloud Platform.


Cloud DataFlow
''''''''''''''

:class:`airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator`
    launching Cloud Dataflow jobs written in Java.

:class:`airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator`
    launching a templated Cloud DataFlow batch job.

:class:`airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator`
    launching Cloud Dataflow jobs written in python.


They also use :class:`airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook` to communicate with Google Cloud Platform.


Cloud DataProc
''''''''''''''

:class:`airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator`
    Create a new cluster on Google Cloud Dataproc.

:class:`airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator`
    Delete a cluster on Google Cloud Dataproc.

:class:`airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator`
    Scale up or down a cluster on Google Cloud Dataproc.

:class:`airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator`
    Start a Hadoop Job on a Cloud DataProc cluster.

:class:`airflow.contrib.operators.dataproc_operator.DataProcHiveOperator`
    Start a Hive query Job on a Cloud DataProc cluster.

:class:`airflow.contrib.operators.dataproc_operator.DataProcPigOperator`
    Start a Pig query Job on a Cloud DataProc cluster.

:class:`airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator`
    Start a PySpark Job on a Cloud DataProc cluster.

:class:`airflow.contrib.operators.dataproc_operator.DataProcSparkOperator`
    Start a Spark Job on a Cloud DataProc cluster.

:class:`airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator`
    Start a Spark SQL query Job on a Cloud DataProc cluster.

:class:`airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateInlineOperator`
    Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc.

:class:`airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateOperator`
    Instantiate a WorkflowTemplate on Google Cloud Dataproc.


Cloud Datastore
'''''''''''''''

:class:`airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator`
    Export entities from Google Cloud Datastore to Cloud Storage.

:class:`airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator`
    Import entities from Cloud Storage to Google Cloud Datastore.


They also use :class:`airflow.contrib.hooks.datastore_hook.DatastoreHook` to communicate with Google Cloud Platform.


Cloud ML Engine
'''''''''''''''

:class:`airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator`
    Start a Cloud ML Engine batch prediction job.

:class:`airflow.contrib.operators.mlengine_operator.MLEngineModelOperator`
    Manages a Cloud ML Engine model.

:class:`airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator`
    Start a Cloud ML Engine training job.

:class:`airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator`
    Manages a Cloud ML Engine model version.


They also use :class:`airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook` to communicate with Google Cloud Platform.


Cloud Storage
'''''''''''''

:class:`airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator`
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

:class:`airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator`
    Loads files from Google cloud storage into BigQuery.

:class:`airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator`
    Copies objects from a bucket to another, with renaming if requested.

:class:`airflow.contrib.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator`
    Copy data from any MySQL Database to Google cloud storage in JSON format.

:class:`airflow.contrib.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator`
    Copy data from any Microsoft SQL Server Database to Google Cloud Storage in JSON format.


They also use :class:`airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook` to communicate with Google Cloud Platform.


Transfer Service
''''''''''''''''

:class:`airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobDeleteOperator`
    Deletes a transfer job.
:class:`airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobCreateOperator`
    Creates a transfer job.
:class:`airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobUpdateOperator`
    Updates a transfer job.
:class:`airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationCancelOperator`
    Cancels a transfer operation.
:class:`airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationGetOperator`
    Gets a transfer operation.
:class:`airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationPauseOperator`
    Pauses a transfer operation
:class:`airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationResumeOperator`
    Resumes a transfer operation.
:class:`airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationsListOperator`
    Gets a list of transfer operations.
:class:`airflow.contrib.operators.gcp_transfer_operator.GoogleCloudStorageToGoogleCloudStorageTransferOperator`
    Copies objects from a Google Cloud Storage bucket to another bucket.
:class:`airflow.contrib.operators.gcp_transfer_operator.S3ToGoogleCloudStorageTransferOperator`
    Synchronizes an S3 bucket with a Google Cloud Storage bucket.


:class:`airflow.contrib.sensors.gcp_transfer_operator.GCPTransferServiceWaitForJobStatusSensor`
    Waits for at least one operation belonging to the job to have the
    expected status.


They also use :class:`airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook` to communicate with Google Cloud Platform.


Cloud Vision
''''''''''''

Cloud Vision Product Search Operators
"""""""""""""""""""""""""""""""""""""

:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionAddProductToProductSetOperator`
    Adds a Product to the specified ProductSet.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionAnnotateImageOperator`
    Run image detection and annotation for an image.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionProductCreateOperator`
    Creates a new Product resource.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionProductDeleteOperator`
    Permanently deletes a product and its reference images.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionProductGetOperator`
    Gets information associated with a Product.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetCreateOperator`
    Creates a new ProductSet resource.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetDeleteOperator`
    Permanently deletes a ProductSet.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetGetOperator`
    Gets information associated with a ProductSet.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetUpdateOperator`
    Makes changes to a ProductSet resource.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionProductUpdateOperator`
    Makes changes to a Product resource.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionReferenceImageCreateOperator`
    Creates a new ReferenceImage resource.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionRemoveProductFromProductSetOperator`
    Removes a Product from the specified ProductSet.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionAnnotateImageOperator`
    Run image detection and annotation for an image.
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectTextOperator`
    Run text detection for an image
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectDocumentTextOperator`
    Run document text detection for an image
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageLabelsOperator`
    Run image labels detection for an image
:class:`airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageSafeSearchOperator`
    Run safe search detection for an image

They also use :class:`airflow.contrib.hooks.gcp_vision_hook.CloudVisionHook` to communicate with Google Cloud Platform.

Cloud Text to Speech
''''''''''''''''''''

:class:`airflow.contrib.operators.gcp_text_to_speech_operator.GcpTextToSpeechSynthesizeOperator`
    Synthesizes input text into audio file and stores this file to GCS.

They also use :class:`airflow.contrib.hooks.gcp_text_to_speech_hook.GCPTextToSpeechHook` to communicate with Google Cloud Platform.

Cloud Speech to Text
''''''''''''''''''''

:class:`airflow.contrib.operators.gcp_speech_to_text_operator.GcpSpeechToTextRecognizeSpeechOperator`
    Recognizes speech in audio input and returns text.

They also use :class:`airflow.contrib.hooks.gcp_speech_to_text_hook.GCPSpeechToTextHook` to communicate with Google Cloud Platform.


Cloud Translate
'''''''''''''''

Cloud Translate Text Operators
""""""""""""""""""""""""""""""

:class:`airflow.contrib.operators.gcp_translate_operator.CloudTranslateTextOperator`
    Translate a string or list of strings.


Cloud Video Intelligence
''''''''''''''''''''''''

:class:`airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoLabelsOperator`
    Performs video annotation, annotating video labels.
:class:`airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoExplicitContentOperator`
    Performs video annotation, annotating explicit content.
:class:`airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoShotsOperator`
    Performs video annotation, annotating video shots.

They also use :class:`airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook` to communicate with Google Cloud Platform.

Google Kubernetes Engine
''''''''''''''''''''''''

:class:`airflow.contrib.operators.gcp_container_operator.GKEClusterCreateOperator`
    Creates a Kubernetes Cluster in Google Cloud Platform

:class:`airflow.contrib.operators.gcp_container_operator.GKEClusterDeleteOperator`
    Deletes a Kubernetes Cluster in Google Cloud Platform

:class:`airflow.contrib.operators.gcp_container_operator.GKEPodOperator`
    Executes a task in a Kubernetes pod in the specified Google Kubernetes Engine cluster

They also use :class:`airflow.contrib.hooks.gcp_container_hook.GKEClusterHook` to communicate with Google Cloud Platform.


Google Natural Language
'''''''''''''''''''''''

:class:`airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageAnalyzeEntities`
    Finds named entities (currently proper names and common nouns) in the text along with entity types,
    salience, mentions for each entity, and other properties.

:class:`airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageAnalyzeEntitySentiment`
    Finds entities, similar to AnalyzeEntities in the text and analyzes sentiment associated with each
    entity and its mentions.

:class:`airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageAnalyzeSentiment`
    Analyzes the sentiment of the provided text.

:class:`airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageClassifyTextOperator`
    Classifies a document into categories.

They also use :class:`airflow.contrib.hooks.gcp_natural_language_operator.CloudNaturalLanguageHook` to communicate with Google Cloud Platform.


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
