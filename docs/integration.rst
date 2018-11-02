Integration
===========

- :ref:`ReverseProxy`
- :ref:`Azure`
- :ref:`AWS`
- :ref:`Databricks`
- :ref:`GCP`
- :ref:`Qubole`

.. _ReverseProxy:

Reverse Proxy
-------------

Airflow can be set up behind a reverse proxy, with the ability to set its endpoint with great
flexibility.

For example, you can configure your reverse proxy to get:

::

    https://lab.mycompany.com/myorg/airflow/

To do so, you need to set the following setting in your `airflow.cfg`::

    base_url = http://my_host/myorg/airflow

Additionally if you use Celery Executor, you can get Flower in `/myorg/flower` with::

    flower_url_prefix = /myorg/flower

Your reverse proxy (ex: nginx) should be configured as follow:

- pass the url and http header as it for the Airflow webserver, without any rewrite, for example::

      server {
        listen 80;
        server_name lab.mycompany.com;

        location /myorg/airflow/ {
            proxy_pass http://localhost:8080;
            proxy_set_header Host $host;
            proxy_redirect off;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
      }

- rewrite the url for the flower endpoint::

      server {
          listen 80;
          server_name lab.mycompany.com;

          location /myorg/flower/ {
              rewrite ^/myorg/flower/(.*)$ /$1 break;  # remove prefix from http header
              proxy_pass http://localhost:5555;
              proxy_set_header Host $host;
              proxy_redirect off;
              proxy_http_version 1.1;
              proxy_set_header Upgrade $http_upgrade;
              proxy_set_header Connection "upgrade";
          }
      }

To ensure that Airflow generates URLs with the correct scheme when
running behind a TLS-terminating proxy, you should configure the proxy
to set the `X-Forwarded-Proto` header, and enable the `ProxyFix`
middleware in your `airflow.cfg`::

    enable_proxy_fix = True

Note: you should only enable the `ProxyFix` middleware when running
Airflow behind a trusted proxy (AWS ELB, nginx, etc.).

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

- :ref:`WasbBlobSensor`: Checks if a blob is present on Azure Blob storage.
- :ref:`WasbPrefixSensor`: Checks if blobs matching a prefix are present on Azure Blob storage.
- :ref:`FileToWasbOperator`: Uploads a local file to a container as a blob.
- :ref:`WasbHook`: Interface with Azure Blob Storage.

.. _WasbBlobSensor:

WasbBlobSensor
"""""""""""""""

.. autoclass:: airflow.contrib.sensors.wasb_sensor.WasbBlobSensor

.. _WasbPrefixSensor:

WasbPrefixSensor
"""""""""""""""""

.. autoclass:: airflow.contrib.sensors.wasb_sensor.WasbPrefixSensor

.. _FileToWasbOperator:

FileToWasbOperator
"""""""""""""""""""

.. autoclass:: airflow.contrib.operators.file_to_wasb.FileToWasbOperator

.. _WasbHook:

WasbHook
"""""""""

.. autoclass:: airflow.contrib.hooks.wasb_hook.WasbHook

Azure File Share
''''''''''''''''

Cloud variant of a SMB file share. Make sure that a Airflow connection of
type `wasb` exists. Authorization can be done by supplying a login (=Storage account name)
and password (=Storage account key), or login and SAS token in the extra field
(see connection `wasb_default` for an example).

AzureFileShareHook
""""""""""""""""""

.. autoclass:: airflow.contrib.hooks.azure_fileshare_hook.AzureFileShareHook

Logging
'''''''

Airflow can be configured to read and write task logs in Azure Blob Storage.
See :ref:`write-logs-azure`.

Azure Data Lake
'''''''''''''''

AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make sure that a
Airflow connection of type `azure_data_lake` exists. Authorization can be done by supplying a
login (=Client ID), password (=Client Secret) and extra fields tenant (Tenant) and account_name (Account Name)
 (see connection `azure_data_lake_default` for an example).

- :ref:`AzureDataLakeHook`: Interface with Azure Data Lake.
- :ref:`AzureDataLakeStorageListOperator`: Lists the files located in a specified Azure Data Lake path.

.. _AzureDataLakeHook:

AzureDataLakeHook
"""""""""""""""""

.. autoclass:: airflow.contrib.hooks.azure_data_lake_hook.AzureDataLakeHook

.. _AzureDataLakeStorageListOperator:

AzureDataLakeStorageListOperator
""""""""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.adls_list_operator.AzureDataLakeStorageListOperator

.. _AWS:

AWS: Amazon Web Services
------------------------

Airflow has extensive support for Amazon Web Services. But note that the Hooks, Sensors and
Operators are in the contrib section.

AWS EMR
'''''''

- :ref:`EmrAddStepsOperator` : Adds steps to an existing EMR JobFlow.
- :ref:`EmrCreateJobFlowOperator` : Creates an EMR JobFlow, reading the config from the EMR connection.
- :ref:`EmrTerminateJobFlowOperator` : Terminates an EMR JobFlow.
- :ref:`EmrHook` : Interact with AWS EMR.

.. _EmrAddStepsOperator:

EmrAddStepsOperator
"""""""""""""""""""

.. autoclass:: airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator

.. _EmrCreateJobFlowOperator:

EmrCreateJobFlowOperator
""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator

.. _EmrTerminateJobFlowOperator:

EmrTerminateJobFlowOperator
"""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.emr_terminate_job_flow_operator.EmrTerminateJobFlowOperator

.. _EmrHook:

EmrHook
"""""""

.. autoclass:: airflow.contrib.hooks.emr_hook.EmrHook


AWS S3
''''''

- :ref:`S3Hook` : Interact with AWS S3.
- :ref:`S3FileTransformOperator` : Copies data from a source S3 location to a temporary location on the local filesystem.
- :ref:`S3ListOperator` : Lists the files matching a key prefix from a S3 location.
- :ref:`S3ToGoogleCloudStorageOperator` : Syncs an S3 location with a Google Cloud Storage bucket.
- :ref:`S3ToHiveTransfer` : Moves data from S3 to Hive. The operator downloads a file from S3, stores the file locally before loading it into a Hive table.

.. _S3Hook:

S3Hook
""""""

.. autoclass:: airflow.hooks.S3_hook.S3Hook

.. _S3FileTransformOperator:

S3FileTransformOperator
"""""""""""""""""""""""

.. autoclass:: airflow.operators.s3_file_transform_operator.S3FileTransformOperator

.. _S3ListOperator:

S3ListOperator
""""""""""""""

.. autoclass:: airflow.contrib.operators.s3_list_operator.S3ListOperator

.. _S3ToGoogleCloudStorageOperator:

S3ToGoogleCloudStorageOperator
""""""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.s3_to_gcs_operator.S3ToGoogleCloudStorageOperator

.. _S3ToHiveTransfer:

S3ToHiveTransfer
""""""""""""""""

.. autoclass:: airflow.operators.s3_to_hive_operator.S3ToHiveTransfer


AWS EC2 Container Service
'''''''''''''''''''''''''

- :ref:`ECSOperator` : Execute a task on AWS EC2 Container Service.

.. _ECSOperator:

ECSOperator
"""""""""""

.. autoclass:: airflow.contrib.operators.ecs_operator.ECSOperator


AWS Batch Service
'''''''''''''''''

- :ref:`AWSBatchOperator` : Execute a task on AWS Batch Service.

.. _AWSBatchOperator:

AWSBatchOperator
""""""""""""""""

.. autoclass:: airflow.contrib.operators.awsbatch_operator.AWSBatchOperator


AWS RedShift
''''''''''''

- :ref:`AwsRedshiftClusterSensor` : Waits for a Redshift cluster to reach a specific status.
- :ref:`RedshiftHook` : Interact with AWS Redshift, using the boto3 library.
- :ref:`RedshiftToS3Transfer` : Executes an unload command to S3 as CSV with or without headers.
- :ref:`S3ToRedshiftTransfer` : Executes an copy command from S3 as CSV with or without headers.

.. _AwsRedshiftClusterSensor:

AwsRedshiftClusterSensor
""""""""""""""""""""""""

.. autoclass:: airflow.contrib.sensors.aws_redshift_cluster_sensor.AwsRedshiftClusterSensor

.. _RedshiftHook:

RedshiftHook
""""""""""""

.. autoclass:: airflow.contrib.hooks.redshift_hook.RedshiftHook

.. _RedshiftToS3Transfer:

RedshiftToS3Transfer
""""""""""""""""""""

.. autoclass:: airflow.operators.redshift_to_s3_operator.RedshiftToS3Transfer

.. _S3ToRedshiftTransfer:

S3ToRedshiftTransfer
""""""""""""""""""""

.. autoclass:: airflow.operators.s3_to_redshift_operator.S3ToRedshiftTransfer

AWS DynamoDB
''''''''''''

- :ref:`HiveToDynamoDBTransferOperator` :  Moves data from Hive to DynamoDB.
- :ref:`AwsDynamoDBHook` : Interact with AWS DynamoDB.

.. _HiveToDynamoDBTransferOperator:

HiveToDynamoDBTransferOperator
""""""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.hive_to_dynamodb.HiveToDynamoDBTransferOperator

.. _AwsDynamoDBHook:

AwsDynamoDBHook
"""""""""""""""

.. autoclass:: airflow.contrib.hooks.aws_dynamodb_hook.AwsDynamoDBHook

AWS Lambda
''''''''''

- :ref:`AwsLambdaHook` : Interact with AWS Lambda.

.. _AwsLambdaHook:

AwsLambdaHook
"""""""""""""

.. autoclass:: airflow.contrib.hooks.aws_lambda_hook.AwsLambdaHook

AWS Kinesis
'''''''''''

- :ref:`AwsFirehoseHook` : Interact with AWS Kinesis Firehose.

.. _AwsFirehoseHook:

AwsFirehoseHook
"""""""""""""""

.. autoclass:: airflow.contrib.hooks.aws_firehose_hook.AwsFirehoseHook

.. _Databricks:

Databricks
----------

`Databricks <https://databricks.com/>`__ has contributed an Airflow operator which enables
submitting runs to the Databricks platform. Internally the operator talks to the
``api/2.0/jobs/runs/submit`` `endpoint <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_.

DatabricksSubmitRunOperator
'''''''''''''''''''''''''''

.. autoclass:: airflow.contrib.operators.databricks_operator.DatabricksSubmitRunOperator



.. _GCP:

GCP: Google Cloud Platform
--------------------------

Airflow has extensive support for the Google Cloud Platform. But note that most Hooks and
Operators are in the contrib section. Meaning that they have a *beta* status, meaning that
they can have breaking changes between minor releases.

See the :ref:`GCP connection type <connection-type-GCP>` documentation to
configure connections to GCP.

Logging
'''''''

Airflow can be configured to read and write task logs in Google Cloud Storage.
See :ref:`write-logs-gcp`.

BigQuery
''''''''

BigQuery Operators
""""""""""""""""""

- :ref:`BigQueryCheckOperator` : Performs checks against a SQL query that will return a single row with different values.
- :ref:`BigQueryValueCheckOperator` : Performs a simple value check using SQL code.
- :ref:`BigQueryIntervalCheckOperator` : Checks that the values of metrics given as SQL expressions are within a certain tolerance of the ones from days_back before.
- :ref:`BigQueryGetDataOperator` : Fetches the data from a BigQuery table and returns data in a python list
- :ref:`BigQueryCreateEmptyTableOperator` : Creates a new, empty table in the specified BigQuery dataset optionally with schema.
- :ref:`BigQueryCreateExternalTableOperator` : Creates a new, external table in the dataset with the data in Google Cloud Storage.
- :ref:`BigQueryDeleteDatasetOperator` : Deletes an existing BigQuery dataset.
- :ref:`BigQueryCreateEmptyDatasetOperator` : Creates an empty BigQuery dataset.
- :ref:`BigQueryOperator` : Executes BigQuery SQL queries in a specific BigQuery database.
- :ref:`BigQueryToBigQueryOperator` : Copy a BigQuery table to another BigQuery table.
- :ref:`BigQueryToCloudStorageOperator` : Transfers a BigQuery table to a Google Cloud Storage bucket


.. _BigQueryCheckOperator:

BigQueryCheckOperator
^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator

.. _BigQueryValueCheckOperator:

BigQueryValueCheckOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator

.. _BigQueryIntervalCheckOperator:

BigQueryIntervalCheckOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator

.. _BigQueryGetDataOperator:

BigQueryGetDataOperator
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator

.. _BigQueryCreateEmptyTableOperator:

BigQueryCreateEmptyTableOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyTableOperator

.. _BigQueryCreateExternalTableOperator:

BigQueryCreateExternalTableOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryCreateExternalTableOperator

.. _BigQueryDeleteDatasetOperator:

BigQueryDeleteDatasetOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryDeleteDatasetOperator

.. _BigQueryCreateEmptyDatasetOperator:

BigQueryCreateEmptyDatasetOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyDatasetOperator

.. _BigQueryOperator:

BigQueryOperator
^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryOperator

.. _BigQueryTableDeleteOperator:

BigQueryTableDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator

.. _BigQueryToBigQueryOperator:

BigQueryToBigQueryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator

.. _BigQueryToCloudStorageOperator:

BigQueryToCloudStorageOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator


BigQueryHook
""""""""""""

.. autoclass:: airflow.contrib.hooks.bigquery_hook.BigQueryHook
    :members:

Cloud SQL
'''''''''

Cloud SQL Operators
"""""""""""""""""""

- :ref:`CloudSqlInstanceDatabaseDeleteOperator` : deletes a database from a Cloud SQL
instance.
- :ref:`CloudSqlInstanceDatabaseCreateOperator` : creates a new database inside a Cloud
SQL instance.
- :ref:`CloudSqlInstanceDatabasePatchOperator` : updates a database inside a Cloud
SQL instance.
- :ref:`CloudSqlInstanceDeleteOperator` : delete a Cloud SQL instance.
- :ref:`CloudSqlInstanceCreateOperator` : create a new Cloud SQL instance.
- :ref:`CloudSqlInstancePatchOperator` : patch a Cloud SQL instance.

.. CloudSqlInstanceDatabaseDeleteOperator:

CloudSqlInstanceDatabaseDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseDeleteOperator

.. CloudSqlInstanceDatabaseCreateOperator:

CloudSqlInstanceDatabaseCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseCreateOperator

.. CloudSqlInstanceDatabasePatchOperator:

CloudSqlInstanceDatabasePatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabasePatchOperator

.. CloudSqlInstanceDeleteOperator:

CloudSqlInstanceDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDeleteOperator

.. CloudSqlInstanceCreateOperator:

CloudSqlInstanceCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceCreateOperator

.. CloudSqlInstancePatchOperator:

CloudSqlInstancePatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_sql_operator.CloudSqlInstancePatchOperator

Cloud SQL Hook
""""""""""""""

.. autoclass:: airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook
    :members:

Compute Engine
''''''''''''''

Compute Engine Operators
""""""""""""""""""""""""

- :ref:`GceInstanceStartOperator` : start an existing Google Compute Engine instance.
- :ref:`GceInstanceStopOperator` : stop an existing Google Compute Engine instance.
- :ref:`GceSetMachineTypeOperator` : change the machine type for a stopped instance.

.. _GceInstanceStartOperator:

GceInstanceStartOperator
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator

.. _GceInstanceStopOperator:

GceInstanceStopOperator
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator

.. _GceSetMachineTypeOperator:

GceSetMachineTypeOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator


Cloud Functions
'''''''''''''''

Cloud Functions Operators
"""""""""""""""""""""""""

- :ref:`GcfFunctionDeployOperator` : deploy Google Cloud Function to Google Cloud Platform
- :ref:`GcfFunctionDeleteOperator` : delete Google Cloud Function in Google Cloud Platform

.. autoclass:: airflow.contrib.operators.gcp_operator.GCP

.. _GcfFunctionDeployOperator:

GcfFunctionDeployOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator


.. _GcfFunctionDeleteOperator:

GcfFunctionDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator


Cloud Functions Hook
""""""""""""""""""""

.. autoclass:: airflow.contrib.hooks.gcp_function_hook.GcfHook
    :members:


Cloud DataFlow
''''''''''''''

DataFlow Operators
""""""""""""""""""

- :ref:`DataFlowJavaOperator` : launching Cloud Dataflow jobs written in Java.
- :ref:`DataflowTemplateOperator` : launching a templated Cloud DataFlow batch job.
- :ref:`DataFlowPythonOperator` : launching Cloud Dataflow jobs written in python.

.. _DataFlowJavaOperator:

DataFlowJavaOperator
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator

.. code:: python

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date':
            (2016, 8, 1),
        'email': ['alex@vanboxel.be'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=30),
        'dataflow_default_options': {
            'project': 'my-gcp-project',
            'zone': 'us-central1-f',
            'stagingLocation': 'gs://bucket/tmp/dataflow/staging/',
        }
    }

    dag = DAG('test-dag', default_args=default_args)

    task = DataFlowJavaOperator(
        gcp_conn_id='gcp_default',
        task_id='normalize-cal',
        jar='{{var.value.gcp_dataflow_base}}pipeline-ingress-cal-normalize-1.0.jar',
        options={
            'autoscalingAlgorithm': 'BASIC',
            'maxNumWorkers': '50',
            'start': '{{ds}}',
            'partitionType': 'DAY'

        },
        dag=dag)

.. _DataflowTemplateOperator:

DataflowTemplateOperator
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator

.. _DataFlowPythonOperator:

DataFlowPythonOperator
^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator


DataFlowHook
""""""""""""

.. autoclass:: airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook
    :members:



Cloud DataProc
''''''''''''''

DataProc Operators
""""""""""""""""""

- :ref:`DataprocClusterCreateOperator` : Create a new cluster on Google Cloud Dataproc.
- :ref:`DataprocClusterDeleteOperator` : Delete a cluster on Google Cloud Dataproc.
- :ref:`DataprocClusterScaleOperator` : Scale up or down a cluster on Google Cloud Dataproc.
- :ref:`DataProcPigOperator` : Start a Pig query Job on a Cloud DataProc cluster.
- :ref:`DataProcHiveOperator` : Start a Hive query Job on a Cloud DataProc cluster.
- :ref:`DataProcSparkSqlOperator` : Start a Spark SQL query Job on a Cloud DataProc cluster.
- :ref:`DataProcSparkOperator` : Start a Spark Job on a Cloud DataProc cluster.
- :ref:`DataProcHadoopOperator` : Start a Hadoop Job on a Cloud DataProc cluster.
- :ref:`DataProcPySparkOperator` : Start a PySpark Job on a Cloud DataProc cluster.
- :ref:`DataprocWorkflowTemplateInstantiateOperator` : Instantiate a WorkflowTemplate on Google Cloud Dataproc.
- :ref:`DataprocWorkflowTemplateInstantiateInlineOperator` : Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc.

.. _DataprocClusterCreateOperator:

DataprocClusterCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator

.. _DataprocClusterScaleOperator:

DataprocClusterScaleOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator

.. _DataprocClusterDeleteOperator:

DataprocClusterDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator

.. _DataProcPigOperator:

DataProcPigOperator
^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcPigOperator

.. _DataProcHiveOperator:

DataProcHiveOperator
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcHiveOperator

.. _DataProcSparkSqlOperator:

DataProcSparkSqlOperator
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator

.. _DataProcSparkOperator:

DataProcSparkOperator
^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcSparkOperator

.. _DataProcHadoopOperator:

DataProcHadoopOperator
^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator

.. _DataProcPySparkOperator:

DataProcPySparkOperator
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator

.. _DataprocWorkflowTemplateInstantiateOperator:

DataprocWorkflowTemplateInstantiateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateOperator

.. _DataprocWorkflowTemplateInstantiateInlineOperator:

DataprocWorkflowTemplateInstantiateInlineOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateInlineOperator

Cloud Datastore
'''''''''''''''

Datastore Operators
"""""""""""""""""""

- :ref:`DatastoreExportOperator` : Export entities from Google Cloud Datastore to Cloud Storage.
- :ref:`DatastoreImportOperator` : Import entities from Cloud Storage to Google Cloud Datastore.

.. _DatastoreExportOperator:

DatastoreExportOperator
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator

.. _DatastoreImportOperator:

DatastoreImportOperator
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator

DatastoreHook
"""""""""""""

.. autoclass:: airflow.contrib.hooks.datastore_hook.DatastoreHook
    :members:


Cloud ML Engine
'''''''''''''''

Cloud ML Engine Operators
"""""""""""""""""""""""""

- :ref:`MLEngineBatchPredictionOperator` : Start a Cloud ML Engine batch prediction job.
- :ref:`MLEngineModelOperator` : Manages a Cloud ML Engine model.
- :ref:`MLEngineTrainingOperator` : Start a Cloud ML Engine training job.
- :ref:`MLEngineVersionOperator` : Manages a Cloud ML Engine model version.

.. _MLEngineBatchPredictionOperator:

MLEngineBatchPredictionOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator
    :members:

.. _MLEngineModelOperator:

MLEngineModelOperator
^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineModelOperator
    :members:

.. _MLEngineTrainingOperator:

MLEngineTrainingOperator
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator
    :members:

.. _MLEngineVersionOperator:

MLEngineVersionOperator
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator
    :members:

Cloud ML Engine Hook
""""""""""""""""""""

.. _MLEngineHook:

MLEngineHook
^^^^^^^^^^^^

.. autoclass:: airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook
    :members:


Cloud Storage
'''''''''''''

Storage Operators
"""""""""""""""""

- :ref:`FileToGoogleCloudStorageOperator` : Uploads a file to Google Cloud Storage.
- :ref:`GoogleCloudStorageCreateBucketOperator` : Creates a new cloud storage bucket.
- :ref:`GoogleCloudStorageListOperator` : List all objects from the bucket with the give string prefix and delimiter in name.
- :ref:`GoogleCloudStorageDownloadOperator` : Downloads a file from Google Cloud Storage.
- :ref:`GoogleCloudStorageToBigQueryOperator` : Loads files from Google cloud storage into BigQuery.
- :ref:`GoogleCloudStorageToGoogleCloudStorageOperator` : Copies objects from a bucket to another, with renaming if requested.

.. _FileToGoogleCloudStorageOperator:

FileToGoogleCloudStorageOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator

.. _GoogleCloudStorageCreateBucketOperator:

GoogleCloudStorageCreateBucketOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcs_operator.GoogleCloudStorageCreateBucketOperator

.. _GoogleCloudStorageDownloadOperator:

GoogleCloudStorageDownloadOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator

.. _GoogleCloudStorageListOperator:

GoogleCloudStorageListOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageListOperator

.. _GoogleCloudStorageToBigQueryOperator:

GoogleCloudStorageToBigQueryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator

.. _GoogleCloudStorageToGoogleCloudStorageOperator:

GoogleCloudStorageToGoogleCloudStorageOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator

GoogleCloudStorageHook
""""""""""""""""""""""

.. autoclass:: airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook
    :members:

Google Kubernetes Engine
''''''''''''''''''''''''

Google Kubernetes Engine Cluster Operators
""""""""""""""""""""""""""""""""""""""""""

- :ref:`GKEClusterCreateOperator` : Creates a Kubernetes Cluster in Google Cloud Platform
- :ref:`GKEClusterDeleteOperator` : Deletes a Kubernetes Cluster in Google Cloud Platform

GKEClusterCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_container_operator.GKEClusterCreateOperator
.. _GKEClusterCreateOperator:

GKEClusterDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_container_operator.GKEClusterDeleteOperator
.. _GKEClusterDeleteOperator:

GKEPodOperator
^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcp_container_operator.GKEPodOperator
.. _GKEPodOperator:

Google Kubernetes Engine Hook
"""""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.hooks.gcp_container_hook.GKEClusterHook
    :members:


.. _Qubole:

Qubole
------

Apache Airflow has a native operator and hooks to talk to `Qubole <https://qubole.com/>`__,
which lets you submit your big data jobs directly to Qubole from Apache Airflow.

QuboleOperator
''''''''''''''

.. autoclass:: airflow.contrib.operators.qubole_operator.QuboleOperator

QubolePartitionSensor
'''''''''''''''''''''

.. autoclass:: airflow.contrib.sensors.qubole_sensor.QubolePartitionSensor


QuboleFileSensor
''''''''''''''''

.. autoclass:: airflow.contrib.sensors.qubole_sensor.QuboleFileSensor
