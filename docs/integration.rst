Integration
===========

- :ref:`Azure`
- :ref:`AWS`
- :ref:`Databricks`
- :ref:`GCP`


.. _Azure:

Azure: Microsoft Azure
----------------------

Airflow has limited support for Microsoft Azure: interfaces exist only for Azure Blob
Storage. Note that the Hook, Sensor and Operator are in the contrib section.

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



.. _AWS:

AWS: Amazon Web Services
-----------------------

Airflow has extensive support for Amazon Web Services. But note that the Hooks, Sensors and
Operators are in the contrib section.

AWS EMR
''''''''

- :ref:`EmrAddStepsOperator` : Adds steps to an existing EMR JobFlow.
- :ref:`EmrCreateJobFlowOperator` : Creates an EMR JobFlow, reading the config from the EMR connection.
- :ref:`EmrTerminateJobFlowOperator` : Terminates an EMR JobFlow.
- :ref:`EmrHook` : Interact with AWS EMR.

.. _EmrAddStepsOperator:

EmrAddStepsOperator
""""""""

.. autoclass:: airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator

.. _EmrCreateJobFlowOperator:

EmrCreateJobFlowOperator
""""""""

.. autoclass:: airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator

.. _EmrTerminateJobFlowOperator:

EmrTerminateJobFlowOperator
""""""""

.. autoclass:: airflow.contrib.operators.emr_terminate_job_flow_operator.EmrTerminateJobFlowOperator

.. _EmrHook:

EmrHook
""""""""

.. autoclass:: airflow.contrib.hooks.emr_hook.EmrHook


AWS S3
'''''''

- :ref:`S3FileTransformOperator` : Copies data from a source S3 location to a temporary location on the local filesystem.
- :ref:`S3ToHiveTransfer` : Moves data from S3 to Hive. The operator downloads a file from S3, stores the file locally before loading it into a Hive table.
- :ref:`S3Hook` : Interact with AWS S3.

.. _S3FileTransformOperator:

S3FileTransformOperator
""""""""""""""""""""""""

.. autoclass:: airflow.operators.s3_file_transform_operator.S3FileTransformOperator

.. _S3ToHiveTransfer:

S3ToHiveTransfer
"""""""""""""""""

.. autoclass:: airflow.operators.s3_to_hive_operator.S3ToHiveTransfer

.. _S3Hook:

S3Hook
"""""""

.. autoclass:: airflow.hooks.S3_hook.S3Hook


AWS EC2 Container Service
''''''''''''''''''''''''''

- :ref:`ECSOperator` : Execute a task on AWS EC2 Container Service.

.. _ECSOperator:

ECSOperator
""""""""""""

.. autoclass:: airflow.contrib.operators.ecs_operator.ECSOperator


AWS RedShift
'''''''''''''

- :ref:`RedshiftToS3Transfer` : Executes an unload command to S3 as a CSV with headers.

.. _RedshiftToS3Transfer:

RedshiftToS3Transfer
"""""""""""""""""""""

.. autoclass:: airflow.operators.redshift_to_s3_operator.RedshiftToS3Transfer



.. _Databricks:

Databricks
----------

`Databricks <https://databricks.com/>`_ has contributed an Airflow operator which enables
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

BigQuery
''''''''

BigQuery Operators
""""""""""""""""""

- :ref:`BigQueryCheckOperator` : Performs checks against a SQL query that will return a single row with different values.
- :ref:`BigQueryValueCheckOperator` : Performs a simple value check using SQL code.
- :ref:`BigQueryIntervalCheckOperator` : Checks that the values of metrics given as SQL expressions are within a certain tolerance of the ones from days_back before.
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

.. _BigQueryOperator:

BigQueryOperator
^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryOperator

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


Cloud DataFlow
''''''''''''''

DataFlow Operators
""""""""""""""""""

- :ref:`DataFlowJavaOperator` : launching Cloud Dataflow jobs written in Java.
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

- :ref:`DataProcPigOperator` : Start a Pig query Job on a Cloud DataProc cluster.
- :ref:`DataProcHiveOperator` : Start a Hive query Job on a Cloud DataProc cluster.
- :ref:`DataProcSparkSqlOperator` : Start a Spark SQL query Job on a Cloud DataProc cluster.
- :ref:`DataProcSparkOperator` : Start a Spark Job on a Cloud DataProc cluster.
- :ref:`DataProcHadoopOperator` : Start a Hadoop Job on a Cloud DataProc cluster.
- :ref:`DataProcPySparkOperator` : Start a PySpark Job on a Cloud DataProc cluster.

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
    :members:


Cloud Datastore
'''''''''''''''

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator
    :members:

.. _MLEngineModelOperator:

MLEngineModelOperator
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineModelOperator
    :members:

.. _MLEngineTrainingOperator:

MLEngineTrainingOperator
^^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^

.. autoclass:: airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook
    :members:


Cloud Storage
'''''''''''''

Storage Operators
"""""""""""""""""

- :ref:`GoogleCloudStorageDownloadOperator` : Downloads a file from Google Cloud Storage.
- :ref:`GoogleCloudStorageToBigQueryOperator` : Loads files from Google cloud storage into BigQuery.

.. _GoogleCloudStorageDownloadOperator:

GoogleCloudStorageDownloadOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator
    :members:

.. _GoogleCloudStorageToBigQueryOperator:

GoogleCloudStorageToBigQueryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator
    :members:


GoogleCloudStorageHook
""""""""""""""""""""""

.. autoclass:: airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook
    :members:
