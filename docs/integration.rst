Integration
===========

- :ref:`ReverseProxy`
- :ref:`Azure`
- :ref:`AWS`
- :ref:`Databricks`
- :ref:`GCP`

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


AWS Batch Service
''''''''''''''''''''''''''

- :ref:`AWSBatchOperator` : Execute a task on AWS Batch Service.

.. _AWSBatchOperator:

AWSBatchOperator
""""""""""""

.. autoclass:: airflow.contrib.operators.awsbatch_operator.AWSBatchOperator


AWS RedShift
'''''''''''''

- :ref:`AwsRedshiftClusterSensor` : Waits for a Redshift cluster to reach a specific status.
- :ref:`RedshiftHook` : Interact with AWS Redshift, using the boto3 library.
- :ref:`RedshiftToS3Transfer` : Executes an unload command to S3 as a CSV with headers.

.. _AwsRedshiftClusterSensor:

AwsRedshiftClusterSensor
"""""""""""""""""""""""""

.. autoclass:: airflow.contrib.sensors.aws_redshift_cluster_sensor.AwsRedshiftClusterSensor

.. _RedshiftHook:

RedshiftHook
"""""""""""""

.. autoclass:: airflow.contrib.hooks.redshift_hook.RedshiftHook

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

Logging
'''''''

Airflow can be configured to read and write task logs in Google cloud storage.
Follow the steps below to enable Google cloud storage logging.

#. Airflow's logging system requires a custom .py file to be located in the ``PYTHONPATH``, so that it's importable from Airflow. Start by creating a directory to store the config file. ``$AIRFLOW_HOME/config`` is recommended.
#. Create empty files called ``$AIRFLOW_HOME/config/log_config.py`` and ``$AIRFLOW_HOME/config/__init__.py``.
#. Copy the contents of ``airflow/config_templates/airflow_local_settings.py`` into the ``log_config.py`` file that was just created in the step above.
#. Customize the following portions of the template:

    .. code-block:: bash

        # Add this variable to the top of the file. Note the trailing slash.
        GCS_LOG_FOLDER = 'gs://<bucket where logs should be persisted>/'

        # Rename DEFAULT_LOGGING_CONFIG to LOGGING CONFIG
        LOGGING_CONFIG = ...

        # Add a GCSTaskHandler to the 'handlers' block of the LOGGING_CONFIG variable
        'gcs.task': {
            'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'gcs_log_folder': GCS_LOG_FOLDER,
            'filename_template': FILENAME_TEMPLATE,
        },

        # Update the airflow.task and airflow.tas_runner blocks to be 'gcs.task' instead of 'file.task'.
        'loggers': {
            'airflow.task': {
                'handlers': ['gcs.task'],
                ...
            },
            'airflow.task_runner': {
                'handlers': ['gcs.task'],
                ...
            },
            'airflow': {
                'handlers': ['console'],
                ...
            },
        }

#. Make sure a Google cloud platform connection hook has been defined in Airflow. The hook should have read and write access to the Google cloud storage bucket defined above in ``GCS_LOG_FOLDER``.

#. Update ``$AIRFLOW_HOME/airflow.cfg`` to contain:

    .. code-block:: bash

        task_log_reader = gcs.task
        logging_config_class = log_config.LOGGING_CONFIG
        remote_log_conn_id = <name of the Google cloud platform hook>

#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the bucket you've defined.
#. Verify that the Google cloud storage viewer is working in the UI. Pull up a newly executed task, and verify that you see something like:

    .. code-block:: bash

        *** Reading remote log from gs://<bucket where logs should be persisted>/example_bash_operator/run_this_last/2017-10-03T00:00:00/16.log.
        [2017-10-03 21:57:50,056] {cli.py:377} INFO - Running on host chrisr-00532
        [2017-10-03 21:57:50,093] {base_task_runner.py:115} INFO - Running: ['bash', '-c', u'airflow run example_bash_operator run_this_last 2017-10-03T00:00:00 --job_id 47 --raw -sd DAGS_FOLDER/example_dags/example_bash_operator.py']
        [2017-10-03 21:57:51,264] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,263] {__init__.py:45} INFO - Using executor SequentialExecutor
        [2017-10-03 21:57:51,306] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,306] {models.py:186} INFO - Filling up the DagBag from /airflow/dags/example_dags/example_bash_operator.py

Note the top line that says it's reading from the remote log file.

Please be aware that if you were persisting logs to Google cloud storage using the old-style airflow.cfg configuration method, the old logs will no longer be visible in the Airflow UI, though they'll still exist in Google cloud storage. This is a backwards incompatbile change. If you are unhappy with it, you can change the ``FILENAME_TEMPLATE`` to reflect the old-style log filename format.

BigQuery
''''''''

BigQuery Operators
""""""""""""""""""

- :ref:`BigQueryCheckOperator` : Performs checks against a SQL query that will return a single row with different values.
- :ref:`BigQueryValueCheckOperator` : Performs a simple value check using SQL code.
- :ref:`BigQueryIntervalCheckOperator` : Checks that the values of metrics given as SQL expressions are within a certain tolerance of the ones from days_back before.
- :ref:`BigQueryCreateEmptyTableOperator` : Creates a new, empty table in the specified BigQuery dataset optionally with schema.
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

- :ref:`FileToGoogleCloudStorageOperator` : Uploads a file to Google Cloud Storage.
- :ref:`GoogleCloudStorageCopyOperator` : Copies objects (optionally from a directory) filtered by 'delimiter' (file extension for e.g .json) from a bucket to another bucket in a different directory, if required.
- :ref:`GoogleCloudStorageListOperator` : List all objects from the bucket with the give string prefix and delimiter in name.
- :ref:`GoogleCloudStorageDownloadOperator` : Downloads a file from Google Cloud Storage.
- :ref:`GoogleCloudStorageToBigQueryOperator` : Loads files from Google cloud storage into BigQuery.
- :ref:`GoogleCloudStorageToGoogleCloudStorageOperator` : Copies a single object from a bucket to another, with renaming if requested.

.. _FileToGoogleCloudStorageOperator:

FileToGoogleCloudStorageOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator

.. _GoogleCloudStorageCopyOperator:

GoogleCloudStorageCopyOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.gcs_copy_operator.GoogleCloudStorageCopyOperator

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
