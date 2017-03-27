Integration
===========

- :ref:`Azure`
- :ref:`AWS`
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

AWS: Amazon Webservices
-----------------------

---

.. _Databricks:

Databricks
--------------------------
`Databricks <https://databricks.com/>`_ has contributed an Airflow operator which enables
submitting runs to the Databricks platform. Internally the operator talks to the
``api/2.0/jobs/runs/submit`` `endpoint <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_.

DatabricksSubmitRunOperator
''''''''''''''''''''''''''''

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
^^^^^^^^^^^^^^^^^^

- :ref:`BigQueryCheckOperator` : Performs checks against a SQL query that will return a single row with different values.
- :ref:`BigQueryValueCheckOperator` : Performs a simple value check using SQL code.
- :ref:`BigQueryIntervalCheckOperator` : Checks that the values of metrics given as SQL expressions are within a certain tolerance of the ones from days_back before.
- :ref:`BigQueryOperator` : Executes BigQuery SQL queries in a specific BigQuery database.
- :ref:`BigQueryToBigQueryOperator` : Copy a BigQuery table to another BigQuery table.
- :ref:`BigQueryToCloudStorageOperator` : Transfers a BigQuery table to a Google Cloud Storage bucket


.. _BigQueryCheckOperator:

BigQueryCheckOperator
"""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator

.. _BigQueryValueCheckOperator:

BigQueryValueCheckOperator
""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator

.. _BigQueryIntervalCheckOperator:

BigQueryIntervalCheckOperator
"""""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator

.. _BigQueryOperator:

BigQueryOperator
""""""""""""""""

.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryOperator

.. _BigQueryToBigQueryOperator:

BigQueryToBigQueryOperator
""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator

.. _BigQueryToCloudStorageOperator:

BigQueryToCloudStorageOperator
""""""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator


BigQueryHook
^^^^^^^^^^^^

.. autoclass:: airflow.contrib.hooks.bigquery_hook.BigQueryHook
    :members:


Cloud DataFlow
''''''''''''''

DataFlow Operators
^^^^^^^^^^^^^^^^^^

- :ref:`DataFlowJavaOperator` :

.. _DataFlowJavaOperator:

DataFlowJavaOperator
""""""""""""""""""""

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

DataFlowHook
^^^^^^^^^^^^

.. autoclass:: airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook
    :members:



Cloud DataProc
''''''''''''''

DataProc Operators
^^^^^^^^^^^^^^^^^^

- :ref:`DataProcPigOperator` : Start a Pig query Job on a Cloud DataProc cluster.
- :ref:`DataProcHiveOperator` : Start a Hive query Job on a Cloud DataProc cluster.
- :ref:`DataProcSparkSqlOperator` : Start a Spark SQL query Job on a Cloud DataProc cluster.
- :ref:`DataProcSparkOperator` : Start a Spark Job on a Cloud DataProc cluster.
- :ref:`DataProcHadoopOperator` : Start a Hadoop Job on a Cloud DataProc cluster.
- :ref:`DataProcPySparkOperator` : Start a PySpark Job on a Cloud DataProc cluster.

.. _DataProcPigOperator:

DataProcPigOperator
"""""""""""""""""""

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcPigOperator

.. _DataProcHiveOperator:

DataProcHiveOperator
""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcHiveOperator

.. _DataProcSparkSqlOperator:

DataProcSparkSqlOperator
""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator

.. _DataProcSparkOperator:

DataProcSparkOperator
"""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcSparkOperator

.. _DataProcHadoopOperator:

DataProcHadoopOperator
""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator

.. _DataProcPySparkOperator:

DataProcPySparkOperator
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator
    :members:


Cloud Datastore
'''''''''''''''

Datastore Operators
^^^^^^^^^^^^^^^^^^^

DatastoreHook
~~~~~~~~~~~~~

.. autoclass:: airflow.contrib.hooks.datastore_hook.DatastoreHook
    :members:



Cloud Storage
'''''''''''''

Storage Operators
^^^^^^^^^^^^^^^^^

- :ref:`GoogleCloudStorageDownloadOperator` : Downloads a file from Google Cloud Storage.
- :ref:`GoogleCloudStorageToBigQueryOperator` : Loads files from Google cloud storage into BigQuery.

.. _GoogleCloudStorageDownloadOperator:

GoogleCloudStorageDownloadOperator
""""""""""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator
    :members:

.. _GoogleCloudStorageToBigQueryOperator:

GoogleCloudStorageToBigQueryOperator
""""""""""""""""""""""""""""""""""""

.. autoclass:: airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator
    :members:


GoogleCloudStorageHook
^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook
    :members:

