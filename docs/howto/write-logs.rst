Writing Logs
============

Writing Logs Locally
--------------------

Users can specify a logs folder in ``airflow.cfg`` using the
``base_log_folder`` setting. By default, it is in the ``AIRFLOW_HOME``
directory.

In addition, users can supply a remote location for storing logs and log
backups in cloud storage.

In the Airflow Web UI, local logs take precedence over remote logs. If local logs
can not be found or accessed, the remote logs will be displayed. Note that logs
are only sent to remote storage once a task completes (including failure). In other
words, remote logs for running tasks are unavailable. Logs are stored in the log
folder as ``{dag_id}/{task_id}/{execution_date}/{try_number}.log``.

.. _write-logs-amazon:

Writing Logs to Amazon S3
-------------------------

Before you begin
''''''''''''''''

Remote logging uses an existing Airflow connection to read/write logs. If you
don't have a connection properly setup, this will fail.

Enabling remote logging
'''''''''''''''''''''''

To enable this feature, ``airflow.cfg`` must be configured as in this
example:

.. code-block:: bash

    [core]
    # Airflow can store logs remotely in AWS S3. Users must supply a remote
    # location URL (starting with either 's3://...') and an Airflow connection
    # id that provides access to the storage location.
    remote_base_log_folder = s3://my-bucket/path/to/logs
    remote_log_conn_id = MyS3Conn
    # Use server-side encryption for logs stored in S3
    encrypt_s3_logs = False

In the above example, Airflow will try to use ``S3Hook('MyS3Conn')``.

.. _write-logs-azure:

Writing Logs to Azure Blob Storage
----------------------------------

Airflow can be configured to read and write task logs in Azure Blob Storage.
Follow the steps below to enable Azure Blob Storage logging.

#. Airflow's logging system requires a custom .py file to be located in the ``PYTHONPATH``, so that it's importable from Airflow. Start by creating a directory to store the config file. ``$AIRFLOW_HOME/config`` is recommended.
#. Create empty files called ``$AIRFLOW_HOME/config/log_config.py`` and ``$AIRFLOW_HOME/config/__init__.py``.
#. Copy the contents of ``airflow/config_templates/airflow_local_settings.py`` into the ``log_config.py`` file that was just created in the step above.
#. Customize the following portions of the template:

    .. code-block:: bash

        # wasb buckets should start with "wasb" just to help Airflow select correct handler
        REMOTE_BASE_LOG_FOLDER = 'wasb-<whatever you want here>'

        # Rename DEFAULT_LOGGING_CONFIG to LOGGING CONFIG
        LOGGING_CONFIG = ...


#. Make sure a Azure Blob Storage (Wasb) connection hook has been defined in Airflow. The hook should have read and write access to the Azure Blob Storage bucket defined above in ``REMOTE_BASE_LOG_FOLDER``.

#. Update ``$AIRFLOW_HOME/airflow.cfg`` to contain:

    .. code-block:: bash

        remote_logging = True
        logging_config_class = log_config.LOGGING_CONFIG
        remote_log_conn_id = <name of the Azure Blob Storage connection>

#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the bucket you've defined.

.. _write-logs-gcp:

Writing Logs to Google Cloud Storage
------------------------------------

Follow the steps below to enable Google Cloud Storage logging.

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

        # Update the airflow.task and airflow.task_runner blocks to be 'gcs.task' instead of 'file.task'.
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

#. Make sure a Google Cloud Platform connection hook has been defined in Airflow. The hook should have read and write access to the Google Cloud Storage bucket defined above in ``GCS_LOG_FOLDER``.

#. Update ``$AIRFLOW_HOME/airflow.cfg`` to contain:

    .. code-block:: bash

        task_log_reader = gcs.task
        logging_config_class = log_config.LOGGING_CONFIG
        remote_log_conn_id = <name of the Google cloud platform hook>

#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the bucket you've defined.
#. Verify that the Google Cloud Storage viewer is working in the UI. Pull up a newly executed task, and verify that you see something like:

    .. code-block:: bash

        *** Reading remote log from gs://<bucket where logs should be persisted>/example_bash_operator/run_this_last/2017-10-03T00:00:00/16.log.
        [2017-10-03 21:57:50,056] {cli.py:377} INFO - Running on host chrisr-00532
        [2017-10-03 21:57:50,093] {base_task_runner.py:115} INFO - Running: ['bash', '-c', u'airflow run example_bash_operator run_this_last 2017-10-03T00:00:00 --job_id 47 --raw -sd DAGS_FOLDER/example_dags/example_bash_operator.py']
        [2017-10-03 21:57:51,264] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,263] {__init__.py:45} INFO - Using executor SequentialExecutor
        [2017-10-03 21:57:51,306] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,306] {models.py:186} INFO - Filling up the DagBag from /airflow/dags/example_dags/example_bash_operator.py

Note the top line that says it's reading from the remote log file.

Please be aware that if you were persisting logs to Google Cloud Storage
using the old-style airflow.cfg configuration method, the old logs will no
longer be visible in the Airflow UI, though they'll still exist in Google
Cloud Storage. This is a backwards incompatbile change. If you are unhappy
with it, you can change the ``FILENAME_TEMPLATE`` to reflect the old-style
log filename format.
