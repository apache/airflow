Default Logging in Apache Airflow
=================================

Apache Airflow has multiple loggers for different components, which can be confusing for new users.
This section explains the default loggers, their purposes, and how to modify their behavior.

Default Loggers
---------------

+------------------------+----------------+----------------------------+--------------------------------+
| Logger Name            | Component      | Output                     | Notes                          |
+========================+================+============================+================================+
| root                   | Webserver      | stdout / webserver log     | Default root logger used by webserver. |
+------------------------+----------------+----------------------------+--------------------------------+
| airflow.task           | Scheduler/Worker | logs/<dag_id>/<task_id>/<execution_date>/<try_number>.log | A new log file is created per task instance and try. Shown in the Web UI. |
+------------------------+----------------+----------------------------+--------------------------------+
| airflow.processor      | Scheduler/Worker | logs/<dag_file_name>.log   | Logs DAG parsing for scheduler and workers. |
+------------------------+----------------+----------------------------+--------------------------------+
| airflow.processor_manager | Scheduler    | logs/<dag_file_name>.log   | Logs task instance execution control. |
+------------------------+----------------+----------------------------+--------------------------------+
| flask_appbuilder       | Webserver      | filters verbose FAB logs  | Typically used for filtering; no config needed by most users. |
+------------------------+----------------+----------------------------+--------------------------------+

Logging by Airflow Component
----------------------------

- **Webserver**: Uses the root logger. Logs to stdout and webserver log file.
- **Worker**: Uses `airflow.task` and `airflow.processor`. Task logs are stored per task instance. DAG parsing logs are stored per DAG file.
- **Scheduler**: Uses `airflow.processor`, `airflow.processor_manager`, and the root logger.

Customizing Logging
-------------------

You can influence the logging configuration using the following methods:

1. **Configuration via airflow.cfg**
   - `[logging]` section allows changing:
     - Base log folder (`base_log_folder`)
     - Remote logging settings
     - Logging format

2. **Custom Python logging configuration**
   - Airflow uses `airflow.utils.log.logging_config.py`
   - You can override `LOGGING_CONFIG` in `airflow_local_settings.py`
   - Example:

   .. code-block:: python

       from airflow.utils.log.logging_config import DEFAULT_LOGGING_CONFIG
       LOGGING_CONFIG = DEFAULT_LOGGING_CONFIG.copy()
       LOGGING_CONFIG['handlers']['console']['level'] = 'INFO'

3. **Environment Variables**
   - Some logging options can be set via environment variables, e.g.:
     - `AIRFLOW__LOGGING__BASE_LOG_FOLDER`
     - `AIRFLOW__LOGGING__REMOTE_LOGGING`

Recommendations
---------------

- Use `airflow.task` logs to debug task failures.
- Use `airflow.processor` to debug DAG parsing issues.
- For production, consider remote logging (S3, GCS, Elasticsearch) for scalability.
- Do **not** modify `flask_appbuilder` logger unless needed.

References
----------

- :doc:`/configuration/logging`
- :ref:`task-logs`
