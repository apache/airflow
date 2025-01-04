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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Airflow Error Guide](#airflow-error-guide)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Airflow Error Guide

|Error Code|       Exception Type       |                         User-facing Error Message                         |                                                 Description                                                 |
|----------|----------------------------|---------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
|  AERR001 |      AirflowException      |                    Dynamic task mapping exceeded limit                    |              Happens when dynamically mapped tasks exceed the maximum number of tasks allowed.              |
|  AERR002 |      AirflowException      |                          Task instance not found                          |            Happens when the scheduler or webserver cannot locate a task instance in the database.           |
|  AERR003 |      AirflowException      |                          Task is in 'None' state                          |   Indicates a task instance has not been assigned a proper state, often due to missing execution context.   |
|  AERR004 |  AirflowWebServerException |                         Webserver 502 Bad Gateway                         |            Triggered when the webserver encounters an upstream issue or fails to proxy requests.            |
|  AERR005 |          KeyError          |                       KeyError in Variable retrieval                      |               Occurs when a requested Airflow Variable is not found in the metadata database.               |
|  AERR006 |       PermissionError      |                         Access denied for SSH hook                        |               Triggered when the SSH hook cannot authenticate or connect to the target server.              |
|  AERR007 |    AirflowXComException    |                    TaskInstance not recognized in XCom                    |              Happens when a task's XCom entry is missing or corrupted in the metadata database.             |
|  AERR008 |  AirflowDatabaseException  |                       Duplicate XCom entry detected                       |            Occurs when the same XCom key-value pair is inserted multiple times into the database.           |
|  AERR009 |  AirflowDatabaseException  |                      Error creating database session                      |                         Triggered when Airflow cannot create a new database session.                        |
|  AERR010 |   AirflowConfigException   |           Strict validation in Dataset URI breaks existing DAGs           |   Happens when a dataset URI is not compliant with stricter validation rules introduced in newer versions.  |
|  AERR011 |      AirflowException      |                  Failed to upload logs to remote storage                  |              Occurs when Airflow cannot push task logs to a configured remote storage backend.              |
|  AERR012 |  AirflowDatabaseException  |                     Cannot connect to airflow database                    |          Happens when the metadata database is unreachable due to network or configuration issues.          |
|  AERR013 |          KeyError          |                     KeyError in retrieving XCom value                     |                   Occurs when a requested XCom value is not found or incorrectly defined.                   |
|  AERR014 |         ImportError        |                 Missing dependency for KubernetesExecutor                 |             Occurs when the required dependencies for the KubernetesExecutor are not installed.             |
|  AERR015 |  AirflowDagPausedException |                       DAG is paused and not running                       |                  Indicates the DAG is manually paused and will not trigger scheduled runs.                  |
|  AERR016 |     AirflowTaskTimeout     |                    Task execution delayed indefinitely                    |                  Happens when a task does not start execution within the specified timeout.                 |
|  AERR017 |   AirflowConfigException   |                         Can't find executor class                         |        Happens when the executor specified in the configuration file is not recognized or available.        |
|  AERR018 |   AirflowConfigException   |                     Invalid value in airflow.cfg file                     |                     Triggered when airflow.cfg contains an invalid or unsupported value.                    |
|  AERR019 |     AirflowCliException    |                     Airflow CLI authentication failed                     |                    Occurs when CLI commands cannot authenticate with the Airflow backend.                   |
|  AERR020 |   AirflowTriggerException  |                       Error triggering external API                       |                         Happens when a trigger for an external API fails to execute.                        |
|  AERR021 |  AirflowDatabaseException  |                         Database deadlock detected                        |                Occurs when multiple processes are locked in conflicting database operations.                |
|  AERR022 |       PermissionError      |                 Permission error in KubernetesPodOperator                 |             Occurs when the KubernetesPodOperator lacks permissions to perform required actions.            |
|  AERR023 |  AirflowSchedulerException |                            Scheduler loop error                           |               Triggered when the scheduler encounters an unexpected condition during its loop.              |
|  AERR024 |    AirflowParseException   |                          Broken DAG: syntax error                         |                  Occurs when a syntax error in the DAG file prevents it from being parsed.                  |
|  AERR025 |  AirflowDatabaseException  |                         DagRun state update failed                        |                 Occurs when Airflow fails to update the state of a DAG run in the database.                 |
|  AERR026 |     AirflowTaskTimeout     |                    Task marked as failed due to timeout                   |                      Happens when a task exceeds its maximum allowable execution time.                      |
|  AERR027 |      FileNotFoundError     |                             Task log not found                            |                     Happens when task logs are missing from the local or remote storage.                    |
|  AERR028 |         ImportError        |                    Cannot import module in BashOperator                   |                Triggered when a script run by BashOperator references missing Python modules.               |
|  AERR029 |   AirflowConfigException   |                   Error loading connections from secret                   |               Happens when Airflow fails to load connection credentials from a secret backend.              |
|  AERR030 |   AirflowWorkerException   |                           Worker not responding                           |                Happens when a worker node becomes unresponsive or fails to report its status.               |
|  AERR031 |      AirflowException      |                       Resource not found in GCP hook                      |                    Triggered when a GCP hook is unable to locate the specified resource.                    |
|  AERR032 |  AirflowExecutorException  |                      Backend not reachable for Celery                     |               Occurs when the CeleryExecutor cannot connect to the configured Celery backend.               |
|  AERR033 |      AirflowException      |                          Invalid cron expression                          |                 Occurs when the cron schedule provided in the DAG is invalid or unparsable.                 |
|  AERR034 |       UnpicklingError      |                     UnpicklingError while running task                    |  Happens when Airflow cannot deserialize data, often due to incompatible Python versions or corrupted data. |
|  AERR035 |   AirflowWorkerException   |                  Task instance killed by external system                  |                 Occurs when an external system terminates a task instance during execution.                 |
|  AERR036 |   AirflowWorkerException   |                     Worker died during task execution                     |                  Happens when the worker process handling a task crashes or is terminated.                  |
|  AERR037 |  AirflowDatabaseException  |                         Failed to fetch task state                        |             Occurs when the metadata database does not return a valid state for a task instance.            |
|  AERR038 |         ValueError         |                        Cron interval parsing failed                       |               Occurs when the cron expression for a DAG's schedule interval cannot be parsed.               |
|  AERR039 |  AirflowSchedulerException |                 Scheduler throttled due to excessive DAGs                 |                 Happens when the scheduler takes too long to process a large number of DAGs.                |
|  AERR040 |  AirflowDatabaseException  |                      DagRun execution_date conflicts                      |               Occurs when there is a mismatch in execution_date for a DAG run in the database.              |
|  AERR041 |     AirflowTaskTimeout     |                       Task is stuck in queued state                       |                  Occurs when a task remains queued without being picked up by an executor.                  |
|  AERR042 |    AirflowParseException   |                        Error while parsing DAG file                       |             Triggered when the scheduler encounters syntax errors or invalid code in a DAG file.            |
|  AERR043 |  AirflowDagCycleException  |                       Task dependency cycle detected                      |                      Triggered when task dependencies in a DAG create an infinite loop.                     |
|  AERR044 |    AirflowTaskException    |                    Task failed due to retries exceeded                    |                      Triggered when a task exhausts its retry limit without succeeding.                     |
|  AERR045 |         ValueError         |                        ValueError in task arguments                       |               Happens when a task operator is provided with invalid or incompatible arguments.              |
|  AERR046 |    AirflowTaskException    |                            Task queue not found                           |                       Occurs when the task's queue is not recognized by the executor.                       |
|  AERR047 |  AirflowSchedulerException |                   Executor cannot retrieve task instance                  |              Happens when the executor fails to fetch task instance details from the database.              |
|  AERR048 |  AirflowWebServerException |                        Webserver connection refused                       |                       Occurs when the webserver process is not running or accessible.                       |
|  AERR049 |    AirflowTaskException    |                  Invalid return type from PythonOperator                  |                     Happens when a PythonOperator returns a value of an unexpected type.                    |
|  AERR050 |   AirflowConfigException   |                     Config error: executor not defined                    |                 Triggered when the Airflow configuration does not specify a valid executor.                 |
|  AERR051 |      AirflowException      |                    Error in task failure hook execution                   |                   Triggered when the failure hook defined for a task encounters an error.                   |
|  AERR052 |  AirflowTemplateException  |                    Failed to resolve template variable                    |               Triggered when a task's templated field contains errors or undefined variables.               |
|  AERR053 |  AirflowSchedulerException |                   Scheduler process killed unexpectedly                   |             Triggered when the scheduler process is terminated due to resource or system issues.            |
|  AERR054 |     AirflowTaskTimeout     |                        Task failed with exit code 1                       |            Occurs when a task script or subprocess exits with a non-zero code indicating failure.           |
|  AERR055 |  AirflowDatabaseException  |                  TaskInstance already exists in database                  |               Occurs when a duplicate TaskInstance entry is created in the metadata database.               |
|  AERR056 |  AirflowDatabaseException  |                        Could not reach the database                       |           Indicates connectivity issues with the metadata database, often due to misconfiguration.          |
|  AERR057 |  AirflowExecutorException  |                       Task stuck in 'deferred' state                      |         Triggered when a task using deferrable operators remains in 'deferred' longer than expected.        |
|  AERR058 |      AirflowException      |                       Error loading custom operator                       |                  Indicates a problem in importing or defining a custom operator in the DAG.                 |
|  AERR059 |   AirflowTriggerException  |                     Trigger timeout for external task                     |             Occurs when an ExternalTaskSensor exceeds its timeout waiting for an external task.             |
|  AERR060 |  AirflowDagImportException |                            DagBag import errors                           |               Happens when the scheduler encounters issues loading DAG files into the DagBag.               |
|  AERR061 |     AirflowDagNotFound     |                      DAG not found in trigger DAG run                     |                 Triggered when the DAG specified in a TriggerDagRunOperator does not exist.                 |
|  AERR062 |  AirflowDagCycleException  |                        Circular dependencies in DAG                       |                        Triggered when tasks in a DAG form a circular dependency loop.                       |
|  AERR063 |      AirflowException      |                    Error in on_failure_callback for DAG                   |                      Occurs when the on_failure_callback for a DAG raises an exception.                     |
|  AERR064 |   AirflowTriggerException  |                        TriggerDagRunOperator failed                       |               Happens when the TriggerDagRunOperator cannot trigger the specified target DAG.               |
|  AERR065 |    AirflowTaskException    |                            DAG execution failed                           |         Indicates that the overall execution of a DAG run failed due to errors in one or more tasks.        |
|  AERR066 |  AirflowSchedulerException |                         Scheduler heartbeat failed                        |          Occurs when the scheduler process stops sending heartbeats, often due to resource issues.          |
|  AERR067 |   AirflowConfigException   |  Scheduler crashes when passing invalid value to argument in default_args |               Occurs when default_args in a DAG contains an invalid or incompatible parameter.              |
|  AERR068 |     AirflowApiException    |                     HTTP error while connecting to API                    |        Occurs when Airflow encounters connectivity issues or invalid responses from an external API.        |
|  AERR069 |  AirflowSchedulerException |                 Scheduler backlog due to excessive retries                |                     Happens when too many tasks are retried, overloading the scheduler.                     |
|  AERR070 |      AirflowException      |                            DAG folder not found                           |              Triggered when the directory specified for DAGs does not exist or is inaccessible.             |
|  AERR071 |      AirflowException      |                     Max active tasks for DAG exceeded                     |            Triggered when the number of concurrent tasks for a DAG exceeds the configured limit.            |
|  AERR072 |      AirflowException      |                    Cannot find DAG run in the database                    |             Happens when the specified DAG run is missing or deleted from the metadata database.            |
|  AERR073 |      AirflowException      |                         Airflow CLI not recognized                        |               Happens when the Airflow CLI command is not installed or available in the PATH.               |
|  AERR074 |  AirflowDatabaseException  |                    SQLAlchemy database connection error                   |           Happens when the connection string for the metadata database is invalid or unreachable.           |
|  AERR075 |       PermissionError      |                     Permission denied for Airflow logs                    |              Occurs when Airflow does not have the required permissions to read or write logs.              |
|  AERR076 |      AirflowException      |          Task marked as 'up for retry' without retries available          |               Happens when a task is incorrectly marked for retry but has exceeded its limit.               |
|  AERR077 |   AirflowConfigException   |                       Invalid section in airflow.cfg                      |                  Triggered when airflow.cfg contains an unrecognized configuration section.                 |
|  AERR078 |      AirflowException      |                       Task dependencies are not met                       |               Indicates a task cannot start due to upstream dependencies not being completed.               |
|  AERR079 |      AirflowException      |                      Task not found in serialized DAG                     |             Occurs when the task referenced is missing from the serialized DAG in the database.             |
|  AERR080 |      AirflowException      |                         Worker node not available                         |                   Triggered when an executor cannot find a suitable worker to run a task.                   |
|  AERR081 |     ModuleNotFoundError    |                      Python dependency not installed                      |                          Occurs when a Python package required by a DAG is missing.                         |
|  AERR082 |  AirflowExecutorException  |                        Celery task not acknowledged                       |                          Occurs when a Celery task is not acknowledged by a worker.                         |
|  AERR083 |     AirflowTaskTimeout     |                       Task duration exceeds timeout                       |                         Happens when a task exceeds its specified execution timeout.                        |
|  AERR084 |      AirflowException      |             Error migrating database to Airflow version 2.9.3             |             Indicates issues during metadata database migration, often due to schema mismatches.            |
|  AERR085 |      AirflowException      |                      Error writing metrics to statsd                      |               Occurs when Airflow fails to send metrics data to the configured statsd server.               |
|  AERR086 |      AirflowException      |                             DAG import timeout                            |        Occurs when DAG files take too long to parse, possibly due to large files or inefficient code.       |
|  AERR087 |  AirflowTemplateException  |                  Invalid template in BashOperator command                 |                      Happens when the command template in BashOperator contains errors.                     |
|  AERR088 |  AirflowDagCycleException  |                         Multiple DAGs with same ID                        |                     Triggered when two or more DAGs have the same ID, causing conflicts.                    |
|  AERR089 |       AttributeError       |                      AttributeError in PythonOperator                     |  Occurs when the Python callable in a PythonOperator is improperly defined or missing required attributes.  |
|  AERR090 |      AirflowException      |                Error in callback execution for TaskInstance               |                      Occurs when a failure callback defined for a task raises an error.                     |
|  AERR091 |     AirflowTaskTimeout     |                      Operator execution exceeded SLA                      |               Happens when a task operator runs longer than its Service Level Agreement (SLA).              |
|  AERR092 |     AirflowDagNotFound     |                         DAG not found in scheduler                        |    Occurs when a DAG file is not loaded into the scheduler, often due to parsing errors or missing files.   |
|  AERR093 |      AirflowException      |                    Error in email notification for task                   |                   Triggered when an email notification for a task failure cannot be sent.                   |
|  AERR094 |  AirflowWebServerException |                       Airflow webserver won't start                       |       Happens when the webserver process fails, often due to misconfiguration or missing dependencies.      |
|  AERR095 |     ModuleNotFoundError    |                     No module named '...' in DAG file                     |               Occurs when the DAG imports a Python module that is not installed or available.               |
|  AERR096 |  AirflowTemplateException  |                     Missing template field in operator                    |                 Happens when a required templated field is not defined for a task operator.                 |
|  AERR097 |      AirflowException      |                    Error syncing DAGs to remote storage                   |              Happens when DAG files fail to sync with a remote storage backend like S3 or GCS.              |
|  AERR098 |     AirflowTaskTimeout     |                    Subprocess in task exceeded timeout                    |                Happens when a subprocess spawned by a task runs longer than its allowed time.               |
|  AERR099 |  AirflowDatabaseException  |                        Inconsistent database schema                       |                      Occurs when the metadata database schema is outdated or corrupted.                     |
|  AERR100 |      AirflowException      |                           Invalid DAG structure                           |                  Triggered when a DAG's dependencies or attributes are incorrectly defined.                 |
