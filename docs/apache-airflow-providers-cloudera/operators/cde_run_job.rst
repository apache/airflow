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



.. _howto/operator:CdeRunJobOperator:


CdeRunJobOperator
==================

Use the :class:``~airflow.providers.cloudera.operators.CdeRunJobOperator`` to submit a new CDE job via CDE ``/jobs/<job_name>/run`` API endpoint.


Using the Operator
------------------

    Runs a job in a CDE Virtual Cluster. The ``CDEJobRunOperator`` runs the
    named job with optional variables and overrides. The job and its resources
    must have already been created via the specified virtual cluster jobs API.

    The virtual cluster API endpoint is specified by setting the
    ``connection_id`` parameter. The "local" virtual cluster jobs API is the
    default and has a special value of ``cde_runtime_api``. Authentication to
    the API is handled automatically and any jobs in the DAG will run as the
    user who submitted the DAG.

    Jobs can be defined in a virtual cluster with variable placeholders,
    e.g. ``{{ inputdir }}``. Currently the fields supporting variable expansion
    are Spark application name, Spark arguments, and Spark configurations.
    Variables can be passed to the operator as a dictionary of key-value string
    pairs. In addition to any user variables passed via the ``variables``
    parameter, the following standard Airflow macros are automatically
    populated as variables by the operator (see
    https://airflow.apache.org/docs/stable/macros-ref):

    * ``ds``: the execution date as ``YYYY-MM-DD``
    * ``ds_nodash``: the execution date as ``YYYYMMDD``
    * ``ts``: execution date in ISO 8601 format
    * ``ts_nodash``: execution date in ISO 8601 format without '-', ':' or
          timezone information
    * ``run_id``: the run_id of the current DAG run

    If a CDE job needs to run with a different configuration, a task can be
    configured with runtime overrides. For example to override the Spark
    executor memory and cores for a task and to supply an additional config
    parameter you could supply the following dictionary can be supplied to
    the ``overrides`` parameter::

        {
            'spark': {
                'executorMemory': '8g',
                'executorCores': '4',
                'conf': {
                    'spark.kubernetes.memoryOverhead': '2048'
                }
            }
        }

    See the CDE Jobs API documentation for the full list of parameters that
    can be overridden.

    Via the ``wait`` parameter, jobs can either be submitted asynchronously to
    the API (``wait=False``) or the task can wait until the job is complete
    before exiting the task (default is ``wait=True``). If ``wait`` is
    ``True``, the task exit status will reflect the final status of the
    submitted job (or the task will fail on timeout if specified). If ``wait``
    is ``False`` the task status will reflect whether the job was successfully
    submitted to the API or not.

    Note: all parameters below can also be provided through the
    ``default_args`` field of the DAG.


.. list-table::
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - job_name: str
     - The name of the job in the target cluster, required
   * - connection_id: str
     - The Airflow connection id for the target API endpoint, default value ``'cde_runtime_api'``
   * - variables: dict
     - A dictionary of key-value pairs to populate in the job configuration, default empty dict.
   * - overrides: dict
     - A dictionary of key-value pairs to override in the job configuration, default empty dict.
   * - wait: bool
     - If set to true, the operator will wait for the job to complete in the target cluster. The task exit status will reflect the  status of the completed job. Default ``True``
   * - timeout: int
     - The maximum time to wait in seconds for the job to complete if ``wait=True``. If set to ``None``, 0 or a negative number, the task will never be timed out. Default ``0``.
   * - job_poll_interval: int
     - The interval in seconds at which the target API is polled for the job status. Default ``10``.
   * - api_retries: int
     - The number of times to retry an API request in the event of a connection failure or non-fatal API error. Default ``9``.
