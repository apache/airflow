#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This module contains Databricks operators.
"""

import time

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.utils.decorators import apply_defaults

XCOM_RUN_ID_KEY = 'run_id'
XCOM_RUN_PAGE_URL_KEY = 'run_page_url'


def _deep_string_coerce(content, json_path='json'):
    """
    Coerces content or all values of content if it is a dict to a string. The
    function will throw if content contains non-string or non-numeric types.

    The reason why we have this function is because the ``self.json`` field must be a
    dict with only string values. This is because ``render_template`` will fail
    for numerical values.
    """
    coerce = _deep_string_coerce
    if isinstance(content, str):
        return content
    elif isinstance(content, (int, float,)):
        # Databricks can tolerate either numeric or string types in the API backend.
        return str(content)
    elif isinstance(content, (list, tuple)):
        return [coerce(e, '{0}[{1}]'.format(json_path, i)) for i, e in enumerate(content)]
    elif isinstance(content, dict):
        return {k: coerce(v, '{0}[{1}]'.format(json_path, k)) for k, v in list(content.items())}
    else:
        param_type = type(content)
        msg = 'Type {0} used for parameter {1} is not a number or a string'.format(param_type, json_path)
        raise AirflowException(msg)


def _handle_databricks_operator_execution(operator, hook, log, context):
    """
    Handles the Airflow + Databricks lifecycle logic for a Databricks operator

    :param operator: Databricks operator being handled
    :param context: Airflow context
    """
    if operator.do_xcom_push:
        context['ti'].xcom_push(key=XCOM_RUN_ID_KEY, value=operator.run_id)
    log.info('Run submitted with run_id: %s', operator.run_id)
    run_page_url = hook.get_run_page_url(operator.run_id)
    if operator.do_xcom_push:
        context['ti'].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=run_page_url)

    log.info('View run status, Spark UI, and logs at %s', run_page_url)
    while True:
        run_state = hook.get_run_state(operator.run_id)
        if run_state.is_terminal:
            if run_state.is_successful:
                log.info('%s completed successfully.', operator.task_id)
                log.info('View run status, Spark UI, and logs at %s', run_page_url)
                return
            else:
                error_message = '{t} failed with terminal state: {s}'.format(t=operator.task_id, s=run_state)
                raise AirflowException(error_message)
        else:
            log.info('%s in run state: %s', operator.task_id, run_state)
            log.info('View run status, Spark UI, and logs at %s', run_page_url)
            log.info('Sleeping for %s seconds.', operator.polling_period_seconds)
            time.sleep(operator.polling_period_seconds)


class DatabricksSubmitRunOperator(BaseOperator):
    """
    Submits a Spark job run to Databricks using the
    `api/2.0/jobs/runs/submit
    <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.0/jobs/runs/submit`` endpoint and pass it directly
    to our ``DatabricksSubmitRunOperator`` through the ``json`` parameter.
    For example ::

        json = {
          'new_cluster': {
            'spark_version': '2.1.0-db3-scala2.11',
            'num_workers': 2
          },
          'notebook_task': {
            'notebook_path': '/Users/airflow@example.com/PrepareData',
          },
        }
        notebook_run = DatabricksSubmitRunOperator(task_id='notebook_run', json=json)

    Another way to accomplish the same thing is to use the named parameters
    of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
    one named parameter for each top level parameter in the ``runs/submit``
    endpoint. In this method, your code would look like this: ::

        new_cluster = {
          'spark_version': '2.1.0-db3-scala2.11',
          'num_workers': 2
        }
        notebook_task = {
          'notebook_path': '/Users/airflow@example.com/PrepareData',
        }
        notebook_run = DatabricksSubmitRunOperator(
            task_id='notebook_run',
            new_cluster=new_cluster,
            notebook_task=notebook_task)

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksSubmitRunOperator`` supports are
        - ``spark_jar_task``
        - ``notebook_task``
        - ``spark_python_task``
        - ``spark_submit_task``
        - ``new_cluster``
        - ``existing_cluster_id``
        - ``libraries``
        - ``run_name``
        - ``timeout_seconds``

    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.0/jobs/runs/submit`` endpoint. The other named parameters
        (i.e. ``spark_jar_task``, ``notebook_task``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`jinja-templating`.
            https://docs.databricks.com/api/latest/jobs.html#runs-submit
    :type json: dict
    :param spark_jar_task: The main class and parameters for the JAR task. Note that
        the actual JAR is specified in the ``libraries``.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobssparkjartask
    :type spark_jar_task: dict
    :param notebook_task: The notebook path and parameters for the notebook task.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobsnotebooktask
    :type notebook_task: dict
    :param spark_python_task: The python file path and parameters to run the python file with.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobssparkpythontask
    :type spark_python_task: dict
    :param spark_submit_task: Parameters needed to run a spark-submit command.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobssparksubmittask
    :type spark_submit_task: dict
    :param new_cluster: Specs for a new cluster on which this task will be run.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobsclusterspecnewcluster
    :type new_cluster: dict
    :param existing_cluster_id: ID for existing cluster on which to run this task.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified.
        This field will be templated.
    :type existing_cluster_id: str
    :param libraries: Libraries which this run will use.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/libraries.html#managedlibrarieslibrary
    :type libraries: list of dicts
    :param run_name: The run name used for this task.
        By default this will be set to the Airflow ``task_id``. This ``task_id`` is a
        required parameter of the superclass ``BaseOperator``.
        This field will be templated.
    :type run_name: str
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :type timeout_seconds: int32
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :type databricks_conn_id: str
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :type databricks_retry_delay: float
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :type do_xcom_push: bool
    """

    # Used in airflow.models.BaseOperator
    template_fields = ('json',)
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        json=None,
        spark_jar_task=None,
        notebook_task=None,
        spark_python_task=None,
        spark_submit_task=None,
        new_cluster=None,
        existing_cluster_id=None,
        libraries=None,
        run_name=None,
        timeout_seconds=None,
        databricks_conn_id='databricks_default',
        polling_period_seconds=30,
        databricks_retry_limit=3,
        databricks_retry_delay=1,
        do_xcom_push=False,
        **kwargs,
    ):
        """
        Creates a new ``DatabricksSubmitRunOperator``.
        """
        super().__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        if spark_jar_task is not None:
            self.json['spark_jar_task'] = spark_jar_task
        if notebook_task is not None:
            self.json['notebook_task'] = notebook_task
        if spark_python_task is not None:
            self.json['spark_python_task'] = spark_python_task
        if spark_submit_task is not None:
            self.json['spark_submit_task'] = spark_submit_task
        if new_cluster is not None:
            self.json['new_cluster'] = new_cluster
        if existing_cluster_id is not None:
            self.json['existing_cluster_id'] = existing_cluster_id
        if libraries is not None:
            self.json['libraries'] = libraries
        if run_name is not None:
            self.json['run_name'] = run_name
        if timeout_seconds is not None:
            self.json['timeout_seconds'] = timeout_seconds
        if 'run_name' not in self.json:
            self.json['run_name'] = run_name or kwargs['task_id']

        self.json = _deep_string_coerce(self.json)
        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.do_xcom_push = do_xcom_push

    def _get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context):
        hook = self._get_hook()
        self.run_id = hook.submit_run(self.json)
        _handle_databricks_operator_execution(self, hook, self.log, context)

    def on_kill(self):
        hook = self._get_hook()
        hook.cancel_run(self.run_id)
        self.log.info('Task: %s with run_id: %s was requested to be cancelled.', self.task_id, self.run_id)


class DatabricksRunNowOperator(BaseOperator):
    """
    Runs an existing Spark job run to Databricks using the
    `api/2.0/jobs/run-now
    <https://docs.databricks.com/api/latest/jobs.html#run-now>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.0/jobs/run-now`` endpoint and pass it directly
    to our ``DatabricksRunNowOperator`` through the ``json`` parameter.
    For example ::

        json = {
          "job_id": 42,
          "notebook_params": {
            "dry-run": "true",
            "oldest-time-to-consider": "1457570074236"
          }
        }

        notebook_run = DatabricksRunNowOperator(task_id='notebook_run', json=json)

    Another way to accomplish the same thing is to use the named parameters
    of the ``DatabricksRunNowOperator`` directly. Note that there is exactly
    one named parameter for each top level parameter in the ``run-now``
    endpoint. In this method, your code would look like this: ::

        job_id=42

        notebook_params = {
            "dry-run": "true",
            "oldest-time-to-consider": "1457570074236"
        }

        python_params = ["douglas adams", "42"]

        spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"]

        notebook_run = DatabricksRunNowOperator(
            job_id=job_id,
            notebook_params=notebook_params,
            python_params=python_params,
            spark_submit_params=spark_submit_params
        )

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksRunNowOperator`` supports are
        - ``job_id``
        - ``json``
        - ``notebook_params``
        - ``python_params``
        - ``spark_submit_params``


    :param job_id: the job_id of the existing Databricks job.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#run-now
    :type job_id: str
    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.0/jobs/run-now`` endpoint. The other named parameters
        (i.e. ``notebook_params``, ``spark_submit_params``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`jinja-templating`.
            https://docs.databricks.com/api/latest/jobs.html#run-now
    :type json: dict
    :param notebook_params: A dict from keys to values for jobs with notebook task,
        e.g. "notebook_params": {"name": "john doe", "age":  "35"}.
        The map is passed to the notebook and will be accessible through the
        dbutils.widgets.get function. See Widgets for more information.
        If not specified upon run-now, the triggered run will use the
        job’s base parameters. notebook_params cannot be
        specified in conjunction with jar_params. The json representation
        of this field (i.e. {"notebook_params":{"name":"john doe","age":"35"}})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/user-guide/notebooks/widgets.html
    :type notebook_params: dict
    :param python_params: A list of parameters for jobs with python tasks,
        e.g. "python_params": ["john doe", "35"].
        The parameters will be passed to python file as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified in
        job setting.
        The json representation of this field (i.e. {"python_params":["john doe","35"]})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#run-now
    :type python_params: list[str]
    :param spark_submit_params: A list of parameters for jobs with spark submit task,
        e.g. "spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"].
        The parameters will be passed to spark-submit script as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified
        in job setting.
        The json representation of this field cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#run-now
    :type spark_submit_params: list[str]
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :type timeout_seconds: int32
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :type databricks_conn_id: str
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :type do_xcom_push: bool
    """

    # Used in airflow.models.BaseOperator
    template_fields = ('json',)
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        job_id=None,
        json=None,
        notebook_params=None,
        python_params=None,
        spark_submit_params=None,
        databricks_conn_id='databricks_default',
        polling_period_seconds=30,
        databricks_retry_limit=3,
        databricks_retry_delay=1,
        do_xcom_push=False,
        **kwargs,
    ):
        """
        Creates a new ``DatabricksRunNowOperator``.
        """
        super().__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay

        if job_id is not None:
            self.json['job_id'] = job_id
        if notebook_params is not None:
            self.json['notebook_params'] = notebook_params
        if python_params is not None:
            self.json['python_params'] = python_params
        if spark_submit_params is not None:
            self.json['spark_submit_params'] = spark_submit_params

        self.json = _deep_string_coerce(self.json)
        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.do_xcom_push = do_xcom_push

    def _get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context):
        hook = self._get_hook()
        self.run_id = hook.run_now(self.json)
        _handle_databricks_operator_execution(self, hook, self.log, context)

    def on_kill(self):
        hook = self._get_hook()
        hook.cancel_run(self.run_id)
        self.log.info('Task: %s with run_id: %s was requested to be cancelled.', self.task_id, self.run_id)
