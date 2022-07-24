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
"""This module contains Databricks operators."""

import time
from logging import Logger
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.databricks.hooks.databricks import DatabricksHook, RunState
from airflow.providers.databricks.triggers.databricks import DatabricksExecutionTrigger
from airflow.providers.databricks.utils.databricks import deep_string_coerce, validate_trigger_event

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.context import Context

DEFER_METHOD_NAME = 'execute_complete'
XCOM_RUN_ID_KEY = 'run_id'
XCOM_RUN_PAGE_URL_KEY = 'run_page_url'


def _handle_databricks_operator_execution(operator, hook, log, context) -> None:
    """
    Handles the Airflow + Databricks lifecycle logic for a Databricks operator

    :param operator: Databricks operator being handled
    :param context: Airflow context
    """
    if operator.do_xcom_push and context is not None:
        context['ti'].xcom_push(key=XCOM_RUN_ID_KEY, value=operator.run_id)
    log.info('Run submitted with run_id: %s', operator.run_id)
    run_page_url = hook.get_run_page_url(operator.run_id)
    if operator.do_xcom_push and context is not None:
        context['ti'].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=run_page_url)

    if operator.wait_for_termination:
        while True:
            run_state = hook.get_run_state(operator.run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    log.info('%s completed successfully.', operator.task_id)
                    log.info('View run status, Spark UI, and logs at %s', run_page_url)
                    return
                else:
                    run_output = hook.get_run_output(operator.run_id)
                    notebook_error = run_output['error']
                    error_message = (
                        f'{operator.task_id} failed with terminal state: {run_state} '
                        f'and with the error {notebook_error}'
                    )
                    raise AirflowException(error_message)
            else:
                log.info('%s in run state: %s', operator.task_id, run_state)
                log.info('View run status, Spark UI, and logs at %s', run_page_url)
                log.info('Sleeping for %s seconds.', operator.polling_period_seconds)
                time.sleep(operator.polling_period_seconds)
    else:
        log.info('View run status, Spark UI, and logs at %s', run_page_url)


def _handle_deferrable_databricks_operator_execution(operator, hook, log, context) -> None:
    """
    Handles the Airflow + Databricks lifecycle logic for deferrable Databricks operators

    :param operator: Databricks async operator being handled
    :param context: Airflow context
    """
    if operator.do_xcom_push and context is not None:
        context['ti'].xcom_push(key=XCOM_RUN_ID_KEY, value=operator.run_id)
    log.info('Run submitted with run_id: %s', operator.run_id)

    run_page_url = hook.get_run_page_url(operator.run_id)
    if operator.do_xcom_push and context is not None:
        context['ti'].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=run_page_url)
    log.info('View run status, Spark UI, and logs at %s', run_page_url)

    if operator.wait_for_termination:
        operator.defer(
            trigger=DatabricksExecutionTrigger(
                run_id=operator.run_id,
                databricks_conn_id=operator.databricks_conn_id,
                polling_period_seconds=operator.polling_period_seconds,
            ),
            method_name=DEFER_METHOD_NAME,
        )


def _handle_deferrable_databricks_operator_completion(event: dict, log: Logger) -> None:
    validate_trigger_event(event)
    run_state = RunState.from_json(event['run_state'])
    run_page_url = event['run_page_url']
    log.info('View run status, Spark UI, and logs at %s', run_page_url)

    if run_state.is_successful:
        log.info('Job run completed successfully.')
        return
    else:
        error_message = f'Job run failed with terminal state: {run_state}'
        raise AirflowException(error_message)


class DatabricksJobRunLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "See Databricks Job Run"

    def get_link(
        self,
        operator,
        dttm=None,
        *,
        ti_key: Optional["TaskInstanceKey"] = None,
    ) -> str:
        if ti_key is not None:
            run_page_url = XCom.get_value(key=XCOM_RUN_PAGE_URL_KEY, ti_key=ti_key)
        else:
            assert dttm
            run_page_url = XCom.get_one(
                key=XCOM_RUN_PAGE_URL_KEY,
                dag_id=operator.dag.dag_id,
                task_id=operator.task_id,
                execution_date=dttm,
            )

        return run_page_url


class DatabricksSubmitRunOperator(BaseOperator):
    """
    Submits a Spark job run to Databricks using the
    `api/2.1/jobs/runs/submit
    <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.1/jobs/runs/submit`` endpoint and pass it directly
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
          'spark_version': '10.1.x-scala2.12',
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
        - ``spark_jar_task``
        - ``spark_submit_task``
        - ``pipeline_task``
        - ``new_cluster``
        - ``existing_cluster_id``
        - ``libraries``
        - ``run_name``
        - ``timeout_seconds``

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksSubmitRunOperator`

    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/runs/submit`` endpoint. The other named parameters
        (i.e. ``spark_jar_task``, ``notebook_task``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`concepts:jinja-templating`.
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit
    :param spark_jar_task: The main class and parameters for the JAR task. Note that
        the actual JAR is specified in the ``libraries``.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparkjartask
    :param notebook_task: The notebook path and parameters for the notebook task.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsnotebooktask
    :param spark_python_task: The python file path and parameters to run the python file with.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparkpythontask
    :param spark_submit_task: Parameters needed to run a spark-submit command.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparksubmittask
    :param pipeline_task: Parameters needed to execute a Delta Live Tables pipeline task.
        The provided dictionary must contain at least ``pipeline_id`` field!
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobspipelinetask
    :param new_cluster: Specs for a new cluster on which this task will be run.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified
        (except when ``pipeline_task`` is used).
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsclusterspecnewcluster
    :param existing_cluster_id: ID for existing cluster on which to run this task.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified
        (except when ``pipeline_task`` is used).
        This field will be templated.
    :param libraries: Libraries which this run will use.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#managedlibrarieslibrary
    :param run_name: The run name used for this task.
        By default this will be set to the Airflow ``task_id``. This ``task_id`` is a
        required parameter of the superclass ``BaseOperator``.
        This field will be templated.
    :param idempotency_token: an optional token that can be used to guarantee the idempotency of job run
        requests. If a run with the provided token already exists, the request does not create a new run but
        returns the ID of the existing run instead.  This token must have at most 64 characters.
    :param access_control_list: optional list of dictionaries representing Access Control List (ACL) for
        a given job run.  Each dictionary consists of following field - specific subject (``user_name`` for
        users, or ``group_name`` for groups), and ``permission_level`` for that subject.  See Jobs API
        documentation for more details.
    :param wait_for_termination: if we should wait for termination of the job run. ``True`` by default.
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty. (templated)
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :param git_source: Optional specification of a remote git repository from which
        supported task types are retrieved.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit
    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ('json', 'databricks_conn_id')
    template_ext: Sequence[str] = ('.json-tpl',)
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'
    operator_extra_links = (DatabricksJobRunLink(),)

    def __init__(
        self,
        *,
        json: Optional[Any] = None,
        tasks: Optional[List[object]] = None,
        spark_jar_task: Optional[Dict[str, str]] = None,
        notebook_task: Optional[Dict[str, str]] = None,
        spark_python_task: Optional[Dict[str, Union[str, List[str]]]] = None,
        spark_submit_task: Optional[Dict[str, List[str]]] = None,
        pipeline_task: Optional[Dict[str, str]] = None,
        new_cluster: Optional[Dict[str, object]] = None,
        existing_cluster_id: Optional[str] = None,
        libraries: Optional[List[Dict[str, str]]] = None,
        run_name: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        databricks_conn_id: str = 'databricks_default',
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: Optional[Dict[Any, Any]] = None,
        do_xcom_push: bool = True,
        idempotency_token: Optional[str] = None,
        access_control_list: Optional[List[Dict[str, str]]] = None,
        wait_for_termination: bool = True,
        git_source: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksSubmitRunOperator``."""
        super().__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.wait_for_termination = wait_for_termination
        if tasks is not None:
            self.json['tasks'] = tasks
        if spark_jar_task is not None:
            self.json['spark_jar_task'] = spark_jar_task
        if notebook_task is not None:
            self.json['notebook_task'] = notebook_task
        if spark_python_task is not None:
            self.json['spark_python_task'] = spark_python_task
        if spark_submit_task is not None:
            self.json['spark_submit_task'] = spark_submit_task
        if pipeline_task is not None:
            self.json['pipeline_task'] = pipeline_task
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
        if idempotency_token is not None:
            self.json['idempotency_token'] = idempotency_token
        if access_control_list is not None:
            self.json['access_control_list'] = access_control_list
        if git_source is not None:
            self.json['git_source'] = git_source

        self.json = deep_string_coerce(self.json)
        # This variable will be used in case our task gets killed.
        self.run_id: Optional[int] = None
        self.do_xcom_push = do_xcom_push

    @cached_property
    def _hook(self):
        return self._get_hook(caller="DatabricksSubmitRunOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: 'Context'):
        self.run_id = self._hook.submit_run(self.json)
        _handle_databricks_operator_execution(self, self._hook, self.log, context)

    def on_kill(self):
        if self.run_id:
            self._hook.cancel_run(self.run_id)
            self.log.info(
                'Task: %s with run_id: %s was requested to be cancelled.', self.task_id, self.run_id
            )
        else:
            self.log.error('Error: Task: %s with invalid run_id was requested to be cancelled.', self.task_id)


class DatabricksSubmitRunDeferrableOperator(DatabricksSubmitRunOperator):
    """Deferrable version of ``DatabricksSubmitRunOperator``"""

    def execute(self, context):
        hook = self._get_hook(caller="DatabricksSubmitRunDeferrableOperator")
        self.run_id = hook.submit_run(self.json)
        _handle_deferrable_databricks_operator_execution(self, hook, self.log, context)

    def execute_complete(self, context: Optional[dict], event: dict):
        _handle_deferrable_databricks_operator_completion(event, self.log)


class DatabricksRunNowOperator(BaseOperator):
    """
    Runs an existing Spark job run to Databricks using the
    `api/2.1/jobs/run-now
    <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.1/jobs/run-now`` endpoint and pass it directly
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

        jar_params = ["douglas adams", "42"]

        spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"]

        notebook_run = DatabricksRunNowOperator(
            job_id=job_id,
            notebook_params=notebook_params,
            python_params=python_params,
            jar_params=jar_params,
            spark_submit_params=spark_submit_params
        )

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksRunNowOperator`` supports are
        - ``job_id``
        - ``job_name``
        - ``json``
        - ``notebook_params``
        - ``python_params``
        - ``python_named_parameters``
        - ``jar_params``
        - ``spark_submit_params``
        - ``idempotency_token``

    :param job_id: the job_id of the existing Databricks job.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param job_name: the name of the existing Databricks job.
        It must exist only one job with the specified name.
        ``job_id`` and ``job_name`` are mutually exclusive.
        This field will be templated.
    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/run-now`` endpoint. The other named parameters
        (i.e. ``notebook_params``, ``spark_submit_params``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`concepts:jinja-templating`.
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param notebook_params: A dict from keys to values for jobs with notebook task,
        e.g. "notebook_params": {"name": "john doe", "age":  "35"}.
        The map is passed to the notebook and will be accessible through the
        dbutils.widgets.get function. See Widgets for more information.
        If not specified upon run-now, the triggered run will use the
        job's base parameters. notebook_params cannot be
        specified in conjunction with jar_params. The json representation
        of this field (i.e. {"notebook_params":{"name":"john doe","age":"35"}})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/user-guide/notebooks/widgets.html
    :param python_params: A list of parameters for jobs with python tasks,
        e.g. "python_params": ["john doe", "35"].
        The parameters will be passed to python file as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified in job setting.
        The json representation of this field (i.e. {"python_params":["john doe","35"]})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param python_named_parameters: A list of parameters for jobs with python wheel tasks,
        e.g. "python_named_parameters": {"name": "john doe", "age":  "35"}.
        If specified upon run-now, it would overwrite the parameters specified in job setting.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param jar_params: A list of parameters for jobs with JAR tasks,
        e.g. "jar_params": ["john doe", "35"].
        The parameters will be passed to JAR file as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified in
        job setting.
        The json representation of this field (i.e. {"jar_params":["john doe","35"]})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param spark_submit_params: A list of parameters for jobs with spark submit task,
        e.g. "spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"].
        The parameters will be passed to spark-submit script as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified
        in job setting.
        The json representation of this field cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param idempotency_token: an optional token that can be used to guarantee the idempotency of job run
        requests. If a run with the provided token already exists, the request does not create a new run but
        returns the ID of the existing run instead.  This token must have at most 64 characters.
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty. (templated)
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :param wait_for_termination: if we should wait for termination of the job run. ``True`` by default.
    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ('json', 'databricks_conn_id')
    template_ext: Sequence[str] = ('.json-tpl',)
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'
    operator_extra_links = (DatabricksJobRunLink(),)

    def __init__(
        self,
        *,
        job_id: Optional[str] = None,
        job_name: Optional[str] = None,
        json: Optional[Any] = None,
        notebook_params: Optional[Dict[str, str]] = None,
        python_params: Optional[List[str]] = None,
        jar_params: Optional[List[str]] = None,
        spark_submit_params: Optional[List[str]] = None,
        python_named_parameters: Optional[Dict[str, str]] = None,
        idempotency_token: Optional[str] = None,
        databricks_conn_id: str = 'databricks_default',
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: Optional[Dict[Any, Any]] = None,
        do_xcom_push: bool = True,
        wait_for_termination: bool = True,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksRunNowOperator``."""
        super().__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.wait_for_termination = wait_for_termination

        if job_id is not None:
            self.json['job_id'] = job_id
        if job_name is not None:
            self.json['job_name'] = job_name
        if 'job_id' in self.json and 'job_name' in self.json:
            raise AirflowException("Argument 'job_name' is not allowed with argument 'job_id'")
        if notebook_params is not None:
            self.json['notebook_params'] = notebook_params
        if python_params is not None:
            self.json['python_params'] = python_params
        if python_named_parameters is not None:
            self.json['python_named_parameters'] = python_named_parameters
        if jar_params is not None:
            self.json['jar_params'] = jar_params
        if spark_submit_params is not None:
            self.json['spark_submit_params'] = spark_submit_params
        if idempotency_token is not None:
            self.json['idempotency_token'] = idempotency_token

        self.json = deep_string_coerce(self.json)
        # This variable will be used in case our task gets killed.
        self.run_id: Optional[int] = None
        self.do_xcom_push = do_xcom_push

    @cached_property
    def _hook(self):
        return self._get_hook(caller="DatabricksRunNowOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: 'Context'):
        hook = self._hook
        if 'job_name' in self.json:
            job_id = hook.find_job_id_by_name(self.json['job_name'])
            if job_id is None:
                raise AirflowException(f"Job ID for job name {self.json['job_name']} can not be found")
            self.json['job_id'] = job_id
            del self.json['job_name']
        self.run_id = hook.run_now(self.json)
        _handle_databricks_operator_execution(self, hook, self.log, context)

    def on_kill(self):
        if self.run_id:
            self._hook.cancel_run(self.run_id)
            self.log.info(
                'Task: %s with run_id: %s was requested to be cancelled.', self.task_id, self.run_id
            )
        else:
            self.log.error('Error: Task: %s with invalid run_id was requested to be cancelled.', self.task_id)


class DatabricksRunNowDeferrableOperator(DatabricksRunNowOperator):
    """Deferrable version of ``DatabricksRunNowOperator``"""

    def execute(self, context):
        hook = self._get_hook(caller="DatabricksRunNowDeferrableOperator")
        self.run_id = hook.run_now(self.json)
        _handle_deferrable_databricks_operator_execution(self, hook, self.log, context)

    def execute_complete(self, context: Optional[dict], event: dict):
        _handle_deferrable_databricks_operator_completion(event, self.log)
