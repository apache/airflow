# -*- coding: utf-8 -*-
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

import time

import six

from airflow.contrib.hooks.databricks_hook import DatabricksHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


XCOM_RUN_ID_KEY = 'run_id'
XCOM_RUN_PAGE_URL_KEY = 'run_page_url'


class DatabricksOperator(BaseOperator):
    """Talks to Databricks (Spark) via REST API."""

    # Used in airflow.models.BaseOperator
    template_fields = ('json',)
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    def __init__(self, endpoint, json=None, run_name=None,
                 timeout_seconds=None, databricks_conn_id='databricks_default',
                 polling_period_seconds=30, databricks_retry_limit=3,
                 wait_run_end=True, do_xcom_push=False, **kwargs):
        """Create a new ``DatabricksOperator``.

        :param endpoint: A string representing the endpoint you wish to
            interact with. One of the options from DatabricksHook.API.
            Ex: '2.0/jobs/runs/get'
        :type action: string
        :param json: A JSON object containing the API parameters to be passed
            directly to the requested endpoint relative to the given
            ``action``. The ``timeout_seconds`` and ``run_name`` named
            arguments will be merged onto this json dictionary if they are
            provided. If specified, the named parameters will always take
            precedence and override the top level json keys. This field will be
            templated.

            .. seealso::
                For more information about templating see
                :ref:`jinja-templating`.
                https://docs.databricks.com/api/latest/jobs.html#runs-submit

        :type json: dict
        :param run_name: The run name used for this task.
            By default this will be set to the Airflow ``task_id``. This
            ``task_id`` is a required parameter of the superclass
            ``BaseOperator``.
            This field will be templated.
        :type run_name: string
        :param timeout_seconds: The timeout for this run. By default a value of
            0 is used which means to have no timeout.
            This field will be templated.
        :type timeout_seconds: int
        :param databricks_conn_id: The name of the Airflow connection to use.
            By default and in the common case this will be
            ``databricks_default``.
        :type databricks_conn_id: string
        :param polling_period_seconds: Controls the rate which we poll for the
            result of this run. By default the operator will poll every 30
            seconds.
        :type polling_period_seconds: int
        :param databricks_retry_limit: Amount of times retry if the Databricks
            backend is unreachable. Its value must be greater than or equal to
            1.
        :type databricks_retry_limit: int
        :param wait_run_end: If we should get the 'run_id' from the initial
            ``action`` and wait until it reaches a terminal state.
        :param do_xcom_push: Whether we should push run_id and run_page_url to
            xcom.
        :type do_xcom_push: boolean
        """
        super(DatabricksOperator, self).__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.wait_run_end = wait_run_end
        self.hook = self._get_hook()
        self.action = self.hook.get_api_method(endpoint)
        self.do_xcom_push = do_xcom_push
        if timeout_seconds is not None:
            self.json['timeout_seconds'] = timeout_seconds
        if 'run_name' not in self.json:
            self.json['run_name'] = run_name or kwargs.get('task_id', None)

        self.json = self._deep_string_coerce(self.json)

        self.run_id = None
        self._run_page_url = None

    def _deep_string_coerce(self, content, json_path='json'):
        """Coerce content or all values of content if it is a dict to a string.

        The function will throw if content contains non-string or non-numeric
        types.

        The reason why we have this function is because the ``self.json`` field
        must be a dict with only string values. This is because
        ``render_template`` will fail for numerical values.
        """
        coerced = self._deep_string_coerce
        if isinstance(content, six.string_types):
            return content
        elif isinstance(content, six.integer_types + (float,)):
            # Databricks can tolerate either numeric or string types in the API
            # backend.
            return str(content)
        elif isinstance(content, (list, tuple)):
            return [coerced(e, '{0}[{1}]'.format(json_path, i))
                    for i, e in enumerate(content)]
        elif isinstance(content, dict):
            return {k: coerced(v, '{0}[{1}]'.format(json_path, k))
                    for k, v in list(content.items())}
        msg = 'Type {} used for parameter {} is not a number or a string'
        raise AirflowException(msg.format(type(content), json_path))

    def _get_hook(self):
        return DatabricksHook(self.databricks_conn_id,
                              retry_limit=self.databricks_retry_limit)

    def _log_run_page_url(self):
        msg = 'View run status, Spark UI, and logs at %s'
        self.log.info(msg, self._run_page_url)

    def _run_until_terminal_state(self):
        """Keep running until the run reaches a terminal state."""
        while True:
            run_state = self.hook.get_run_state(self.run_id)
            if run_state.is_terminal():
                if run_state.is_successful():
                    self.log.info('%s completed successfully.', self.task_id)
                    self._log_run_page_url()
                    return
                msg = '{t} failed with terminal state: {s}'
                raise AirflowException(msg.format(t=self.task_id, s=run_state))
            self.log.info('%s in run state: %s', self.task_id, run_state)
            self._log_run_page_url()
            self.log.info('Sleeping for %s seconds.',
                          self.polling_period_seconds)
            time.sleep(self.polling_period_seconds)

    def execute(self, context):
        response = self.action(self.json)
        if 'run_id' in response:
            self.run_id = response.get('run_id')
            if self.do_xcom_push:
                context['ti'].xcom_push(key=XCOM_RUN_ID_KEY, value=self.run_id)

            self._run_page_url = self.hook.get_run_page_url(self.run_id)
            if self.do_xcom_push:
                context['ti'].xcom_push(key=XCOM_RUN_PAGE_URL_KEY,
                                        value=self._run_page_url)

            self.log.info('Run submitted with run_id: %s', self.run_id)
            self.log.info('View run status, Spark UI, and logs at %s',
                          self._run_page_url)

            if self.wait_run_end:
                self._run_until_terminal_state()

    def on_kill(self):
        self.hook.cancel_run(self.run_id)
        msg = 'Task: {t} with run_id: {r} was requested to be cancelled.'
        self.log.info(msg, t=self.task_id, r=self.run_id)


class DatabricksSubmitRunOperator(DatabricksOperator):
    """Post to the job run Databricks endpoint.

    Submits an Spark job run to Databricks using the
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
        notebook_run = DatabricksSubmitRunOperator(task_id='notebook_run',
                                                   json=json)

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

    In the case where both the json parameter **AND** the named parameters are
    provided, they will be merged together. If there are conflicts during the
    merge, the named parameters will take precedence and override the top level
    ``json`` keys.

    Currently the named parameters that ``DatabricksSubmitRunOperator``
    supports are:
        - ``spark_jar_task``
        - ``notebook_task``
        - ``new_cluster``
        - ``existing_cluster_id``
        - ``libraries``
        - ``run_name``
        - ``timeout_seconds``

    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.0/jobs/runs/submit`` endpoint. The other named
        parameters (i.e. ``spark_jar_task``, ``notebook_task``..) to this
        operator will be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will take
        precedence and override the top level json keys. This field will be
        templated.

        .. seealso::
            For more information about templating see :ref:`jinja-templating`.
            https://docs.databricks.com/api/latest/jobs.html#runs-submit
    :type json: dict
    :param spark_jar_task: The main class and parameters for the JAR task. Note
        that the actual JAR is specified in the ``libraries``.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobssparkjartask
    :type spark_jar_task: dict
    :param notebook_task: The notebook path and parameters for the notebook
        task. *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` should be
        specified. This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobsnotebooktask
    :type notebook_task: dict
    :param new_cluster: Specs for a new cluster on which this task will be run.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be
        specified. This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobsclusterspecnewcluster
    :type new_cluster: dict
    :param existing_cluster_id: ID for existing cluster on which to run this
        task. *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be
        specified. This field will be templated.
    :type existing_cluster_id: string
    :param libraries: Libraries which this run will use.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/libraries.html#managedlibrarieslibrary
    :type libraries: list of dicts
    :param run_name: The run name used for this task.
        By default this will be set to the Airflow ``task_id``. This
        ``task_id`` is a required parameter of the superclass ``BaseOperator``.
        This field will be templated.
    :type run_name: string
    """

    def __init__(self, json=None, spark_jar_task=None, notebook_task=None,
                 new_cluster=None, existing_cluster_id=None, libraries=None,
                 **kwargs):
        """Create a new ``DatabricksSubmitRunOperator``."""
        json = json or {}
        if spark_jar_task is not None:
            json['spark_jar_task'] = spark_jar_task
        if notebook_task is not None:
            json['notebook_task'] = notebook_task
        if new_cluster is not None:
            json['new_cluster'] = new_cluster
        if existing_cluster_id is not None:
            json['existing_cluster_id'] = existing_cluster_id
        if libraries is not None:
            json['libraries'] = libraries

        super(DatabricksSubmitRunOperator, self).__init__(
            endpoint='2.0/jobs/runs/submit',
            json=json,
            **kwargs)
