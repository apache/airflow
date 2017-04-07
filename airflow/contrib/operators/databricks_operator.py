# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import time

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.databricks_hook import DatabricksHook
from airflow.models import BaseOperator

LINE_BREAK = ('-' * 80)


class DatabricksSubmitRunOperator(BaseOperator):
    """
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
        take precedence and override the top level json keys.
        https://docs.databricks.com/api/latest/jobs.html#runs-submit
    :type json: dict
    :param spark_jar_task: The main class and parameters for the JAR task. Note that
        the actual JAR is specified in the ``libraries``.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` should be specified.
        https://docs.databricks.com/api/latest/jobs.html#jobssparkjartask
    :type spark_jar_task: dict
    :param notebook_task: The notebook path and parameters for the notebook task.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` should be specified.
        https://docs.databricks.com/api/latest/jobs.html#jobsnotebooktask
    :type notebook_task: dict
    :param new_cluster: Specs for a new cluster on which this task will be run.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified.
        https://docs.databricks.com/api/latest/jobs.html#jobsclusterspecnewcluster
    :type new_cluster: dict
    :param existing_cluster_id: ID for existing cluster on which to run this task.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified.
    :type existing_cluster_id: string
    :param libraries: Libraries which this run will use.
        https://docs.databricks.com/api/latest/libraries.html#managedlibrarieslibrary
    :type libraries: list of dicts
    :param run_name: The run name used for this task.
        By default this will be set to the Airflow ``task_id``. This ``task_id`` is a
        required parameter of the superclass ``BaseOperator``.
    :type run_name: string
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
    :type timeout_seconds: int32
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``.
    :type databricks_conn_id: string
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    """
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    def __init__(
            self,
            json=None,
            spark_jar_task=None,
            notebook_task=None,
            new_cluster=None,
            existing_cluster_id=None,
            libraries=None,
            run_name=None,
            timeout_seconds=None,
            databricks_conn_id='databricks_default',
            polling_period_seconds=30,
            databricks_retry_limit=3,
            **kwargs):
        """
        Creates a new ``DatabricksSubmitRunOperator``.
        """
        super(DatabricksSubmitRunOperator, self).__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        if spark_jar_task is not None:
            self.json['spark_jar_task'] = spark_jar_task
        if notebook_task is not None:
            self.json['notebook_task'] = notebook_task
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

        # This variable will be used in case our task gets killed.
        self.run_id = None

    def _log_run_page_url(self, url):
        logging.info('View run status, Spark UI, and logs at {}'.format(url))

    def get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit)

    def execute(self, context):
        hook = self.get_hook()
        self.run_id = hook.submit_run(self.json)
        run_page_url = hook.get_run_page_url(self.run_id)
        logging.info(LINE_BREAK)
        logging.info('Run submitted with run_id: {}'.format(self.run_id))
        self._log_run_page_url(run_page_url)
        logging.info(LINE_BREAK)
        while True:
            run_state = hook.get_run_state(self.run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    logging.info('{} completed successfully.'.format(
                        self.task_id))
                    self._log_run_page_url(run_page_url)
                    return
                else:
                    error_message = '{t} failed with terminal state: {s}'.format(
                        t=self.task_id,
                        s=run_state)
                    raise AirflowException(error_message)
            else:
                logging.info('{t} in run state: {s}'.format(t=self.task_id,
                                                            s=run_state))
                self._log_run_page_url(run_page_url)
                logging.info('Sleeping for {} seconds.'.format(
                    self.polling_period_seconds))
                time.sleep(self.polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        logging.info('Task: {t} with run_id: {r} was requested to be cancelled.'.format(
            t=self.task_id,
            r=self.run_id))
