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

POLL_SLEEP_PERIOD_SECONDS = 5
LINE_BREAK = ("-" * 80)


class DatabricksSubmitRunOperator(BaseOperator):
    """
    Submits an ephemeral run to Databricks.

    https://docs.databricks.com/api/latest/jobs.html#runs-submit

    Note that the named
    parameters to this operator match the parameters exposed by
    the Databricks ``api/2.0/jobs/runs/submit`` endpoint.

    As a result, one way to instantiate a ``DatabricksSubmitRunOperator``
    is to pass the same JSON object used to call ``api/2.0/jobs/runs/submit``
    into the ``DatabricksSubmitRunOperator``. For example: ::

        params = {
          "new_cluster": {
            "spark_version": "2.0.x-scala2.10",
            "node_type_id": "r3.xlarge",
            "aws_attributes": {
              "availability": "ON_DEMAND"
            },
            "num_workers": 2
          },
          "libraries": [
            {
              "jar": "dbfs:/test.jar"
            },
            {
              "maven": {
                "coordinates": "org.jsoup:jsoup:1.7.2"
              }
            }
          ],
          "spark_jar_task": {
            "main_class_name": "com.databricks.ComputeModels"
          }
        }
        DatabricksSubmitRunOperator(task_id='spark_jar_run', **params)

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
        By default this will be set to the Airflow ``task_id``. This task_id is a
        required parameter of the superclass ``BaseOperator``.
    :type run_name: string
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
    :type timeout_seconds: int32
    :param extra_api_parameters: Extra parameters which will be merged with parameters
        listed above. This may be used if additional features are added to the
        ``api/2.0/jobs/runs/submit`` endpoint.
    :type extra_api_parameters: dict
    :param databricks_conn_id: The name of the connection to use.
        By default and in the common case this will be ``databricks_default``.
    :type databricks_conn_id: string
    """
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    def __init__(
            self,
            spark_jar_task=None,
            notebook_task=None,
            new_cluster=None,
            existing_cluster_id=None,
            libraries=None,
            run_name=None,
            timeout_seconds=0,
            extra_api_parameters=None,
            databricks_conn_id='databricks_default',
            **kwargs):
        """
        Creates a new ``DatabricksSubmitRunOperator``.
        """
        super(DatabricksSubmitRunOperator, self).__init__(**kwargs)
        self.spark_jar_task = spark_jar_task
        self.notebook_task = notebook_task
        self.new_cluster = new_cluster
        self.existing_cluster_id = existing_cluster_id
        self.libraries = libraries
        self.run_name = kwargs['task_id'] if run_name is None else run_name
        self.timeout_seconds = timeout_seconds
        if extra_api_parameters is None:
            self.extra_api_parameters = {}
        else:
            self.extra_api_parameters = extra_api_parameters
        self.databricks_conn_id = databricks_conn_id
        self._validate_parameters()

    def _validate_parameters(self):
        """
        Validates the parameters provided to this operator.
        """
        if not self._validate_oneof(self.spark_jar_task, self.notebook_task):
            raise AirflowException('You must specify exactly one of spark_jar_task ' +
                    'and notebook_task.')
        if not self._validate_oneof(self.new_cluster, self.existing_cluster_id):
            raise AirflowException('You must specify exactly one of new_cluster ' +
                    'and existing_cluster_id.')

    def _validate_oneof(self, param_a, param_b):
        """
        Ensures either param_a XOR param_b is set.
        :return: True if validates correctly. False if fails validation.
        :rtype: bool
        """
        if (param_a is None) == (param_b is None):
            return False
        return True

    def _log_run_page_url(self, url):
        logging.info('View run status, Spark UI, and logs at {}'.format(url))

    def get_hook(self):
        return DatabricksHook(self.databricks_conn_id)

    def execute(self, context):
        hook = self.get_hook()
        run_id = hook.submit_run(
            self.spark_jar_task,
            self.notebook_task,
            self.new_cluster,
            self.existing_cluster_id,
            self.libraries,
            self.run_name,
            self.timeout_seconds,
            **self.extra_api_parameters)
        run_page_url = hook.get_run_page_url(run_id)
        logging.info(LINE_BREAK)
        logging.info('Run submitted with run_id: {}'.format(run_id))
        self._log_run_page_url(run_page_url)
        logging.info(LINE_BREAK)
        while True:
            run_state = hook.get_run_state(run_id)
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
                    POLL_SLEEP_PERIOD_SECONDS))
                time.sleep(POLL_SLEEP_PERIOD_SECONDS)
