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

from uuid import uuid4

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook
from airflow.utils.decorators import apply_defaults


class AWSAthenaOperator(BaseOperator):
    """
    An operator that submit presto query to athena.

    If ``do_xcom_push`` is True, the QueryExecutionID assigned to the
    query will be pushed to an XCom when it successfuly completes.

    :param query: Presto to be run on athena. (templated)
    :type query: str
    :param database: Database to select. (templated)
    :type database: str
    :param output_location: s3 path to write the query results into. (templated)
    :type output_location: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    :param sleep_time: Time to wait between two consecutive call to check query status on athena
    :type sleep_time: int
    :param max_tries: Number of times to poll for query state before function exits
    :type max_triex: int
    """

    ui_color = '#44b5e2'
    template_fields = ('query', 'database', 'output_location')
    template_ext = ('.sql', )

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        query,
        database,
        output_location,
        aws_conn_id="aws_default",
        client_request_token=None,
        workgroup="primary",
        query_execution_context=None,
        result_configuration=None,
        sleep_time=30,
        max_tries=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.database = database
        self.output_location = output_location
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.workgroup = workgroup
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.max_tries = max_tries
        self.query_execution_id = None
        self.hook = None

    def get_hook(self):
        """Create and return an AWSAthenaHook."""
        return AWSAthenaHook(self.aws_conn_id, self.sleep_time)

    def execute(self, context):
        """
        Run Presto Query on Athena
        """
        self.hook = self.get_hook()

        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.output_location
        self.query_execution_id = self.hook.run_query(self.query, self.query_execution_context,
                                                      self.result_configuration, self.client_request_token,
                                                      self.workgroup)
        query_status = self.hook.poll_query_status(self.query_execution_id, self.max_tries)

        if query_status in AWSAthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(self.query_execution_id)
            raise Exception(
                'Final state of Athena job is {}, query_execution_id is {}. Error: {}'
                .format(query_status, self.query_execution_id, error_message))
        elif not query_status or query_status in AWSAthenaHook.INTERMEDIATE_STATES:
            raise Exception(
                'Final state of Athena job is {}. '
                'Max tries of poll status exceeded, query_execution_id is {}.'
                .format(query_status, self.query_execution_id))

        return self.query_execution_id

    def on_kill(self):
        """
        Cancel the submitted athena query
        """
        if self.query_execution_id:
            self.log.info('⚰️⚰️⚰️ Received a kill Signal. Time to Die')
            self.log.info(
                'Stopping Query with executionId - %s', self.query_execution_id
            )
            response = self.hook.stop_query(self.query_execution_id)
            http_status_code = None
            try:
                http_status_code = response['ResponseMetadata']['HTTPStatusCode']
            except Exception as ex:  # pylint: disable=broad-except
                self.log.error('Exception while cancelling query: %s', ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error('Unable to request query cancel on athena. Exiting')
                else:
                    self.log.info(
                        'Polling Athena for query with id %s to reach final state', self.query_execution_id
                    )
                    self.hook.poll_query_status(self.query_execution_id)
