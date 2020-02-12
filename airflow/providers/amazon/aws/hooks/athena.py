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

"""
This module contains AWS Athena hook
"""
from time import sleep

from airflow.providers.amazon.aws.hooks.aws_hook import AwsHook


class AWSAthenaHook(AwsHook):
    """
    Interact with AWS Athena to run, poll queries and return query results

    :param aws_conn_id: aws connection to use.
    :type aws_conn_id: str
    :param sleep_time: Time to wait between two consecutive call to check query status on athena
    :type sleep_time: int
    """

    INTERMEDIATE_STATES = ('QUEUED', 'RUNNING',)
    FAILURE_STATES = ('FAILED', 'CANCELLED',)
    SUCCESS_STATES = ('SUCCEEDED',)

    def __init__(self, aws_conn_id='aws_default', sleep_time=30, *args, **kwargs):
        super().__init__(aws_conn_id, *args, **kwargs)
        self.sleep_time = sleep_time
        self.conn = None

    def get_conn(self):
        """
        check if aws conn exists already or create one and return it

        :return: boto3 session
        """
        if not self.conn:
            self.conn = self.get_client_type('athena')
        return self.conn

    def run_query(self, query, query_context, result_configuration, client_request_token=None,
                  workgroup='primary'):
        """
        Run Presto query on athena with provided config and return submitted query_execution_id

        :param query: Presto query to run
        :type query: str
        :param query_context: Context in which query need to be run
        :type query_context: dict
        :param result_configuration: Dict with path to store results in and config related to encryption
        :type result_configuration: dict
        :param client_request_token: Unique token created by user to avoid multiple executions of same query
        :type client_request_token: str
        :param workgroup: Athena workgroup name, when not specified, will be 'primary'
        :type workgroup: str
        :return: str
        """
        response = self.get_conn().start_query_execution(QueryString=query,
                                                         ClientRequestToken=client_request_token,
                                                         QueryExecutionContext=query_context,
                                                         ResultConfiguration=result_configuration,
                                                         WorkGroup=workgroup)
        query_execution_id = response['QueryExecutionId']
        return query_execution_id

    def check_query_status(self, query_execution_id):
        """
        Fetch the status of submitted athena query. Returns None or one of valid query states.

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: str
        """
        response = self.get_conn().get_query_execution(QueryExecutionId=query_execution_id)
        state = None
        try:
            state = response['QueryExecution']['Status']['State']
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error('Exception while getting query state %s', ex)
        finally:
            # The error is being absorbed here and is being handled by the caller.
            # The error is being absorbed to implement retries.
            return state  # pylint: disable=lost-exception

    def get_state_change_reason(self, query_execution_id):
        """
        Fetch the reason for a state change (e.g. error message). Returns None or reason string.

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: str
        """
        response = self.get_conn().get_query_execution(QueryExecutionId=query_execution_id)
        reason = None
        try:
            reason = response['QueryExecution']['Status']['StateChangeReason']
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error('Exception while getting query state change reason: %s', ex)
        finally:
            # The error is being absorbed here and is being handled by the caller.
            # The error is being absorbed to implement retries.
            return reason  # pylint: disable=lost-exception

    def get_query_results(self, query_execution_id):
        """
        Fetch submitted athena query results. returns none if query is in intermediate state or
        failed/cancelled state else dict of query output

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: dict
        """
        query_state = self.check_query_status(query_execution_id)
        if query_state is None:
            self.log.error('Invalid Query state')
            return None
        elif query_state in self.INTERMEDIATE_STATES or query_state in self.FAILURE_STATES:
            self.log.error('Query is in "%s" state. Cannot fetch results', query_state)
            return None
        return self.get_conn().get_query_results(QueryExecutionId=query_execution_id)

    def poll_query_status(self, query_execution_id, max_tries=None):
        """
        Poll the status of submitted athena query until query state reaches final state.
        Returns one of the final states

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :param max_tries: Number of times to poll for query state before function exits
        :type max_tries: int
        :return: str
        """
        try_number = 1
        final_query_state = None  # Query state when query reaches final state or max_tries reached
        while True:
            query_state = self.check_query_status(query_execution_id)
            if query_state is None:
                self.log.info('Trial %s: Invalid query state. Retrying again', try_number)
            elif query_state in self.INTERMEDIATE_STATES:
                self.log.info(
                    'Trial %s: Query is still in an intermediate state - %s', try_number, query_state
                )
            else:
                self.log.info(
                    'Trial %s: Query execution completed. Final state is %s}', try_number, query_state
                )
                final_query_state = query_state
                break
            if max_tries and try_number >= max_tries:  # Break loop if max_tries reached
                final_query_state = query_state
                break
            try_number += 1
            sleep(self.sleep_time)
        return final_query_state

    def stop_query(self, query_execution_id):
        """
        Cancel the submitted athena query

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: dict
        """
        return self.get_conn().stop_query_execution(QueryExecutionId=query_execution_id)
