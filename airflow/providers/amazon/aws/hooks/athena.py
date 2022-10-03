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
This module contains AWS Athena hook.

.. spelling::

    PageIterator
"""
from __future__ import annotations

import warnings
from time import sleep
from typing import Any

from botocore.paginate import PageIterator

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AthenaHook(AwsBaseHook):
    """
    Interact with AWS Athena to run, poll queries and return query results

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
    """

    INTERMEDIATE_STATES = (
        'QUEUED',
        'RUNNING',
    )
    FAILURE_STATES = (
        'FAILED',
        'CANCELLED',
    )
    SUCCESS_STATES = ('SUCCEEDED',)
    TERMINAL_STATES = (
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    )

    def __init__(self, *args: Any, sleep_time: int = 30, **kwargs: Any) -> None:
        super().__init__(client_type='athena', *args, **kwargs)  # type: ignore
        self.sleep_time = sleep_time

    def run_query(
        self,
        query: str,
        query_context: dict[str, str],
        result_configuration: dict[str, Any],
        client_request_token: str | None = None,
        workgroup: str = 'primary',
    ) -> str:
        """
        Run Presto query on athena with provided config and return submitted query_execution_id

        :param query: Presto query to run
        :param query_context: Context in which query need to be run
        :param result_configuration: Dict with path to store results in and config related to encryption
        :param client_request_token: Unique token created by user to avoid multiple executions of same query
        :param workgroup: Athena workgroup name, when not specified, will be 'primary'
        :return: str
        """
        params = {
            'QueryString': query,
            'QueryExecutionContext': query_context,
            'ResultConfiguration': result_configuration,
            'WorkGroup': workgroup,
        }
        if client_request_token:
            params['ClientRequestToken'] = client_request_token
        response = self.get_conn().start_query_execution(**params)
        return response['QueryExecutionId']

    def check_query_status(self, query_execution_id: str) -> str | None:
        """
        Fetch the status of submitted athena query. Returns None or one of valid query states.

        :param query_execution_id: Id of submitted athena query
        :return: str
        """
        response = self.get_conn().get_query_execution(QueryExecutionId=query_execution_id)
        state = None
        try:
            state = response['QueryExecution']['Status']['State']
        except Exception as ex:
            self.log.error('Exception while getting query state %s', ex)
        finally:
            # The error is being absorbed here and is being handled by the caller.
            # The error is being absorbed to implement retries.
            return state

    def get_state_change_reason(self, query_execution_id: str) -> str | None:
        """
        Fetch the reason for a state change (e.g. error message). Returns None or reason string.

        :param query_execution_id: Id of submitted athena query
        :return: str
        """
        response = self.get_conn().get_query_execution(QueryExecutionId=query_execution_id)
        reason = None
        try:
            reason = response['QueryExecution']['Status']['StateChangeReason']
        except Exception as ex:
            self.log.error('Exception while getting query state change reason: %s', ex)
        finally:
            # The error is being absorbed here and is being handled by the caller.
            # The error is being absorbed to implement retries.
            return reason

    def get_query_results(
        self, query_execution_id: str, next_token_id: str | None = None, max_results: int = 1000
    ) -> dict | None:
        """
        Fetch submitted athena query results. returns none if query is in intermediate state or
        failed/cancelled state else dict of query output

        :param query_execution_id: Id of submitted athena query
        :param next_token_id:  The token that specifies where to start pagination.
        :param max_results: The maximum number of results (rows) to return in this request.
        :return: dict
        """
        query_state = self.check_query_status(query_execution_id)
        if query_state is None:
            self.log.error('Invalid Query state')
            return None
        elif query_state in self.INTERMEDIATE_STATES or query_state in self.FAILURE_STATES:
            self.log.error('Query is in "%s" state. Cannot fetch results', query_state)
            return None
        result_params = {'QueryExecutionId': query_execution_id, 'MaxResults': max_results}
        if next_token_id:
            result_params['NextToken'] = next_token_id
        return self.get_conn().get_query_results(**result_params)

    def get_query_results_paginator(
        self,
        query_execution_id: str,
        max_items: int | None = None,
        page_size: int | None = None,
        starting_token: str | None = None,
    ) -> PageIterator | None:
        """
        Fetch submitted athena query results. returns none if query is in intermediate state or
        failed/cancelled state else a paginator to iterate through pages of results. If you
        wish to get all results at once, call build_full_result() on the returned PageIterator

        :param query_execution_id: Id of submitted athena query
        :param max_items: The total number of items to return.
        :param page_size: The size of each page.
        :param starting_token: A token to specify where to start paginating.
        :return: PageIterator
        """
        query_state = self.check_query_status(query_execution_id)
        if query_state is None:
            self.log.error('Invalid Query state (null)')
            return None
        if query_state in self.INTERMEDIATE_STATES or query_state in self.FAILURE_STATES:
            self.log.error('Query is in "%s" state. Cannot fetch results', query_state)
            return None
        result_params = {
            'QueryExecutionId': query_execution_id,
            'PaginationConfig': {
                'MaxItems': max_items,
                'PageSize': page_size,
                'StartingToken': starting_token,
            },
        }
        paginator = self.get_conn().get_paginator('get_query_results')
        return paginator.paginate(**result_params)

    def poll_query_status(
        self,
        query_execution_id: str,
        max_tries: int | None = None,
        max_polling_attempts: int | None = None,
    ) -> str | None:
        """
        Poll the status of submitted athena query until query state reaches final state.
        Returns one of the final states

        :param query_execution_id: Id of submitted athena query
        :param max_tries: Deprecated - Use max_polling_attempts instead
        :param max_polling_attempts: Number of times to poll for query state before function exits
        :return: str
        """
        if max_tries:
            warnings.warn(
                f"Method `{self.__class__.__name__}.max_tries` is deprecated and will be removed "
                "in a future release.  Please use method `max_polling_attempts` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if max_polling_attempts and max_polling_attempts != max_tries:
                raise Exception("max_polling_attempts must be the same value as max_tries")
            else:
                max_polling_attempts = max_tries

        try_number = 1
        final_query_state = None  # Query state when query reaches final state or max_polling_attempts reached
        while True:
            query_state = self.check_query_status(query_execution_id)
            if query_state is None:
                self.log.info('Trial %s: Invalid query state. Retrying again', try_number)
            elif query_state in self.TERMINAL_STATES:
                self.log.info(
                    'Trial %s: Query execution completed. Final state is %s}', try_number, query_state
                )
                final_query_state = query_state
                break
            else:
                self.log.info('Trial %s: Query is still in non-terminal state - %s', try_number, query_state)
            if (
                max_polling_attempts and try_number >= max_polling_attempts
            ):  # Break loop if max_polling_attempts reached
                final_query_state = query_state
                break
            try_number += 1
            sleep(self.sleep_time)
        return final_query_state

    def get_output_location(self, query_execution_id: str) -> str:
        """
        Function to get the output location of the query results
        in s3 uri format.

        :param query_execution_id: Id of submitted athena query
        :return: str
        """
        output_location = None
        if query_execution_id:
            response = self.get_conn().get_query_execution(QueryExecutionId=query_execution_id)

            if response:
                try:
                    output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                except KeyError:
                    self.log.error("Error retrieving OutputLocation")
                    raise
            else:
                raise
        else:
            raise ValueError("Invalid Query execution id")

        return output_location

    def stop_query(self, query_execution_id: str) -> dict:
        """
        Cancel the submitted athena query

        :param query_execution_id: Id of submitted athena query
        :return: dict
        """
        return self.get_conn().stop_query_execution(QueryExecutionId=query_execution_id)
