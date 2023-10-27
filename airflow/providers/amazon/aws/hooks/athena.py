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

.. spelling:word-list::

    PageIterator
"""
from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import wait

if TYPE_CHECKING:
    from botocore.paginate import PageIterator


class AthenaHook(AwsBaseHook):
    """Interact with Amazon Athena.

    Provide thick wrapper around
    :external+boto3:py:class:`boto3.client("athena") <Athena.Client>`.

    :param sleep_time: obsolete, please use the parameter of `poll_query_status` method instead
    :param log_query: Whether to log athena query and other execution params
        when it's executed. Defaults to *True*.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    INTERMEDIATE_STATES = (
        "QUEUED",
        "RUNNING",
    )
    FAILURE_STATES = (
        "FAILED",
        "CANCELLED",
    )
    SUCCESS_STATES = ("SUCCEEDED",)
    TERMINAL_STATES = (
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    )

    def __init__(
        self, *args: Any, sleep_time: int | None = None, log_query: bool = True, **kwargs: Any
    ) -> None:
        super().__init__(client_type="athena", *args, **kwargs)  # type: ignore
        if sleep_time is not None:
            self.sleep_time = sleep_time
            warnings.warn(
                "The `sleep_time` parameter of the Athena hook is deprecated, "
                "please pass this parameter to the poll_query_status method instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        else:
            self.sleep_time = 30  # previous default value
        self.log_query = log_query
        self.__query_results: dict[str, Any] = {}

    def run_query(
        self,
        query: str,
        query_context: dict[str, str],
        result_configuration: dict[str, Any],
        client_request_token: str | None = None,
        workgroup: str = "primary",
    ) -> str:
        """Run a Trino/Presto query on Athena with provided config.

        .. seealso::
            - :external+boto3:py:meth:`Athena.Client.start_query_execution`

        :param query: Trino/Presto query to run.
        :param query_context: Context in which query need to be run.
        :param result_configuration: Dict with path to store results in and
            config related to encryption.
        :param client_request_token: Unique token created by user to avoid
            multiple executions of same query.
        :param workgroup: Athena workgroup name, when not specified, will be ``'primary'``.
        :return: Submitted query execution ID.
        """
        params = {
            "QueryString": query,
            "QueryExecutionContext": query_context,
            "ResultConfiguration": result_configuration,
            "WorkGroup": workgroup,
        }
        if client_request_token:
            params["ClientRequestToken"] = client_request_token
        if self.log_query:
            self.log.info("Running Query with params: %s", params)
        response = self.get_conn().start_query_execution(**params)
        query_execution_id = response["QueryExecutionId"]
        self.log.info("Query execution id: %s", query_execution_id)
        return query_execution_id

    def get_query_info(self, query_execution_id: str, use_cache: bool = False) -> dict:
        """Get information about a single execution of a query.

        .. seealso::
            - :external+boto3:py:meth:`Athena.Client.get_query_execution`

        :param query_execution_id: Id of submitted athena query
        :param use_cache: If True, use execution information cache
        """
        if use_cache and query_execution_id in self.__query_results:
            return self.__query_results[query_execution_id]
        response = self.get_conn().get_query_execution(QueryExecutionId=query_execution_id)
        if use_cache:
            self.__query_results[query_execution_id] = response
        return response

    def check_query_status(self, query_execution_id: str, use_cache: bool = False) -> str | None:
        """Fetch the state of a submitted query.

        .. seealso::
            - :external+boto3:py:meth:`Athena.Client.get_query_execution`

        :param query_execution_id: Id of submitted athena query
        :return: One of valid query states, or *None* if the response is
            malformed.
        """
        response = self.get_query_info(query_execution_id=query_execution_id, use_cache=use_cache)
        state = None
        try:
            state = response["QueryExecution"]["Status"]["State"]
        except Exception:
            self.log.exception(
                "Exception while getting query state. Query execution id: %s", query_execution_id
            )
        finally:
            # The error is being absorbed here and is being handled by the caller.
            # The error is being absorbed to implement retries.
            return state

    def get_state_change_reason(self, query_execution_id: str, use_cache: bool = False) -> str | None:
        """
        Fetch the reason for a state change (e.g. error message). Returns None or reason string.

        .. seealso::
            - :external+boto3:py:meth:`Athena.Client.get_query_execution`

        :param query_execution_id: Id of submitted athena query
        """
        response = self.get_query_info(query_execution_id=query_execution_id, use_cache=use_cache)
        reason = None
        try:
            reason = response["QueryExecution"]["Status"]["StateChangeReason"]
        except Exception:
            self.log.exception(
                "Exception while getting query state change reason. Query execution id: %s",
                query_execution_id,
            )
        finally:
            # The error is being absorbed here and is being handled by the caller.
            # The error is being absorbed to implement retries.
            return reason

    def get_query_results(
        self, query_execution_id: str, next_token_id: str | None = None, max_results: int = 1000
    ) -> dict | None:
        """Fetch submitted query results.

        .. seealso::
            - :external+boto3:py:meth:`Athena.Client.get_query_results`

        :param query_execution_id: Id of submitted athena query
        :param next_token_id:  The token that specifies where to start pagination.
        :param max_results: The maximum number of results (rows) to return in this request.
        :return: *None* if the query is in intermediate, failed, or cancelled
            state. Otherwise a dict of query outputs.
        """
        query_state = self.check_query_status(query_execution_id)
        if query_state is None:
            self.log.error("Invalid Query state. Query execution id: %s", query_execution_id)
            return None
        elif query_state in self.INTERMEDIATE_STATES or query_state in self.FAILURE_STATES:
            self.log.error(
                'Query is in "%s" state. Cannot fetch results. Query execution id: %s',
                query_state,
                query_execution_id,
            )
            return None
        result_params = {"QueryExecutionId": query_execution_id, "MaxResults": max_results}
        if next_token_id:
            result_params["NextToken"] = next_token_id
        return self.get_conn().get_query_results(**result_params)

    def get_query_results_paginator(
        self,
        query_execution_id: str,
        max_items: int | None = None,
        page_size: int | None = None,
        starting_token: str | None = None,
    ) -> PageIterator | None:
        """Fetch submitted Athena query results.

        .. seealso::
            - :external+boto3:py:class:`Athena.Paginator.GetQueryResults`

        :param query_execution_id: Id of submitted athena query
        :param max_items: The total number of items to return.
        :param page_size: The size of each page.
        :param starting_token: A token to specify where to start paginating.
        :return: *None* if the query is in intermediate, failed, or cancelled
            state. Otherwise a paginator to iterate through pages of results.

        Call :meth`.build_full_result()` on the returned paginator to get all
        results at once.
        """
        query_state = self.check_query_status(query_execution_id)
        if query_state is None:
            self.log.error("Invalid Query state (null). Query execution id: %s", query_execution_id)
            return None
        if query_state in self.INTERMEDIATE_STATES or query_state in self.FAILURE_STATES:
            self.log.error(
                'Query is in "%s" state. Cannot fetch results, Query execution id: %s',
                query_state,
                query_execution_id,
            )
            return None
        result_params = {
            "QueryExecutionId": query_execution_id,
            "PaginationConfig": {
                "MaxItems": max_items,
                "PageSize": page_size,
                "StartingToken": starting_token,
            },
        }
        paginator = self.get_conn().get_paginator("get_query_results")
        return paginator.paginate(**result_params)

    def poll_query_status(
        self, query_execution_id: str, max_polling_attempts: int | None = None, sleep_time: int | None = None
    ) -> str | None:
        """Poll the state of a submitted query until it reaches final state.

        :param query_execution_id: ID of submitted athena query
        :param max_polling_attempts: Number of times to poll for query state before function exits
        :param sleep_time: Time (in seconds) to wait between two consecutive query status checks.
        :return: One of the final states
        """
        try:
            wait(
                waiter=self.get_waiter("query_complete"),
                waiter_delay=self.sleep_time if sleep_time is None else sleep_time,
                waiter_max_attempts=max_polling_attempts or 120,
                args={"QueryExecutionId": query_execution_id},
                failure_message=f"Error while waiting for query {query_execution_id} to complete",
                status_message=f"Query execution id: {query_execution_id}, "
                f"Query is still in non-terminal state",
                status_args=["QueryExecution.Status.State"],
            )
        except AirflowException as error:
            # this function does not raise errors to keep previous behavior.
            self.log.warning(error)
        finally:
            return self.check_query_status(query_execution_id)

    def get_output_location(self, query_execution_id: str) -> str:
        """Get the output location of the query results in S3 URI format.

        .. seealso::
            - :external+boto3:py:meth:`Athena.Client.get_query_execution`

        :param query_execution_id: Id of submitted athena query
        """
        output_location = None
        if query_execution_id:
            response = self.get_query_info(query_execution_id=query_execution_id, use_cache=True)

            if response:
                try:
                    output_location = response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
                except KeyError:
                    self.log.error(
                        "Error retrieving OutputLocation. Query execution id: %s", query_execution_id
                    )
                    raise
            else:
                raise
        else:
            raise ValueError("Invalid Query execution id. Query execution id: %s", query_execution_id)

        return output_location

    def stop_query(self, query_execution_id: str) -> dict:
        """Cancel the submitted query.

        .. seealso::
            - :external+boto3:py:meth:`Athena.Client.stop_query_execution`

        :param query_execution_id: Id of submitted athena query
        """
        self.log.info("Stopping Query with executionId - %s", query_execution_id)
        return self.get_conn().stop_query_execution(QueryExecutionId=query_execution_id)
