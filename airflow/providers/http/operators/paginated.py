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
from __future__ import annotations

import base64
import pickle
from typing import TYPE_CHECKING, Any, Callable, List, cast

from requests import Response  # as RequestResponse

from airflow.exceptions import AirflowException
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.triggers.http import HttpTrigger
from airflow.utils.helpers import merge_dicts

if TYPE_CHECKING:
    from airflow.utils.context import Context

# PaginatedHttpOperator pass a list of request.Response object as parameter to
# injected functions, and return a list of str object (from request.Response.text) as result.
ResponseList = List[Response]
ResponseTextList = List[str]


class PaginatedHttpOperator(SimpleHttpOperator[ResponseList, ResponseTextList]):
    """
    Extends the functionalities of the SimpleHttpOperator to provide pagination.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PaginatedHttpOperator`

    :param pagination_function: A callable that generates the parameters used to call the API again.
        Typically used when the API is paginated and returns for e.g a cursor, a 'next page id', or
        a 'next page URL'. When provided, the Operator will call the API repeatedly until this function
        returns None. Also, the result of the Operator will become by default a list of Response.text
        objects (instead of a single response object). Same with the other injected functions (like
        response_check, response_filter, ...) which will also receive a list of Response object. This
        function should return a dict of parameters (`endpoint`, `data`, `headers`, `extra_options`),
        which will be merged and override the one used in the initial API call.
    """

    def __init__(
        self,
        *,
        pagination_function: Callable[..., Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pagination_function = pagination_function

    def execute_sync(self, context: Context) -> Any:
        self.log.info("Calling HTTP method")
        response = self.hook.run(self.endpoint, self.data, self.headers, self.extra_options)

        if self.pagination_function:
            all_responses: ResponseList = cast(ResponseList, [response])
            while True:
                next_page_params = self.pagination_function(response)
                if not next_page_params:
                    break
                response = self.hook.run(**self._merge_next_page_parameters(next_page_params))
                all_responses.append(response)
            response = all_responses
        return self.process_response(context=context, response=response)

    @staticmethod
    def default_response_maker(responses: ResponseList) -> ResponseTextList:
        return [response.text for response in responses]

    def execute_complete(
        self, context: Context, event: dict, paginated_responses: None | ResponseList = None
    ):
        """Callback for when the trigger fires.

        When no pagination, this method returns immediately. Otherwise, it creates a new deferrable.
        Relies on trigger to throw an exception; otherwise it assumes execution was successful.
        """
        if event["status"] == "success":
            response = pickle.loads(base64.standard_b64decode(event["response"]))

            if self.pagination_function:
                paginated_responses = paginated_responses or []
                paginated_responses.append(response)

                next_page_params = self.pagination_function(response)
                if not next_page_params:
                    return self.process_response(context=context, response=paginated_responses)
                self.defer(
                    trigger=HttpTrigger(
                        http_conn_id=self.http_conn_id,
                        auth_type=self.auth_type,
                        method=self.method,
                        **self._merge_next_page_parameters(next_page_params),
                    ),
                    method_name="execute_complete",
                    kwargs={"paginated_responses": paginated_responses},
                )
            else:
                return self.process_response(context=context, response=response)
        else:
            raise AirflowException(f"Unexpected error in the operation: {event['message']}")

    def _merge_next_page_parameters(self, next_page_params: dict) -> dict:
        """Merge initial request parameters with next page parameters.

        Merge initial requests parameters with the ones for the next page, generated by
        the pagination function. Items in the 'next_page_params' overrides those defined
        in the previous request.

        :param next_page_params: A dictionary containing the parameters for the next page.
        :return: A dictionary containing the merged parameters.
        """
        return dict(
            endpoint=next_page_params.get("endpoint") or self.endpoint,
            data=merge_dicts(self.data, next_page_params.get("data", {})),
            headers=merge_dicts(self.headers, next_page_params.get("headers", {})),
            extra_options=merge_dicts(self.extra_options, next_page_params.get("extra_options", {})),
        )
