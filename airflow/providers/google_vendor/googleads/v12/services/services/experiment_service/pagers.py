# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from typing import Any, Callable, Iterable, Iterator, Sequence, Tuple

from airflow.providers.google_vendor.googleads.v12.services.types import experiment_service
from google.rpc import status_pb2  # type: ignore


class ListExperimentAsyncErrorsPager:
    """A pager for iterating through ``list_experiment_async_errors`` requests.

    This class thinly wraps an initial
    :class:`google.ads.googleads.v12.services.types.ListExperimentAsyncErrorsResponse` object, and
    provides an ``__iter__`` method to iterate through its
    ``errors`` field.

    If there are more pages, the ``__iter__`` method will make additional
    ``ListExperimentAsyncErrors`` requests and continue to iterate
    through the ``errors`` field on the
    corresponding responses.

    All the usual :class:`google.ads.googleads.v12.services.types.ListExperimentAsyncErrorsResponse`
    attributes are available on the pager. If multiple requests are made, only
    the most recent response is retained, and thus used for attribute lookup.
    """

    def __init__(
        self,
        method: Callable[
            ..., experiment_service.ListExperimentAsyncErrorsResponse
        ],
        request: experiment_service.ListExperimentAsyncErrorsRequest,
        response: experiment_service.ListExperimentAsyncErrorsResponse,
        metadata: Sequence[Tuple[str, str]] = (),
    ):
        """Instantiate the pager.

        Args:
            method (Callable): The method that was originally called, and
                which instantiated this pager.
            request (:class:`google.ads.googleads.v12.services.types.ListExperimentAsyncErrorsRequest`):
                The initial request object.
            response (:class:`google.ads.googleads.v12.services.types.ListExperimentAsyncErrorsResponse`):
                The initial response object.
            metadata (Sequence[Tuple[str, str]]): Strings which should be
                sent along with the request as metadata.
        """
        self._method = method
        self._request = experiment_service.ListExperimentAsyncErrorsRequest(
            request
        )
        self._response = response
        self._metadata = metadata

    def __getattr__(self, name: str) -> Any:
        return getattr(self._response, name)

    @property
    def pages(
        self,
    ) -> Iterable[experiment_service.ListExperimentAsyncErrorsResponse]:
        yield self._response
        while self._response.next_page_token:
            self._request.page_token = self._response.next_page_token
            self._response = self._method(
                self._request, metadata=self._metadata
            )
            yield self._response

    def __iter__(self) -> Iterator[status_pb2.Status]:
        for page in self.pages:
            yield from page.errors

    def __repr__(self) -> str:
        return "{0}<{1!r}>".format(self.__class__.__name__, self._response)
