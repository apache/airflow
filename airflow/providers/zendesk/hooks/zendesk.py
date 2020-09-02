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

import time

from zdesk import RateLimitError, Zendesk, ZendeskError

from airflow.hooks.base_hook import BaseHook


class ZendeskHook(BaseHook):
    """
    A hook to talk to Zendesk
    """

    def __init__(self, zendesk_conn_id):
        super().__init__()
        self.__zendesk_conn_id = zendesk_conn_id
        self.__url = None

    def get_conn(self):
        conn = self.get_connection(self.__zendesk_conn_id)
        self.__url = "https://" + conn.host
        return Zendesk(
            zdesk_url=self.__url, zdesk_email=conn.login, zdesk_password=conn.password, zdesk_token=True
        )

    def __handle_rate_limit_exception(self, rate_limit_exception):
        """
        Sleep for the time specified in the exception. If not specified, wait
        for 60 seconds.
        """
        retry_after = int(rate_limit_exception.response.headers.get('Retry-After', 60))
        self.log.info("Hit Zendesk API rate limit. Pausing for %s seconds", retry_after)
        time.sleep(retry_after)

    def call(self, path, query=None, get_all_pages=True, side_loading=False):
        """
        Call Zendesk API and return results

        :param path: The Zendesk API to call
        :param query: Query parameters
        :param get_all_pages: Accumulate results over all pages before
               returning. Due to strict rate limiting, this can often timeout.
               Waits for recommended period between tries after a timeout.
        :param side_loading: Retrieve related records as part of a single
               request. In order to enable side-loading, add an 'include'
               query parameter containing a comma-separated list of resources
               to load. For more information on side-loading see
               https://developer.zendesk.com/rest_api/docs/core/side_loading
        """
        zendesk = self.get_conn()
        first_request_successful = False

        while not first_request_successful:
            try:
                results = zendesk.call(path, query)
                first_request_successful = True
            except RateLimitError as rle:
                self.__handle_rate_limit_exception(rle)

        # Find the key with the results
        keys = [path.split("/")[-1].split(".json")[0]]
        next_page = results['next_page']
        if side_loading:
            keys += query['include'].split(',')
        results = {key: results[key] for key in keys}

        # pylint: disable=too-many-nested-blocks
        if get_all_pages:
            while next_page is not None:
                try:
                    # Need to split because the next page URL has
                    # `github.zendesk...`
                    # in it, but the call function needs it removed.
                    next_url = next_page.split(self.__url)[1]
                    self.log.info("Calling %s", next_url)
                    more_res = zendesk.call(next_url)
                    for key in results:
                        results[key].extend(more_res[key])
                    if next_page == more_res['next_page']:
                        # Unfortunately zdesk doesn't always throw ZendeskError
                        # when we are done getting all the data. Sometimes the
                        # next just refers to the current set of results.
                        # Hence, need to deal with this special case
                        break
                    next_page = more_res['next_page']
                except RateLimitError as rle:
                    self.__handle_rate_limit_exception(rle)
                except ZendeskError as zde:
                    if b"Use a start_time older than 5 minutes" in zde.msg:
                        # We have pretty up to date data
                        break
                    raise zde

        return results
