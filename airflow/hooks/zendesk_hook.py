"""
A hook to talk to Zendesk
"""

import logging
import time
from zdesk import Zendesk, RateLimitError, ZendeskError
from airflow.hooks import BaseHook
from airflow.plugins_manager import AirflowPlugin


class ZendeskHook(BaseHook):

    def __init__(self, zendesk_conn_id):
        self.__zendesk_conn_id = zendesk_conn_id
        self.__url = None

    def get_conn(self):
        conn = self.get_connection(self.__zendesk_conn_id)
        self.__url = "https://" + conn.host
        return Zendesk(self.__url, conn.login, conn.password, True)

    def __handle_rate_limit_exception(self, rate_limit_exception):
        """
        Sleep for the time specified in the exception. If not specified, wait
        for 60 seconds.
        """
        retry_after = int(
            rate_limit_exception.response.headers.get('Retry-After', 60))
        logging.info(
            "Hit Zendesk API rate limit. Pausing for {} "
            "seconds".format(
                retry_after))
        time.sleep(retry_after)

    def call(self, path, query=None, get_all_pages=True):
        """
        Call Zendesk API and return results

        :param path: The Zendesk API to call
        :param query: Query parameters
        :param get_all_pages: Accumulate results over all pages before
               returning. Due to strict rate limiting, this can often timeout.
               Waits for recommended period between tries after a timeout.
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
        key = path.split("/")[-1].split(".json")[0]
        next_page = results['next_page']
        results = results[key]

        if get_all_pages:
            while next_page is not None:
                try:
                    # Need to split because the next page URL has
                    # `github.zendesk...`
                    # in it, but the call function needs it removed.
                    next_url = next_page.split(self.__url)[1]
                    logging.info("Calling {}".format(next_url))
                    more_res = zendesk.call(next_url)
                    results.extend(more_res[key])
                    if next_page == more_res['next_page']:
                        # Unfortunately zdesk doesn't always throw ZendeskError
                        # when we are done getting all the data. Sometimes the
                        # next just refers to the current set of results. Hence,
                        # need to deal with this special case
                        break
                    else:
                        next_page = more_res['next_page']
                except RateLimitError as rle:
                    self.__handle_rate_limit_exception(rle)
                except ZendeskError as ze:
                    if b"Use a start_time older than 5 minutes" in ze.msg:
                        # We have pretty up to date data
                        break
                    else:
                        raise ze

        return results