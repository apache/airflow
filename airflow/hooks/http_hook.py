from builtins import str
import logging

import requests

from airflow.hooks.base_hook import BaseHook
from airflow.utils import AirflowException


class HttpHook(BaseHook):
    """
    Interact with HTTP servers.
    """

    def __init__(self, method='POST', http_conn_id='http_default'):
        self.http_conn_id = http_conn_id
        self.method = method

    # headers is required to make it required
    def get_conn(self, headers):
        """
        Returns http session for use with requests
        """
        conn = self.get_connection(self.http_conn_id)
        session = requests.Session()
        self.base_url = conn.host

        if conn.port:
            self.base_url = self.base_url + ":" + str(conn.port) + "/"
        if conn.login:
            session.auth = (conn.login, conn.password)
        if headers:
            session.headers.update(headers)

        return session

    def run(self, endpoint, data=None, headers=None, extra_options=None):
        """
        Performs the request
        """
        session = self.get_conn(headers)

        url = self.base_url + endpoint
        req = None
        if self.method == 'GET':
            # GET uses params
            req = requests.Request(self.method,
                                   url,
                                   params=data,
                                   headers=headers)
        else:
            # Others use data
            req = requests.Request(self.method,
                                   url,
                                   data=data,
                                   headers=headers)

        prepped_request = session.prepare_request(req)
        logging.info("Sending '" + self.method + "' to url: " + url)
        return self.run_and_check(session, prepped_request, extra_options)

    def run_and_check(self, session, prepped_request, extra_options):
        """
        Grabs extra options like timeout and actually runs the request,
        checking for the result
        """
        stream = extra_options.get("stream", False)
        verify = extra_options.get("verify", False)
        proxies = extra_options.get("proxies", {})
        cert = extra_options.get("cert", None)
        timeout = extra_options.get("timeout", None)
        allow_redirects = extra_options.get("allow_redirects", True)

        response = session.send(prepped_request,
                                stream=stream,
                                verify=verify,
                                proxies=proxies,
                                cert=cert,
                                timeout=timeout,
                                allow_redirects=allow_redirects)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            # Tried rewrapping, but not supported. This way, it's possible
            # to get reason and code for failure by checking first 3 chars
            # for the code, or do a split on ':'
            logging.error("HTTP error: " + response.reason)
            if self.method != 'GET':
                # The sensor uses GET, so this prevents filling up the log
                # with the body every time the GET 'misses'.
                # That's ok to do, because GETs should be repeatable and
                # all data should be visible in the log (no post data)
                logging.error(response.text)
            raise AirflowException(str(response.status_code)+":"+response.reason)
        return response
