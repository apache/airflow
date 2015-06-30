import logging

import requests

from airflow import utils
from airflow.hooks.base_hook import BaseHook


class HttpHook(BaseHook):

    """
    Interact with HTTP servers.
    """

    def __init__(
            self, http_conn_id='http_default'):
        self.http_conn_id = http_conn_id

    def get_conn(self, headers):
        """
        Returns http session for use with requests
        """
        conn = self.get_connection(self.http_conn_id)
        session = requests.Session()
        if len(conn.login) > 0:
            session.auth = (conn.login, conn.password)
        if headers != None:
            s.headers.update(headers)

        return session

    def execute_and_check( self, s, prepped, context ):
        stream = utils.get_val_or_default( context, "stream", False )
        verify = utils.get_val_or_default( context, "verify", False )
        proxies= utils.get_val_or_default( context, "proxies", {} )
        cert = utils.get_val_or_default( context, "cert", None )
        timeout = utils.get_val_or_default( context, "timeout", None )

        s.send(prepped,
            stream=stream,
            verify=verify,
            proxies=proxies,
            cert=cert,
            timeout=timeout
        )
        if resp.status_code != requests.codes.ok:
            raise AirflowException("%s failed. %d [%s]"%( op, resp.status_code, resp.reason ))

        return resp.content

    def get(self, url, params={}, headers=None, context={}):
        """
        Performs a GET request
        """
        s = get_conn( headers )
        req = Request('GET',  url,
            data=params
            headers=headers
        )
        prepped = s.prepare_request( req )
        logging.info("Getting url: " + url)
        self.execute_and_check( s, prepped, context )

    def post(self, url, data, headers=None, context={}):
        """
        Performs a POST request
        """
        s = get_conn( headers )
        req = Request('POST',  url,
            data=data
            headers=headers
        )
        prepped = s.prepare_request( req )
        logging.info("Posting to url: " + url)
        self.execute_and_check( s, prepped, context )

    def put(self, url, data, headers=None, context={}):
        """
        Performs a PUT request
        """
        s = get_conn( headers )
        req = Request('PUT',  url,
            data=data
            headers=headers
        )
        prepped = s.prepare_request( req )
        logging.info("Putting to url: " + url)
        self.execute_and_check( s, prepped, context )

    def delete(self, url, data, headers=None, context={}):
        """
        Performs a PUT request
        """
        s = get_conn( headers )
        req = Request('DELETE',  url,
            data=data
            headers=headers
        )
        prepped = s.prepare_request( req )
        logging.info("Deleting from url: " + url)
        self.execute_and_check( s, prepped, context )

