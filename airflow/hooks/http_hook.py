import logging

import requests

from airflow import utils
from airflow.hooks.base_hook import BaseHook


class HttpHook(BaseHook):

    """
    Interact with HTTP servers.
    """

    def __init__(
            self, method='POST', http_conn_id='http_default'):
        self.http_conn_id = http_conn_id
        self.method = method

    def get_conn(self, headers):
        """
        Returns http session for use with requests
        """
        conn = self.get_connection(self.http_conn_id)
        session = requests.Session()
        self.base_url = conn.host

        if conn.port != None and conn.port > 0:
            self.base_url = self.base_url + ":%d/"%( conn.port )

        if len(conn.login) > 0:
            session.auth = (conn.login, conn.password)
        if headers != None:
            session.headers.update(headers)

        return session

    def run(self, url, data=None, headers=None, extra_options={}):
        """
        Performs the request
        """
        s = self.get_conn( headers )

        url = self.base_url + url
        req = None
        if self.method == 'GET':
            req = requests.Request( self.method, 
                url,
                params=data,
                headers=headers
            )
        else:
            req = requests.Request( self.method, 
                url,
                data=data,
                headers=headers
            )
        prepped = s.prepare_request( req )
        logging.info("Posting to url: " + url)
        return self.run_and_check( s, prepped, extra_options )    

    def run_and_check( self, s, prepped, extra_options ):
        stream = utils.get_val_or_default( extra_options, "stream", False )
        verify = utils.get_val_or_default( extra_options, "verify", False )
        proxies= utils.get_val_or_default( extra_options, "proxies", {} )
        cert = utils.get_val_or_default( extra_options, "cert", None )
        timeout = utils.get_val_or_default( extra_options, "timeout", None )
        allow_redirects = utils.get_val_or_default( extra_options, "allow_redirects", True )

        response = s.send(prepped,
            stream=stream,
            verify=verify,
            proxies=proxies,
            cert=cert,
            timeout=timeout,
            allow_redirects=allow_redirects
        )
        if response.status_code != requests.codes.ok:
            logging.error("HTTP call failed: %d[%s]"%( response.status_code, response.reason ))
            logging.error( response.text )
            return False, response.reason
        return True, response.content

