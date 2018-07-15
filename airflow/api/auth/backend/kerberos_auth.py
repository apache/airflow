# -*- coding: utf-8 -*-
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

from future.standard_library import install_aliases

from airflow.utils.log.logging_mixin import LoggingMixin

import kerberos
import os

from airflow import configuration as conf

from flask import Response
from flask import _request_ctx_stack as stack
from flask import make_response
from flask import request
from flask import g
from functools import wraps

from requests_kerberos import HTTPKerberosAuth
from socket import getfqdn

install_aliases()

client_auth = HTTPKerberosAuth(service='airflow')

_SERVICE_NAME = None

log = LoggingMixin().log


def init_app(app):
    global _SERVICE_NAME

    hostname = app.config.get('SERVER_NAME')
    if not hostname:
        hostname = getfqdn()
    log.info("Kerberos: hostname %s", hostname)

    service = 'airflow'

    _SERVICE_NAME = "{}@{}".format(service, hostname)

    if 'KRB5_KTNAME' not in os.environ:
        os.environ['KRB5_KTNAME'] = conf.get('kerberos', 'keytab')

    try:
        log.info("Kerberos init: %s %s", service, hostname)
        principal = kerberos.getServerPrincipalDetails(service, hostname)
    except kerberos.KrbError as err:
        log.warning("Kerberos: %s", err)
    else:
        log.info("Kerberos API: server is %s", principal)


def _unauthorized():
    """
    Indicate that authorization is required
    :return:
    """
    return Response("Unauthorized", 401, {"WWW-Authenticate": "Negotiate"})


def _forbidden():
    return Response("Forbidden", 403)


def _gssapi_authenticate(token):
    state = None
    ctx = stack.top
    try:
        rc, state = kerberos.authGSSServerInit(_SERVICE_NAME)
        if rc != kerberos.AUTH_GSS_COMPLETE:
            return None
        rc = kerberos.authGSSServerStep(state, token)
        if rc == kerberos.AUTH_GSS_COMPLETE:
            ctx.kerberos_token = kerberos.authGSSServerResponse(state)
            ctx.kerberos_user = kerberos.authGSSServerUserName(state)
            return rc
        elif rc == kerberos.AUTH_GSS_CONTINUE:
            return kerberos.AUTH_GSS_CONTINUE
        else:
            return None
    except kerberos.GSSError:
        return None
    finally:
        if state:
            kerberos.authGSSServerClean(state)


def requires_authentication(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        header = request.headers.get("Authorization")
        if header:
            ctx = stack.top
            token = ''.join(header.split()[1:])
            rc = _gssapi_authenticate(token)
            if rc == kerberos.AUTH_GSS_COMPLETE:
                g.user = ctx.kerberos_user
                response = function(*args, **kwargs)
                response = make_response(response)
                if ctx.kerberos_token is not None:
                    response.headers['WWW-Authenticate'] = ' '.join(['negotiate',
                                                                     ctx.kerberos_token])

                return response
            elif rc != kerberos.AUTH_GSS_CONTINUE:
                return _forbidden()
        return _unauthorized()
    return decorated
