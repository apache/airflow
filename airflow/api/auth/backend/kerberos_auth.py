# Copyright (c) 2013, Michael Komitee
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from future.standard_library import install_aliases

from airflow.utils.log.LoggingMixin import LoggingMixin

install_aliases()

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

client_auth = HTTPKerberosAuth(service='airflow')

_SERVICE_NAME = None

log = LoggingMixin().logger


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
