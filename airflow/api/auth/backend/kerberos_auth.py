# -*- coding: utf-8 -*-
#
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
"""Kerberos authentication module"""
import os

from functools import wraps
from socket import getfqdn

from flask import Response
# noinspection PyProtectedMember
from flask import _request_ctx_stack as stack  # type: ignore
from flask import make_response
from flask import request
from flask import g

import kerberos

from requests_kerberos import HTTPKerberosAuth

from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin


# pylint: disable=c-extension-no-member
CLIENT_AUTH = HTTPKerberosAuth(service='airflow')


LOG = LoggingMixin().log


class KerberosService:  # pylint: disable=too-few-public-methods
    """Class to keep information about the Kerberos Service initialized """
    def __init__(self):
        self.service_name = None


# Stores currently initialized Kerberos Service
_KERBEROS_SERVICE = KerberosService()


def init_app(app):
    """Initializes application with kerberos"""

    hostname = app.config.get('SERVER_NAME')
    if not hostname:
        hostname = getfqdn()
    LOG.info("Kerberos: hostname %s", hostname)

    service = 'airflow'

    _KERBEROS_SERVICE.service_name = "{}@{}".format(service, hostname)

    if 'KRB5_KTNAME' not in os.environ:
        os.environ['KRB5_KTNAME'] = conf.get('kerberos', 'keytab')

    try:
        LOG.info("Kerberos init: %s %s", service, hostname)
        principal = kerberos.getServerPrincipalDetails(service, hostname)
    except kerberos.KrbError as err:
        LOG.warning("Kerberos: %s", err)
    else:
        LOG.info("Kerberos API: server is %s", principal)


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
        return_code, state = kerberos.authGSSServerInit(_KERBEROS_SERVICE.service_name)
        if return_code != kerberos.AUTH_GSS_COMPLETE:
            return None
        return_code = kerberos.authGSSServerStep(state, token)
        if return_code == kerberos.AUTH_GSS_COMPLETE:
            ctx.kerberos_token = kerberos.authGSSServerResponse(state)
            ctx.kerberos_user = kerberos.authGSSServerUserName(state)
            return return_code
        if return_code == kerberos.AUTH_GSS_CONTINUE:
            return kerberos.AUTH_GSS_CONTINUE
        return None
    except kerberos.GSSError:
        return None
    finally:
        if state:
            kerberos.authGSSServerClean(state)


def requires_authentication(function):
    """Decorator for functions that require authentication with Kerberos"""
    @wraps(function)
    def decorated(*args, **kwargs):
        header = request.headers.get("Authorization")
        if header:
            ctx = stack.top
            token = ''.join(header.split()[1:])
            return_code = _gssapi_authenticate(token)
            if return_code == kerberos.AUTH_GSS_COMPLETE:
                g.user = ctx.kerberos_user
                response = function(*args, **kwargs)
                response = make_response(response)
                if ctx.kerberos_token is not None:
                    response.headers['WWW-Authenticate'] = ' '.join(['negotiate',
                                                                     ctx.kerberos_token])

                return response
            if return_code != kerberos.AUTH_GSS_CONTINUE:
                return _forbidden()
        return _unauthorized()
    return decorated
