from __future__ import annotations

import warnings
import logging
import os
from functools import wraps
from typing import TYPE_CHECKING, Callable, NamedTuple, TypeVar, cast

import kerberos
from flask import Response, g, make_response, request
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import numpy as np

from airflow.configuration import conf
from airflow.utils.net import getfqdn
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.utils.airflow_flask_app import get_airflow_app

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser

log = logging.getLogger(__name__)


class KerberosService:
    """Class to keep information about the Kerberos Service initialized."""

    def __init__(self):
        self.service_name = None


class _KerberosAuth(NamedTuple):
    return_code: int | None
    user: str = ""
    token: str | None = None


# Stores currently initialized Kerberos Service
_KERBEROS_SERVICE = KerberosService()

# Initialize an IsolationForest model for anomaly detection
anomaly_detector = IsolationForest(contamination=0.01, random_state=42)
scaler = StandardScaler()

# Placeholder for user behavior data (could be extended to store actual historical data)
user_behavior_data = []


def init_app(app):
    """Initialize application with kerberos."""
    hostname = app.config.get("SERVER_NAME")
    if not hostname:
        hostname = getfqdn()
    log.info("Kerberos: hostname %s", hostname)

    service = "airflow"

    _KERBEROS_SERVICE.service_name = f"{service}@{hostname}"

    if "KRB5_KTNAME" not in os.environ:
        os.environ["KRB5_KTNAME"] = conf.get("kerberos", "keytab")

    try:
        log.info("Kerberos init: %s %s", service, hostname)
        principal = kerberos.getServerPrincipalDetails(service, hostname)
    except kerberos.KrbError as err:
        log.warning("Kerberos: %s", err)
    else:
        log.info("Kerberos API: server is %s", principal)


def _unauthorized():
    """Indicate that authorization is required."""
    return Response("Unauthorized", 401, {"WWW-Authenticate": "Negotiate"})


def _forbidden():
    return Response("Forbidden", 403)


def _gssapi_authenticate(token) -> _KerberosAuth | None:
    state = None
    try:
        return_code, state = kerberos.authGSSServerInit(_KERBEROS_SERVICE.service_name)
        if return_code != kerberos.AUTH_GSS_COMPLETE:
            return _KerberosAuth(return_code=None)

        if (return_code := kerberos.authGSSServerStep(state, token)) == kerberos.AUTH_GSS_COMPLETE:
            user = kerberos.authGSSServerUserName(state)
            auth_token = kerberos.authGSSServerResponse(state)

            # AI-Based Anomaly Detection
            global user_behavior_data
            if len(user_behavior_data) > 1:
                # Convert user behavior data to numpy array and scale
                behavior_array = np.array(user_behavior_data).reshape(-1, 1)
                scaled_behavior = scaler.fit_transform(behavior_array)
                predictions = anomaly_detector.fit_predict(scaled_behavior)
                
                # Detect anomalies
                if predictions[-1] == -1:
                    log.warning("Anomalous behavior detected for user: %s", user)
                    return _forbidden()

            user_behavior_data.append([return_code])  # Example behavior data
            return _KerberosAuth(
                return_code=return_code,
                user=user,
                token=auth_token,
            )
        elif return_code == kerberos.AUTH_GSS_CONTINUE:
            return _KerberosAuth(return_code=return_code)
        return _KerberosAuth(return_code=return_code)
    except kerberos.GSSError:
        return _KerberosAuth(return_code=None)
    finally:
        if state:
            kerberos.authGSSServerClean(state)


T = TypeVar("T", bound=Callable)


def requires_authentication(function: T, find_user: Callable[[str], BaseUser] | None = None):
    """Decorate functions that require authentication with Kerberos."""
    if not find_user:
        warnings.warn(
            "This module is deprecated. Please use "
            "`airflow.providers.fab.auth_manager.api.auth.backend.kerberos_auth` instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        find_user = get_airflow_app().appbuilder.sm.find_user

    @wraps(function)
    def decorated(*args, **kwargs):
        header = request.headers.get("Authorization")
        if header:
            token = "".join(header.split()[1:])
            auth = _gssapi_authenticate(token)
            if auth.return_code == kerberos.AUTH_GSS_COMPLETE:
                g.user = find_user(auth.user)
                response = function(*args, **kwargs)
                response = make_response(response)
                if auth.token is not None:
                    response.headers["WWW-Authenticate"] = f"negotiate {auth.token}"
                return response
            elif auth.return_code != kerberos.AUTH_GSS_CONTINUE:
                return _forbidden()
        return _unauthorized()

    return cast(T, decorated)


def __getattr__(name):
    # PEP-562: Lazy loaded attributes on python modules
    if name != "CLIENT_AUTH":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from requests_kerberos import HTTPKerberosAuth

    val = HTTPKerberosAuth(service="airflow")
    # Store for next time
    globals()[name] = val
    return val
