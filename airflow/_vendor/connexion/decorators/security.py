# Authentication and authorization related decorators
import base64
import functools
import logging
import os
import textwrap

import httpx
from airflow._vendor.connexion.utils import get_function_from_name
import http.cookies

from ..exceptions import (ConnexionException, OAuthProblem,
                          OAuthResponseProblem, OAuthScopeProblem)

logger = logging.getLogger('connexion.api.security')
# use connection pool for OAuth tokeninfo
limits = httpx.Limits(max_keepalive_connections=100, max_connections=100)
session = httpx.Client(limits=limits)


def get_tokeninfo_func(security_definition):
    """
    :type security_definition: dict
    :rtype: function

    >>> get_tokeninfo_url({'x-tokenInfoFunc': 'foo.bar'})
    '<function foo.bar>'
    """
    token_info_func = (security_definition.get("x-tokenInfoFunc") or
                       os.environ.get('TOKENINFO_FUNC'))
    if token_info_func:
        return get_function_from_name(token_info_func)

    token_info_url = (security_definition.get('x-tokenInfoUrl') or
                      os.environ.get('TOKENINFO_URL'))
    if token_info_url:
        return functools.partial(get_tokeninfo_remote, token_info_url)

    return None


def get_scope_validate_func(security_definition):
    """
    :type security_definition: dict
    :rtype: function

    >>> get_scope_validate_func({'x-scopeValidateFunc': 'foo.bar'})
    '<function foo.bar>'
    """
    func = (security_definition.get("x-scopeValidateFunc") or
            os.environ.get('SCOPEVALIDATE_FUNC'))
    if func:
        return get_function_from_name(func)
    return validate_scope


def get_basicinfo_func(security_definition):
    """
    :type security_definition: dict
    :rtype: function

    >>> get_basicinfo_func({'x-basicInfoFunc': 'foo.bar'})
    '<function foo.bar>'
    """
    func = (security_definition.get("x-basicInfoFunc") or
            os.environ.get('BASICINFO_FUNC'))
    if func:
        return get_function_from_name(func)
    return None


def get_apikeyinfo_func(security_definition):
    """
    :type security_definition: dict
    :rtype: function

    >>> get_apikeyinfo_func({'x-apikeyInfoFunc': 'foo.bar'})
    '<function foo.bar>'
    """
    func = (security_definition.get("x-apikeyInfoFunc") or
            os.environ.get('APIKEYINFO_FUNC'))
    if func:
        return get_function_from_name(func)
    return None


def get_bearerinfo_func(security_definition):
    """
    :type security_definition: dict
    :rtype: function

    >>> get_bearerinfo_func({'x-bearerInfoFunc': 'foo.bar'})
    '<function foo.bar>'
    """
    func = (security_definition.get("x-bearerInfoFunc") or
            os.environ.get('BEARERINFO_FUNC'))
    if func:
        return get_function_from_name(func)
    return None


def security_passthrough(function):
    """
    :type function: types.FunctionType
    :rtype: types.FunctionType
    """
    return function


def security_deny(function):
    """
    :type function: types.FunctionType
    :rtype: types.FunctionType
    """

    def deny(*args, **kwargs):
        raise ConnexionException("Error in security definitions")

    return deny


def get_authorization_info(auth_funcs, request, required_scopes):
    for func in auth_funcs:
        token_info = func(request, required_scopes)
        if token_info is not None:
            return token_info

    logger.info("... No auth provided. Aborting with 401.")
    raise OAuthProblem(description='No authorization token provided')


def validate_scope(required_scopes, token_scopes):
    """
    :param required_scopes: Scopes required to access operation
    :param token_scopes: Scopes granted by authorization server
    :rtype: bool
    """
    required_scopes = set(required_scopes)
    if isinstance(token_scopes, list):
        token_scopes = set(token_scopes)
    else:
        token_scopes = set(token_scopes.split())
    logger.debug("... Scopes required: %s", required_scopes)
    logger.debug("... Token scopes: %s", token_scopes)
    if not required_scopes <= token_scopes:
        logger.info(textwrap.dedent("""
                    ... Token scopes (%s) do not match the scopes necessary to call endpoint (%s).
                     Aborting with 403.""").replace('\n', ''),
                    token_scopes, required_scopes)
        return False
    return True


def verify_authorization_token(request, token_info_func):
    """
    :param request: ConnexionRequest
    :param token_info_func: types.FunctionType
    :rtype: dict
    """
    authorization = request.headers.get('Authorization')
    if not authorization:
        return None

    try:
        auth_type, token = authorization.split(None, 1)
    except ValueError:
        raise OAuthProblem(description='Invalid authorization header')

    if auth_type.lower() != 'bearer':
        return None

    token_info = token_info_func(token)
    if token_info is None:
        raise OAuthResponseProblem(
            description='Provided token is not valid',
            token_response=None
        )

    return token_info


def verify_oauth(token_info_func, scope_validate_func):

    def wrapper(request, required_scopes):
        token_info = verify_authorization_token(request, token_info_func)
        if token_info is None:
            return None

        # Fallback to 'scopes' for backward compability
        token_scopes = token_info.get('scope', token_info.get('scopes', ''))
        if not scope_validate_func(required_scopes, token_scopes):
            raise OAuthScopeProblem(
                description='Provided token doesn\'t have the required scope',
                required_scopes=required_scopes,
                token_scopes=token_scopes
            )

        return token_info

    return wrapper


def verify_basic(basic_info_func):

    def wrapper(request, required_scopes):
        authorization = request.headers.get('Authorization')
        if not authorization:
            return None

        try:
            auth_type, user_pass = authorization.split(None, 1)
        except ValueError:
            raise OAuthProblem(description='Invalid authorization header')

        if auth_type.lower() != 'basic':
            return None

        try:
            username, password = base64.b64decode(user_pass).decode('latin1').split(':', 1)
        except Exception:
            raise OAuthProblem(description='Invalid authorization header')

        token_info = basic_info_func(username, password, required_scopes=required_scopes)
        if token_info is None:
            raise OAuthResponseProblem(
                description='Provided authorization is not valid',
                token_response=None
            )
        return token_info

    return wrapper


def get_cookie_value(cookies, name):
    '''
    Returns cookie value by its name. None if no such value.
    :param cookies: str: cookies raw data
    :param name: str: cookies key
    '''
    cookie_parser = http.cookies.SimpleCookie()
    cookie_parser.load(str(cookies))
    try:
        return cookie_parser[name].value
    except KeyError:
        return None


def verify_apikey(apikey_info_func, loc, name):

    def wrapper(request, required_scopes):

        def _immutable_pop(_dict, key):
            """
            Pops the key from an immutable dict and returns the value that was popped,
            and a new immutable dict without the popped key.
            """
            cls = type(_dict)
            try:
                _dict = _dict.to_dict(flat=False)
                return _dict.pop(key)[0], cls(_dict)
            except AttributeError:
                _dict = dict(_dict.items())
                return _dict.pop(key), cls(_dict)

        if loc == 'query':
            try:
                apikey, request.query = _immutable_pop(request.query, name)
            except KeyError:
                apikey = None
        elif loc == 'header':
            apikey = request.headers.get(name)
        elif loc == 'cookie':
            cookieslist = request.headers.get('Cookie')
            apikey = get_cookie_value(cookieslist, name)
        else:
            return None

        if apikey is None:
            return None

        token_info = apikey_info_func(apikey, required_scopes=required_scopes)
        if token_info is None:
            raise OAuthResponseProblem(
                description='Provided apikey is not valid',
                token_response=None
            )
        return token_info

    return wrapper


def verify_bearer(bearer_info_func):
    """
    :param bearer_info_func: types.FunctionType
    :rtype: types.FunctionType
    """

    def wrapper(request, required_scopes):
        return verify_authorization_token(request, bearer_info_func)

    return wrapper


def verify_none():
    """
    :rtype: types.FunctionType
    """

    def wrapper(request, required_scopes):
        return {}

    return wrapper


def verify_security(auth_funcs, required_scopes, function):

    @functools.wraps(function)
    def wrapper(request):
        token_info = get_authorization_info(auth_funcs, request, required_scopes)

        # Fallback to 'uid' for backward compability
        request.context['user'] = token_info.get('sub', token_info.get('uid'))
        request.context['token_info'] = token_info
        return function(request)

    return wrapper


def get_tokeninfo_remote(token_info_url, token):
    """
    Retrieve oauth token_info remotely using HTTP
    :param token_info_url: Url to get information about the token
    :type token_info_url: str
    :param token: oauth token from authorization header
    :type token: str
    :rtype: dict
    """
    token_request = httpx.get(token_info_url, headers={'Authorization': 'Bearer {}'.format(token)}, timeout=5)
    if not token_request.ok:
        return None
    return token_request.json()
