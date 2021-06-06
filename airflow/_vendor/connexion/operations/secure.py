import functools
import logging

from ..decorators.decorator import RequestResponseDecorator
from ..decorators.security import (get_apikeyinfo_func, get_basicinfo_func,
                                   get_bearerinfo_func,
                                   get_scope_validate_func, get_tokeninfo_func,
                                   security_deny, security_passthrough,
                                   verify_apikey, verify_basic, verify_bearer,
                                   verify_none, verify_oauth, verify_security)

logger = logging.getLogger("connexion.operations.secure")

DEFAULT_MIMETYPE = 'application/json'


class SecureOperation(object):

    def __init__(self, api, security, security_schemes):
        """
        :param security: list of security rules the application uses by default
        :type security: list
        :param security_definitions: `Security Definitions Object
            <https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#security-definitions-object>`_
        :type security_definitions: dict
        """
        self._api = api
        self._security = security
        self._security_schemes = security_schemes

    @property
    def api(self):
        return self._api

    @property
    def security(self):
        return self._security

    @property
    def security_schemes(self):
        return self._security_schemes

    @property
    def security_decorator(self):
        """
        Gets the security decorator for operation

        From Swagger Specification:

        **Security Definitions Object**

        A declaration of the security schemes available to be used in the specification.

        This does not enforce the security schemes on the operations and only serves to provide the relevant details
        for each scheme.


        **Operation Object -> security**

        A declaration of which security schemes are applied for this operation. The list of values describes alternative
        security schemes that can be used (that is, there is a logical OR between the security requirements).
        This definition overrides any declared top-level security. To remove a top-level security declaration,
        an empty array can be used.


        **Security Requirement Object**

        Lists the required security schemes to execute this operation. The object can have multiple security schemes
        declared in it which are all required (that is, there is a logical AND between the schemes).

        The name used for each property **MUST** correspond to a security scheme declared in the Security Definitions.

        :rtype: types.FunctionType
        """
        logger.debug('... Security: %s', self.security, extra=vars(self))
        if not self.security:
            return security_passthrough

        auth_funcs = []
        required_scopes = None
        for security_req in self.security:
            if not security_req:
                auth_funcs.append(verify_none())
                continue
            elif len(security_req) > 1:
                logger.warning("... More than one security scheme in security requirement defined. "
                               "**DENYING ALL REQUESTS**", extra=vars(self))
                return security_deny

            scheme_name, scopes = next(iter(security_req.items()))
            security_scheme = self.security_schemes[scheme_name]

            if security_scheme['type'] == 'oauth2':
                required_scopes = scopes
                token_info_func = get_tokeninfo_func(security_scheme)
                scope_validate_func = get_scope_validate_func(security_scheme)
                if not token_info_func:
                    logger.warning("... x-tokenInfoFunc missing", extra=vars(self))
                    continue

                auth_funcs.append(verify_oauth(token_info_func, scope_validate_func))

            # Swagger 2.0
            elif security_scheme['type'] == 'basic':
                basic_info_func = get_basicinfo_func(security_scheme)
                if not basic_info_func:
                    logger.warning("... x-basicInfoFunc missing", extra=vars(self))
                    continue

                auth_funcs.append(verify_basic(basic_info_func))

            # OpenAPI 3.0.0
            elif security_scheme['type'] == 'http':
                scheme = security_scheme['scheme'].lower()
                if scheme == 'basic':
                    basic_info_func = get_basicinfo_func(security_scheme)
                    if not basic_info_func:
                        logger.warning("... x-basicInfoFunc missing", extra=vars(self))
                        continue

                    auth_funcs.append(verify_basic(basic_info_func))
                elif scheme == 'bearer':
                    bearer_info_func = get_bearerinfo_func(security_scheme)
                    if not bearer_info_func:
                        logger.warning("... x-bearerInfoFunc missing", extra=vars(self))
                        continue
                    auth_funcs.append(verify_bearer(bearer_info_func))
                else:
                    logger.warning("... Unsupported http authorization scheme %s" % scheme, extra=vars(self))

            elif security_scheme['type'] == 'apiKey':
                scheme = security_scheme.get('x-authentication-scheme', '').lower()
                if scheme == 'bearer':
                    bearer_info_func = get_bearerinfo_func(security_scheme)
                    if not bearer_info_func:
                        logger.warning("... x-bearerInfoFunc missing", extra=vars(self))
                        continue
                    auth_funcs.append(verify_bearer(bearer_info_func))
                else:
                    apikey_info_func = get_apikeyinfo_func(security_scheme)
                    if not apikey_info_func:
                        logger.warning("... x-apikeyInfoFunc missing", extra=vars(self))
                        continue

                    auth_funcs.append(verify_apikey(apikey_info_func, security_scheme['in'], security_scheme['name']))

            else:
                logger.warning("... Unsupported security scheme type %s" % security_scheme['type'], extra=vars(self))

        return functools.partial(verify_security, auth_funcs, required_scopes)

    def get_mimetype(self):
        return DEFAULT_MIMETYPE

    @property
    def _request_response_decorator(self):
        """
        Guarantees that instead of the internal representation of the
        operation handler response
        (connexion.lifecycle.ConnexionRequest) a framework specific
        object is returned.
        :rtype: types.FunctionType
        """
        return RequestResponseDecorator(self.api, self.get_mimetype())
