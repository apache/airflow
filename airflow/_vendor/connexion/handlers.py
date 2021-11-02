import logging

from .operations.secure import SecureOperation
from .exceptions import AuthenticationProblem, ResolverProblem

logger = logging.getLogger('connexion.handlers')

RESOLVER_ERROR_ENDPOINT_RANDOM_DIGITS = 6


class AuthErrorHandler(SecureOperation):
    """
    Wraps an error with authentication.
    """

    def __init__(self, api, exception, security, security_definitions):
        """
        This class uses the exception instance to produce the proper response problem in case the
        request is authenticated.

        :param exception: the exception to be wrapped with authentication
        :type exception: werkzeug.exceptions.HTTPException
        :param security: list of security rules the application uses by default
        :type security: list
        :param security_definitions: `Security Definitions Object
            <https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#security-definitions-object>`_
        :type security_definitions: dict
        """
        self.exception = exception
        super(AuthErrorHandler, self).__init__(api, security, security_definitions)

    @property
    def function(self):
        """
        Configured error auth handler.
        """
        security_decorator = self.security_decorator
        logger.debug('... Adding security decorator (%r)', security_decorator, extra=vars(self))
        function = self.handle
        function = security_decorator(function)
        function = self._request_response_decorator(function)
        return function

    def handle(self, *args, **kwargs):
        """
        Actual handler for the execution after authentication.
        """
        raise AuthenticationProblem(
            title=self.exception.name,
            detail=self.exception.description,
            status=self.exception.code
        )


class ResolverErrorHandler(SecureOperation):
    """
    Handler for responding to ResolverError.
    """

    def __init__(self, api, status_code, exception, security, security_definitions):
        self.status_code = status_code
        self.exception = exception
        super(ResolverErrorHandler, self).__init__(api, security, security_definitions)

    @property
    def function(self):
        return self.handle

    def handle(self, *args, **kwargs):
        raise ResolverProblem(
            title='Not Implemented',
            detail=self.exception.reason,
            status=self.status_code
        )

    @property
    def operation_id(self):
        return "noop"

    @property
    def randomize_endpoint(self):
        return RESOLVER_ERROR_ENDPOINT_RANDOM_DIGITS

    def get_path_parameter_types(self):
        return {}
