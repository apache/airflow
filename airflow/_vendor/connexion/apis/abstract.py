import abc
import logging
import pathlib
import sys
import warnings
from enum import Enum

from ..decorators.produces import NoContent
from ..exceptions import ResolverError
from ..http_facts import METHODS
from ..jsonifier import Jsonifier
from ..lifecycle import ConnexionResponse
from ..operations import make_operation
from ..options import ConnexionOptions
from ..resolver import Resolver
from ..spec import Specification
from ..utils import is_json_mimetype

MODULE_PATH = pathlib.Path(__file__).absolute().parent.parent
SWAGGER_UI_URL = 'ui'

logger = logging.getLogger('connexion.apis.abstract')


class AbstractAPIMeta(abc.ABCMeta):

    def __init__(cls, name, bases, attrs):
        abc.ABCMeta.__init__(cls, name, bases, attrs)
        cls._set_jsonifier()


class AbstractAPI(metaclass=AbstractAPIMeta):
    """
    Defines an abstract interface for a Swagger API
    """

    def __init__(self, specification, base_path=None, arguments=None,
                 validate_responses=False, strict_validation=False, resolver=None,
                 auth_all_paths=False, debug=False, resolver_error_handler=None,
                 validator_map=None, pythonic_params=False, pass_context_arg_name=None, options=None):
        """
        :type specification: pathlib.Path | dict
        :type base_path: str | None
        :type arguments: dict | None
        :type validate_responses: bool
        :type strict_validation: bool
        :type auth_all_paths: bool
        :type debug: bool
        :param validator_map: Custom validators for the types "parameter", "body" and "response".
        :type validator_map: dict
        :param resolver: Callable that maps operationID to a function
        :param resolver_error_handler: If given, a callable that generates an
            Operation used for handling ResolveErrors
        :type resolver_error_handler: callable | None
        :param pythonic_params: When True CamelCase parameters are converted to snake_case and an underscore is appended
        to any shadowed built-ins
        :type pythonic_params: bool
        :param options: New style options dictionary.
        :type options: dict | None
        :param pass_context_arg_name: If not None URL request handling functions with an argument matching this name
        will be passed the framework's request context.
        :type pass_context_arg_name: str | None
        """
        self.debug = debug
        self.validator_map = validator_map
        self.resolver_error_handler = resolver_error_handler

        logger.debug('Loading specification: %s', specification,
                     extra={'swagger_yaml': specification,
                            'base_path': base_path,
                            'arguments': arguments,
                            'auth_all_paths': auth_all_paths})

        # Avoid validator having ability to modify specification
        self.specification = Specification.load(specification, arguments=arguments)

        logger.debug('Read specification', extra={'spec': self.specification})

        self.options = ConnexionOptions(options, oas_version=self.specification.version)

        logger.debug('Options Loaded',
                     extra={'swagger_ui': self.options.openapi_console_ui_available,
                            'swagger_path': self.options.openapi_console_ui_from_dir,
                            'swagger_url': self.options.openapi_console_ui_path})

        self._set_base_path(base_path)

        logger.debug('Security Definitions: %s', self.specification.security_definitions)

        self.resolver = resolver or Resolver()

        logger.debug('Validate Responses: %s', str(validate_responses))
        self.validate_responses = validate_responses

        logger.debug('Strict Request Validation: %s', str(strict_validation))
        self.strict_validation = strict_validation

        logger.debug('Pythonic params: %s', str(pythonic_params))
        self.pythonic_params = pythonic_params

        logger.debug('pass_context_arg_name: %s', pass_context_arg_name)
        self.pass_context_arg_name = pass_context_arg_name

        if self.options.openapi_spec_available:
            self.add_openapi_json()
            self.add_openapi_yaml()

        if self.options.openapi_console_ui_available:
            self.add_swagger_ui()

        self.add_paths()

        if auth_all_paths:
            self.add_auth_on_not_found(
                self.specification.security,
                self.specification.security_definitions
            )

    def _set_base_path(self, base_path=None):
        if base_path is not None:
            # update spec to include user-provided base_path
            self.specification.base_path = base_path
            self.base_path = base_path
        else:
            self.base_path = self.specification.base_path

    @abc.abstractmethod
    def add_openapi_json(self):
        """
        Adds openapi spec to {base_path}/openapi.json
             (or {base_path}/swagger.json for swagger2)
        """

    @abc.abstractmethod
    def add_swagger_ui(self):
        """
        Adds swagger ui to {base_path}/ui/
        """

    @abc.abstractmethod
    def add_auth_on_not_found(self, security, security_definitions):
        """
        Adds a 404 error handler to authenticate and only expose the 404 status if the security validation pass.
        """

    def add_operation(self, path, method):
        """
        Adds one operation to the api.

        This method uses the OperationID identify the module and function that will handle the operation

        From Swagger Specification:

        **OperationID**

        A friendly name for the operation. The id MUST be unique among all operations described in the API.
        Tools and libraries MAY use the operation id to uniquely identify an operation.

        :type method: str
        :type path: str
        """
        operation = make_operation(
            self.specification,
            self,
            path,
            method,
            self.resolver,
            validate_responses=self.validate_responses,
            validator_map=self.validator_map,
            strict_validation=self.strict_validation,
            pythonic_params=self.pythonic_params,
            uri_parser_class=self.options.uri_parser_class,
            pass_context_arg_name=self.pass_context_arg_name
        )
        self._add_operation_internal(method, path, operation)

    @abc.abstractmethod
    def _add_operation_internal(self, method, path, operation):
        """
        Adds the operation according to the user framework in use.
        It will be used to register the operation on the user framework router.
        """

    def _add_resolver_error_handler(self, method, path, err):
        """
        Adds a handler for ResolverError for the given method and path.
        """
        operation = self.resolver_error_handler(
            err,
            security=self.specification.security,
            security_definitions=self.specification.security_definitions
        )
        self._add_operation_internal(method, path, operation)

    def add_paths(self, paths=None):
        """
        Adds the paths defined in the specification as endpoints

        :type paths: list
        """
        paths = paths or self.specification.get('paths', dict())
        for path, methods in paths.items():
            logger.debug('Adding %s%s...', self.base_path, path)

            for method in methods:
                if method not in METHODS:
                    continue
                try:
                    self.add_operation(path, method)
                except ResolverError as err:
                    # If we have an error handler for resolver errors, add it as an operation.
                    # Otherwise treat it as any other error.
                    if self.resolver_error_handler is not None:
                        self._add_resolver_error_handler(method, path, err)
                    else:
                        self._handle_add_operation_error(path, method, err.exc_info)
                except Exception:
                    # All other relevant exceptions should be handled as well.
                    self._handle_add_operation_error(path, method, sys.exc_info())

    def _handle_add_operation_error(self, path, method, exc_info):
        url = '{base_path}{path}'.format(base_path=self.base_path, path=path)
        error_msg = 'Failed to add operation for {method} {url}'.format(
            method=method.upper(),
            url=url)
        if self.debug:
            logger.exception(error_msg)
        else:
            logger.error(error_msg)
            _type, value, traceback = exc_info
            raise value.with_traceback(traceback)

    @classmethod
    @abc.abstractmethod
    def get_request(self, *args, **kwargs):
        """
        This method converts the user framework request to a ConnexionRequest.
        """

    @classmethod
    @abc.abstractmethod
    def get_response(self, response, mimetype=None, request=None):
        """
        This method converts a handler response to a framework response.
        This method should just retrieve response from handler then call `cls._get_response`.
        It is mainly here to handle AioHttp async handler.
        :param response: A response to cast (tuple, framework response, etc).
        :param mimetype: The response mimetype.
        :type mimetype: Union[None, str]
        :param request: The request associated with this response (the user framework request).
        """

    @classmethod
    def _get_response(cls, response, mimetype=None, extra_context=None):
        """
        This method converts a handler response to a framework response.
        The response can be a ConnexionResponse, an operation handler, a framework response or a tuple.
        Other type than ConnexionResponse are handled by `cls._response_from_handler`
        :param response: A response to cast (tuple, framework response, etc).
        :param mimetype: The response mimetype.
        :type mimetype: Union[None, str]
        :param extra_context: dict of extra details, like url, to include in logs
        :type extra_context: Union[None, dict]
        """
        if extra_context is None:
            extra_context = {}
        logger.debug('Getting data and status code',
                     extra={
                         'data': response,
                         'data_type': type(response),
                         **extra_context
                     })

        if isinstance(response, ConnexionResponse):
            framework_response = cls._connexion_to_framework_response(response, mimetype, extra_context)
        else:
            framework_response = cls._response_from_handler(response, mimetype, extra_context)

        logger.debug('Got framework response',
                     extra={
                         'response': framework_response,
                         'response_type': type(framework_response),
                         **extra_context
                     })
        return framework_response

    @classmethod
    def _response_from_handler(cls, response, mimetype, extra_context=None):
        """
        Create a framework response from the operation handler data.
        An operation handler can return:
        - a framework response
        - a body (str / binary / dict / list), a response will be created
            with a status code 200 by default and empty headers.
        - a tuple of (body: str, status_code: int)
        - a tuple of (body: str, status_code: int, headers: dict)
        :param response: A response from an operation handler.
        :type response Union[Response, str, Tuple[str,], Tuple[str, int], Tuple[str, int, dict]]
        :param mimetype: The response mimetype.
        :type mimetype: str
        :param extra_context: dict of extra details, like url, to include in logs
        :type extra_context: Union[None, dict]
        :return A framework response.
        :rtype Response
        """
        if cls._is_framework_response(response):
            return response

        if isinstance(response, tuple):
            len_response = len(response)
            if len_response == 1:
                data, = response
                return cls._build_response(mimetype=mimetype, data=data, extra_context=extra_context)
            if len_response == 2:
                if isinstance(response[1], (int, Enum)):
                    data, status_code = response
                    return cls._build_response(mimetype=mimetype, data=data, status_code=status_code, extra_context=extra_context)
                else:
                    data, headers = response
                return cls._build_response(mimetype=mimetype, data=data, headers=headers, extra_context=extra_context)
            elif len_response == 3:
                data, status_code, headers = response
                return cls._build_response(mimetype=mimetype, data=data, status_code=status_code, headers=headers, extra_context=extra_context)
            else:
                raise TypeError(
                    'The view function did not return a valid response tuple.'
                    ' The tuple must have the form (body), (body, status, headers),'
                    ' (body, status), or (body, headers).'
                )
        else:
            return cls._build_response(mimetype=mimetype, data=response, extra_context=extra_context)

    @classmethod
    def get_connexion_response(cls, response, mimetype=None):
        """ Cast framework dependent response to ConnexionResponse used for schema validation """
        if isinstance(response, ConnexionResponse):
            # If body in ConnexionResponse is not byte, it may not pass schema validation.
            # In this case, rebuild response with aiohttp to have consistency
            if response.body is None or isinstance(response.body, bytes):
                return response
            else:
                response = cls._build_response(
                    data=response.body,
                    mimetype=mimetype,
                    content_type=response.content_type,
                    headers=response.headers,
                    status_code=response.status_code
                )

        if not cls._is_framework_response(response):
            response = cls._response_from_handler(response, mimetype)
        return cls._framework_to_connexion_response(response=response, mimetype=mimetype)

    @classmethod
    @abc.abstractmethod
    def _is_framework_response(cls, response):
        """ Return True if `response` is a framework response class """

    @classmethod
    @abc.abstractmethod
    def _framework_to_connexion_response(cls, response, mimetype):
        """ Cast framework response class to ConnexionResponse used for schema validation """

    @classmethod
    @abc.abstractmethod
    def _connexion_to_framework_response(cls, response, mimetype, extra_context=None):
        """ Cast ConnexionResponse to framework response class """

    @classmethod
    @abc.abstractmethod
    def _build_response(cls, data, mimetype, content_type=None, status_code=None, headers=None, extra_context=None):
        """
        Create a framework response from the provided arguments.
        :param data: Body data.
        :param content_type: The response mimetype.
        :type content_type: str
        :param content_type: The response status code.
        :type status_code: int
        :param headers: The response status code.
        :type headers: Union[Iterable[Tuple[str, str]], Dict[str, str]]
        :param extra_context: dict of extra details, like url, to include in logs
        :type extra_context: Union[None, dict]
        :return A framework response.
        :rtype Response
        """

    @classmethod
    def _prepare_body_and_status_code(cls, data, mimetype, status_code=None, extra_context=None):
        if data is NoContent:
            data = None

        if status_code is None:
            if data is None:
                status_code = 204
                mimetype = None
            else:
                status_code = 200
        elif hasattr(status_code, "value"):
            # If we got an enum instead of an int, extract the value.
            status_code = status_code.value

        if data is not None:
            body, mimetype = cls._serialize_data(data, mimetype)
        else:
            body = data

        if extra_context is None:
            extra_context = {}
        logger.debug('Prepared body and status code (%d)',
                     status_code,
                     extra={
                         'body': body,
                         **extra_context
                     })

        return body, status_code, mimetype

    @classmethod
    def _serialize_data(cls, data, mimetype):
        # TODO: Harmonize with flask_api. Currently this is the backwards compatible with aiohttp_api._cast_body.
        if not isinstance(data, bytes):
            if isinstance(mimetype, str) and is_json_mimetype(mimetype):
                body = cls.jsonifier.dumps(data)
            elif isinstance(data, str):
                body = data
            else:
                warnings.warn(
                    "Implicit (aiohttp) serialization with str() will change in the next major version. "
                    "This is triggered because a non-JSON response body is being stringified. "
                    "This will be replaced by something that is mimetype-specific and may "
                    "serialize some things as JSON or throw an error instead of silently "
                    "stringifying unknown response bodies. "
                    "Please make sure to specify media/mime types in your specs.",
                    FutureWarning  # a Deprecation targeted at application users.
                )
                body = str(data)
        else:
            body = data
        return body, mimetype

    def json_loads(self, data):
        return self.jsonifier.loads(data)

    @classmethod
    def _set_jsonifier(cls):
        cls.jsonifier = Jsonifier()
