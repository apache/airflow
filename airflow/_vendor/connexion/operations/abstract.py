import abc
import logging

from airflow._vendor.connexion.operations.secure import SecureOperation

from ..decorators.metrics import UWSGIMetricsCollector
from ..decorators.parameter import parameter_to_arg
from ..decorators.produces import BaseSerializer, Produces
from ..decorators.response import ResponseValidator
from ..decorators.validation import ParameterValidator, RequestBodyValidator
from ..utils import all_json, is_nullable

logger = logging.getLogger('connexion.operations.abstract')

DEFAULT_MIMETYPE = 'application/json'

VALIDATOR_MAP = {
    'parameter': ParameterValidator,
    'body': RequestBodyValidator,
    'response': ResponseValidator,
}


class AbstractOperation(SecureOperation, metaclass=abc.ABCMeta):

    """
    An API routes requests to an Operation by a (path, method) pair.
    The operation uses a resolver to resolve its handler function.
    We use the provided spec to do a bunch of heavy lifting before
    (and after) we call security_schemes handler.
    The registered handler function ends up looking something like:

        @secure_endpoint
        @validate_inputs
        @deserialize_function_inputs
        @serialize_function_outputs
        @validate_outputs
        def user_provided_handler_function(important, stuff):
            if important:
                serious_business(stuff)
    """
    def __init__(self, api, method, path, operation, resolver,
                 app_security=None, security_schemes=None,
                 validate_responses=False, strict_validation=False,
                 randomize_endpoint=None, validator_map=None,
                 pythonic_params=False, uri_parser_class=None,
                 pass_context_arg_name=None):
        """
        :param api: api that this operation is attached to
        :type api: apis.AbstractAPI
        :param method: HTTP method
        :type method: str
        :param path:
        :type path: str
        :param operation: swagger operation object
        :type operation: dict
        :param resolver: Callable that maps operationID to a function
        :param app_produces: list of content types the application can return by default
        :param app_security: list of security rules the application uses by default
        :type app_security: list
        :param security_schemes: `Security Definitions Object
            <https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#security-definitions-object>`_
        :type security_schemes: dict
        :param validate_responses: True enables validation. Validation errors generate HTTP 500 responses.
        :type validate_responses: bool
        :param strict_validation: True enables validation on invalid request parameters
        :type strict_validation: bool
        :param randomize_endpoint: number of random characters to append to operation name
        :type randomize_endpoint: integer
        :param validator_map: Custom validators for the types "parameter", "body" and "response".
        :type validator_map: dict
        :param pythonic_params: When True CamelCase parameters are converted to snake_case and an underscore is appended
        to any shadowed built-ins
        :type pythonic_params: bool
        :param uri_parser_class: class to use for uri parsing
        :type uri_parser_class: AbstractURIParser
        :param pass_context_arg_name: If not None will try to inject the request context to the function using this
        name.
        :type pass_context_arg_name: str|None
        """
        self._api = api
        self._method = method
        self._path = path
        self._operation = operation
        self._resolver = resolver
        self._security = app_security
        self._security_schemes = security_schemes
        self._validate_responses = validate_responses
        self._strict_validation = strict_validation
        self._pythonic_params = pythonic_params
        self._uri_parser_class = uri_parser_class
        self._pass_context_arg_name = pass_context_arg_name
        self._randomize_endpoint = randomize_endpoint

        self._operation_id = self._operation.get("operationId")
        self._resolution = resolver.resolve(self)
        self._operation_id = self._resolution.operation_id

        self._responses = self._operation.get("responses", {})

        self._validator_map = dict(VALIDATOR_MAP)
        self._validator_map.update(validator_map or {})

    @property
    def method(self):
        """
        The HTTP method for this operation (ex. GET, POST)
        """
        return self._method

    @property
    def path(self):
        """
        The path of the operation, relative to the API base path
        """
        return self._path

    @property
    def responses(self):
        """
        Returns the responses for this operation
        """
        return self._responses

    @property
    def validator_map(self):
        """
        Validators to use for parameter, body, and response validation
        """
        return self._validator_map

    @property
    def operation_id(self):
        """
        The operation id used to indentify the operation internally to the app
        """
        return self._operation_id

    @property
    def randomize_endpoint(self):
        """
        number of random digits to generate and append to the operation_id.
        """
        return self._randomize_endpoint

    @property
    def router_controller(self):
        """
        The router controller to use (python module where handler functions live)
        """
        return self._router_controller

    @property
    def strict_validation(self):
        """
        If True, validate all requests against the spec
        """
        return self._strict_validation

    @property
    def pythonic_params(self):
        """
        If True, convert CamelCase into pythonic_variable_names
        """
        return self._pythonic_params

    @property
    def validate_responses(self):
        """
        If True, check the response against the response schema, and return an
        error if the response does not validate.
        """
        return self._validate_responses

    @staticmethod
    def _get_file_arguments(files, arguments, has_kwargs=False):
        return {k: v for k, v in files.items() if k in arguments or has_kwargs}

    @abc.abstractmethod
    def _get_val_from_param(self, value, query_defn):
        """
        Convert input parameters into the correct type
        """

    def _query_args_helper(self, query_defns, query_arguments,
                           function_arguments, has_kwargs, sanitize):
        res = {}
        for key, value in query_arguments.items():
            key = sanitize(key)
            if not has_kwargs and key not in function_arguments:
                logger.debug("Query Parameter '%s' not in function arguments", key)
            else:
                logger.debug("Query Parameter '%s' in function arguments", key)
                try:
                    query_defn = query_defns[key]
                except KeyError:  # pragma: no cover
                    logger.error("Function argument '{}' not defined in specification".format(key))
                else:
                    logger.debug('%s is a %s', key, query_defn)
                    res.update({key: self._get_val_from_param(value, query_defn)})
        return res

    @abc.abstractmethod
    def _get_query_arguments(self, query, arguments, has_kwargs, sanitize):
        """
        extract handler function arguments from the query parameters
        """

    @abc.abstractmethod
    def _get_body_argument(self, body, arguments, has_kwargs, sanitize):
        """
        extract handler function arguments from the request body
        """

    def _get_path_arguments(self, path_params, sanitize):
        """
        extract handler function arguments from path parameters
        """
        kwargs = {}
        path_defns = {p["name"]: p for p in self.parameters if p["in"] == "path"}
        for key, value in path_params.items():
            sanitized_key = sanitize(key)
            if key in path_defns:
                kwargs[sanitized_key] = self._get_val_from_param(value, path_defns[key])
            else:  # Assume path params mechanism used for injection
                kwargs[sanitized_key] = value
        return kwargs

    @abc.abstractproperty
    def parameters(self):
        """
        Returns the parameters for this operation
        """

    @abc.abstractproperty
    def produces(self):
        """
        Content-Types that the operation produces
        """

    @abc.abstractproperty
    def consumes(self):
        """
        Content-Types that the operation consumes
        """

    @abc.abstractproperty
    def body_schema(self):
        """
        The body schema definition for this operation.
        """

    @abc.abstractproperty
    def body_definition(self):
        """
        The body definition for this operation.
        :rtype: dict
        """

    def get_arguments(self, path_params, query_params, body, files, arguments,
                      has_kwargs, sanitize):
        """
        get arguments for handler function
        """
        ret = {}
        ret.update(self._get_path_arguments(path_params, sanitize))
        ret.update(self._get_query_arguments(query_params, arguments,
                                             has_kwargs, sanitize))

        if self.method.upper() in ["PATCH", "POST", "PUT"]:
            ret.update(self._get_body_argument(body, arguments,
                                               has_kwargs, sanitize))
            ret.update(self._get_file_arguments(files, arguments, has_kwargs))
        return ret

    def response_definition(self, status_code=None,
                            content_type=None):
        """
        response definition for this endpoint
        """
        content_type = content_type or self.get_mimetype()
        response_definition = self.responses.get(
            str(status_code),
            self.responses.get("default", {})
        )
        return response_definition

    @abc.abstractmethod
    def response_schema(self, status_code=None, content_type=None):
        """
        response schema for this endpoint
        """

    @abc.abstractmethod
    def example_response(self, status_code=None, content_type=None):
        """
        Returns an example from the spec
        """

    @abc.abstractmethod
    def get_path_parameter_types(self):
        """
        Returns the types for parameters in the path
        """

    @abc.abstractmethod
    def with_definitions(self, schema):
        """
        Returns the given schema, but with the definitions from the spec
        attached. This allows any remaining references to be resolved by a
        validator (for example).
        """

    def get_mimetype(self):
        """
        If the endpoint has no 'produces' then the default is
        'application/json'.

        :rtype str
        """
        if all_json(self.produces):
            try:
                return self.produces[0]
            except IndexError:
                return DEFAULT_MIMETYPE
        elif len(self.produces) == 1:
            return self.produces[0]
        else:
            return DEFAULT_MIMETYPE

    @property
    def _uri_parsing_decorator(self):
        """
        Returns a decorator that parses request data and handles things like
        array types, and duplicate parameter definitions.
        """
        return self._uri_parser_class(self.parameters, self.body_definition)

    @property
    def function(self):
        """
        Operation function with decorators

        :rtype: types.FunctionType
        """
        function = parameter_to_arg(
            self, self._resolution.function, self.pythonic_params,
            self._pass_context_arg_name
        )

        if self.validate_responses:
            logger.debug('... Response validation enabled.')
            response_decorator = self.__response_validation_decorator
            logger.debug('... Adding response decorator (%r)', response_decorator)
            function = response_decorator(function)

        produces_decorator = self.__content_type_decorator
        logger.debug('... Adding produces decorator (%r)', produces_decorator)
        function = produces_decorator(function)

        for validation_decorator in self.__validation_decorators:
            function = validation_decorator(function)

        uri_parsing_decorator = self._uri_parsing_decorator
        function = uri_parsing_decorator(function)

        # NOTE: the security decorator should be applied last to check auth before anything else :-)
        security_decorator = self.security_decorator
        logger.debug('... Adding security decorator (%r)', security_decorator)
        function = security_decorator(function)

        function = self._request_response_decorator(function)

        if UWSGIMetricsCollector.is_available():  # pragma: no cover
            decorator = UWSGIMetricsCollector(self.path, self.method)
            function = decorator(function)

        return function

    @property
    def __content_type_decorator(self):
        """
        Get produces decorator.

        If the operation mimetype format is json then the function return value is jsonified

        From Swagger Specification:

        **Produces**

        A list of MIME types the operation can produce. This overrides the produces definition at the Swagger Object.
        An empty value MAY be used to clear the global definition.

        :rtype: types.FunctionType
        """

        logger.debug('... Produces: %s', self.produces, extra=vars(self))

        mimetype = self.get_mimetype()
        if all_json(self.produces):  # endpoint will return json
            logger.debug('... Produces json', extra=vars(self))
            # TODO: Refactor this.
            return lambda f: f

        elif len(self.produces) == 1:
            logger.debug('... Produces %s', mimetype, extra=vars(self))
            decorator = Produces(mimetype)
            return decorator

        else:
            return BaseSerializer()

    @property
    def __validation_decorators(self):
        """
        :rtype: types.FunctionType
        """
        ParameterValidator = self.validator_map['parameter']
        RequestBodyValidator = self.validator_map['body']
        if self.parameters:
            yield ParameterValidator(self.parameters,
                                     self.api,
                                     strict_validation=self.strict_validation)
        if self.body_schema:
            yield RequestBodyValidator(self.body_schema, self.consumes, self.api,
                                       is_nullable(self.body_definition),
                                       strict_validation=self.strict_validation)

    @property
    def __response_validation_decorator(self):
        """
        Get a decorator for validating the generated Response.
        :rtype: types.FunctionType
        """
        ResponseValidator = self.validator_map['response']
        return ResponseValidator(self, self.get_mimetype())

    def json_loads(self, data):
        """
        A wrapper for calling the API specific JSON loader.

        :param data: The JSON data in textual form.
        :type data: bytes
        """
        return self.api.json_loads(data)
