# Decorators to change the return type of endpoints
import functools
import logging

from jsonschema import ValidationError

from ..exceptions import (NonConformingResponseBody,
                          NonConformingResponseHeaders)
from ..utils import all_json, has_coroutine
from .decorator import BaseDecorator
from .validation import ResponseBodyValidator

logger = logging.getLogger('connexion.decorators.response')


class ResponseValidator(BaseDecorator):
    def __init__(self, operation, mimetype, validator=None):
        """
        :type operation: Operation
        :type mimetype: str
        :param validator: Validator class that should be used to validate passed data
                          against API schema. Default is jsonschema.Draft4Validator.
        :type validator: jsonschema.IValidator
        """
        self.operation = operation
        self.mimetype = mimetype
        self.validator = validator

    def validate_response(self, data, status_code, headers, url):
        """
        Validates the Response object based on what has been declared in the specification.
        Ensures the response body matches the declated schema.
        :type data: dict
        :type status_code: int
        :type headers: dict
        :rtype bool | None
        """
        # check against returned header, fall back to expected mimetype
        content_type = headers.get("Content-Type", self.mimetype)
        content_type = content_type.rsplit(";", 1)[0]  # remove things like utf8 metadata

        response_definition = self.operation.response_definition(str(status_code), content_type)
        response_schema = self.operation.response_schema(str(status_code), content_type)

        if self.is_json_schema_compatible(response_schema):
            v = ResponseBodyValidator(response_schema, validator=self.validator)
            try:
                data = self.operation.json_loads(data)
                v.validate_schema(data, url)
            except ValidationError as e:
                raise NonConformingResponseBody(message=str(e))

        if response_definition and response_definition.get("headers"):
            # converting to set is needed to support python 2.7
            response_definition_header_keys = set(response_definition.get("headers").keys())
            header_keys = set(headers.keys())
            missing_keys = response_definition_header_keys - header_keys
            if missing_keys:
                pretty_list = ', '.join(missing_keys)
                msg = ("Keys in header don't match response specification. "
                       "Difference: {0}").format(pretty_list)
                raise NonConformingResponseHeaders(message=msg)
        return True

    def is_json_schema_compatible(self, response_schema):
        """
        Verify if the specified operation responses are JSON schema
        compatible.

        All operations that specify a JSON schema and have content
        type "application/json" or "text/plain" can be validated using
        json_schema package.

        :type response_schema: dict
        :rtype bool
        """
        if not response_schema:
            return False
        return all_json([self.mimetype]) or self.mimetype == 'text/plain'

    def __call__(self, function):
        """
        :type function: types.FunctionType
        :rtype: types.FunctionType
        """

        def _wrapper(request, response):
            connexion_response = \
                self.operation.api.get_connexion_response(response, self.mimetype)
            self.validate_response(
                connexion_response.body, connexion_response.status_code,
                connexion_response.headers, request.url)

            return response

        if has_coroutine(function):
            from .coroutine_wrappers import get_response_validator_wrapper
            wrapper = get_response_validator_wrapper(function, _wrapper)

        else:  # pragma: 3 no cover
            @functools.wraps(function)
            def wrapper(request):
                response = function(request)
                return _wrapper(request, response)

        return wrapper

    def __repr__(self):
        """
        :rtype: str
        """
        return '<ResponseValidator>'  # pragma: no cover
