import logging
from copy import copy, deepcopy

from airflow._vendor.connexion.operations.abstract import AbstractOperation

from ..decorators.uri_parsing import OpenAPIURIParser
from ..utils import deep_get, deep_merge, is_null, is_nullable, make_type

logger = logging.getLogger("connexion.operations.openapi3")


class OpenAPIOperation(AbstractOperation):

    """
    A single API operation on a path.
    """

    def __init__(self, api, method, path, operation, resolver, path_parameters=None,
                 app_security=None, components=None, validate_responses=False,
                 strict_validation=False, randomize_endpoint=None, validator_map=None,
                 pythonic_params=False, uri_parser_class=None, pass_context_arg_name=None):
        """
        This class uses the OperationID identify the module and function that will handle the operation

        From Swagger Specification:

        **OperationID**

        A friendly name for the operation. The id MUST be unique among all operations described in the API.
        Tools and libraries MAY use the operation id to uniquely identify an operation.

        :param method: HTTP method
        :type method: str
        :param path:
        :type path: str
        :param operation: swagger operation object
        :type operation: dict
        :param resolver: Callable that maps operationID to a function
        :param path_parameters: Parameters defined in the path level
        :type path_parameters: list
        :param app_security: list of security rules the application uses by default
        :type app_security: list
        :param components: `Components Object
            <https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.1.md#componentsObject>`_
        :type components: dict
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
        self.components = components or {}

        def component_get(oas3_name):
            return self.components.get(oas3_name, {})

        # operation overrides globals
        security_schemes = component_get('securitySchemes')
        app_security = operation.get('security', app_security)
        uri_parser_class = uri_parser_class or OpenAPIURIParser

        self._router_controller = operation.get('x-openapi-router-controller')

        super(OpenAPIOperation, self).__init__(
            api=api,
            method=method,
            path=path,
            operation=operation,
            resolver=resolver,
            app_security=app_security,
            security_schemes=security_schemes,
            validate_responses=validate_responses,
            strict_validation=strict_validation,
            randomize_endpoint=randomize_endpoint,
            validator_map=validator_map,
            pythonic_params=pythonic_params,
            uri_parser_class=uri_parser_class,
            pass_context_arg_name=pass_context_arg_name
        )

        self._definitions_map = {
            'components': {
                'schemas': component_get('schemas'),
                'examples': component_get('examples'),
                'requestBodies': component_get('requestBodies'),
                'parameters': component_get('parameters'),
                'securitySchemes': component_get('securitySchemes'),
                'responses': component_get('responses'),
                'headers': component_get('headers'),
            }
        }

        self._request_body = operation.get('requestBody', {})

        self._parameters = operation.get('parameters', [])
        if path_parameters:
            self._parameters += path_parameters

        self._responses = operation.get('responses', {})

        # TODO figure out how to support multiple mimetypes
        # NOTE we currently just combine all of the possible mimetypes,
        #      but we need to refactor to support mimetypes by response code
        response_content_types = []
        for _, defn in self._responses.items():
            response_content_types += defn.get('content', {}).keys()
        self._produces = response_content_types or ['application/json']

        request_content = self._request_body.get('content', {})
        self._consumes = list(request_content.keys()) or ['application/json']

        logger.debug('consumes: %s' % self.consumes)
        logger.debug('produces: %s' % self.produces)

    @classmethod
    def from_spec(cls, spec, api, path, method, resolver, *args, **kwargs):
        return cls(
            api,
            method,
            path,
            spec.get_operation(path, method),
            resolver=resolver,
            path_parameters=spec.get_path_params(path),
            app_security=spec.security,
            components=spec.components,
            *args,
            **kwargs
        )

    @property
    def request_body(self):
        return self._request_body

    @property
    def parameters(self):
        return self._parameters

    @property
    def consumes(self):
        return self._consumes

    @property
    def produces(self):
        return self._produces

    def with_definitions(self, schema):
        if self.components:
            schema['schema']['components'] = self.components
        return schema

    def response_schema(self, status_code=None, content_type=None):
        response_definition = self.response_definition(
            status_code, content_type
        )
        content_definition = response_definition.get("content", response_definition)
        content_definition = content_definition.get(content_type, content_definition)
        if "schema" in content_definition:
            return self.with_definitions(content_definition).get("schema", {})
        return {}

    def example_response(self, status_code=None, content_type=None):
        """
        Returns example response from spec
        """
        # simply use the first/lowest status code, this is probably 200 or 201
        status_code = status_code or sorted(self._responses.keys())[0]

        content_type = content_type or self.get_mimetype()
        examples_path = [str(status_code), 'content', content_type, 'examples']
        example_path = [str(status_code), 'content', content_type, 'example']
        schema_example_path = [
            str(status_code), 'content', content_type, 'schema', 'example'
        ]
        schema_path = [str(status_code), 'content', content_type, 'schema']

        try:
            status_code = int(status_code)
        except ValueError:
            status_code = 200
        try:
            # TODO also use example header?
            return (
                list(deep_get(self._responses, examples_path).values())[0]['value'],
                status_code
            )
        except (KeyError, IndexError):
            pass
        try:
            return (deep_get(self._responses, example_path), status_code)
        except KeyError:
            pass
        try:
            return (deep_get(self._responses, schema_example_path),
                    status_code)
        except KeyError:
            pass

        try:
            return (self._nested_example(deep_get(self._responses, schema_path)),
                    status_code)
        except KeyError:
            return (None, status_code)

    def _nested_example(self, schema):
        try:
            return schema["example"]
        except KeyError:
            pass
        try:
            # Recurse if schema is an object
            return {key: self._nested_example(value)
                    for (key, value) in schema["properties"].items()}
        except KeyError:
            pass
        try:
            # Recurse if schema is an array
            return [self._nested_example(schema["items"])]
        except KeyError:
            raise

    def get_path_parameter_types(self):
        types = {}
        path_parameters = (p for p in self.parameters if p["in"] == "path")
        for path_defn in path_parameters:
            path_schema = path_defn["schema"]
            if path_schema.get('type') == 'string' and path_schema.get('format') == 'path':
                # path is special case for type 'string'
                path_type = 'path'
            else:
                path_type = path_schema.get('type')
            types[path_defn['name']] = path_type
        return types

    @property
    def body_schema(self):
        """
        The body schema definition for this operation.
        """
        return self.body_definition.get('schema', {})

    @property
    def body_definition(self):
        """
        The body complete definition for this operation.

        **There can be one "body" parameter at most.**

        :rtype: dict
        """
        if self._request_body:
            if len(self.consumes) > 1:
                logger.warning(
                    'this operation accepts multiple content types, using %s',
                    self.consumes[0])
            res = self._request_body.get('content', {}).get(self.consumes[0], {})
            return self.with_definitions(res)
        return {}

    def _get_body_argument(self, body, arguments, has_kwargs, sanitize):
        x_body_name = sanitize(self.body_schema.get('x-body-name', 'body'))
        if is_nullable(self.body_schema) and is_null(body):
            return {x_body_name: None}

        default_body = self.body_schema.get('default', {})
        body_props = {k: {"schema": v} for k, v
                      in self.body_schema.get("properties", {}).items()}

        # by OpenAPI specification `additionalProperties` defaults to `true`
        # see: https://github.com/OAI/OpenAPI-Specification/blame/3.0.2/versions/3.0.2.md#L2305
        additional_props = self.body_schema.get("additionalProperties", True)

        if body is None:
            body = deepcopy(default_body)

        if self.body_schema.get("type") != "object":
            if x_body_name in arguments or has_kwargs:
                return {x_body_name: body}
            return {}

        body_arg = deepcopy(default_body)
        body_arg.update(body or {})

        res = {}
        if body_props or additional_props:
            res = self._get_typed_body_values(body_arg, body_props, additional_props)

        if x_body_name in arguments or has_kwargs:
            return {x_body_name: res}
        return {}

    def _get_typed_body_values(self, body_arg, body_props, additional_props):
        """
        Return a copy of the provided body_arg dictionary
        whose values will have the appropriate types
        as defined in the provided schemas.

        :type body_arg: type dict
        :type body_props: dict
        :type additional_props: dict|bool
        :rtype: dict
        """
        additional_props_defn = {"schema": additional_props} if isinstance(additional_props, dict) else None
        res = {}

        for key, value in body_arg.items():
            try:
                prop_defn = body_props[key]
                res[key] = self._get_val_from_param(value, prop_defn)
            except KeyError:  # pragma: no cover
                if not additional_props:
                    logger.error("Body property '{}' not defined in body schema".format(key))
                    continue
                if additional_props_defn is not None:
                    value = self._get_val_from_param(value, additional_props_defn)
                res[key] = value

        return res

    def _build_default_obj_recursive(self, _properties, res):
        """ takes disparate and nested default keys, and builds up a default object
        """
        for key, prop in _properties.items():
            if 'default' in prop and key not in res:
                res[key] = copy(prop['default'])
            elif prop.get('type') == 'object' and 'properties' in prop:
                res.setdefault(key, {})
                res[key] = self._build_default_obj_recursive(prop['properties'], res[key])
        return res

    def _get_default_obj(self, schema):
        try:
            return deepcopy(schema["default"])
        except KeyError:
            _properties = schema.get("properties", {})
            return self._build_default_obj_recursive(_properties, {})

    def _get_query_defaults(self, query_defns):
        defaults = {}
        for k, v in query_defns.items():
            try:
                if v["schema"]["type"] == "object":
                    defaults[k] = self._get_default_obj(v["schema"])
                else:
                    defaults[k] = v["schema"]["default"]
            except KeyError:
                pass
        return defaults

    def _get_query_arguments(self, query, arguments, has_kwargs, sanitize):
        query_defns = {sanitize(p["name"]): p
                       for p in self.parameters
                       if p["in"] == "query"}
        default_query_params = self._get_query_defaults(query_defns)

        query_arguments = deepcopy(default_query_params)
        query_arguments = deep_merge(query_arguments, query)
        return self._query_args_helper(query_defns, query_arguments,
                                       arguments, has_kwargs, sanitize)

    def _get_val_from_param(self, value, query_defn):
        query_schema = query_defn["schema"]

        if is_nullable(query_schema) and is_null(value):
            return None

        if query_schema["type"] == "array":
            return [make_type(part, query_schema["items"]["type"]) for part in value]
        else:
            return make_type(value, query_schema["type"])
