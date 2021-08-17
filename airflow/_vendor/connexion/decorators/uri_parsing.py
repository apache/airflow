# Decorators to split query and path parameters
import abc
import functools
import logging
import re
import json
from .. import utils

from .decorator import BaseDecorator

logger = logging.getLogger('connexion.decorators.uri_parsing')

QUERY_STRING_DELIMITERS = {
    'spaceDelimited': ' ',
    'pipeDelimited': '|',
    'simple': ',',
    'form': ','
}


class AbstractURIParser(BaseDecorator, metaclass=abc.ABCMeta):
    parsable_parameters = ["query", "path"]

    def __init__(self, param_defns, body_defn):
        """
        a URI parser is initialized with parameter definitions.
        When called with a request object, it handles array types in the URI
        both in the path and query according to the spec.
        Some examples include:
         - https://mysite.fake/in/path/1,2,3/            # path parameters
         - https://mysite.fake/?in_query=a,b,c           # simple query params
         - https://mysite.fake/?in_query=a|b|c           # various separators
         - https://mysite.fake/?in_query=a&in_query=b,c  # complex query params
        """
        self._param_defns = {p["name"]: p
                             for p in param_defns
                             if p["in"] in self.parsable_parameters}
        self._body_schema = body_defn.get("schema", {})
        self._body_encoding = body_defn.get("encoding", {})

    @abc.abstractproperty
    def param_defns(self):
        """
        returns the parameter definitions by name
        """

    @abc.abstractproperty
    def param_schemas(self):
        """
        returns the parameter schemas by name
        """

    def __repr__(self):
        """
        :rtype: str
        """
        return "<{classname}>".format(
            classname=self.__class__.__name__)  # pragma: no cover

    @abc.abstractmethod
    def resolve_form(self, form_data):
        """ Resolve cases where form parameters are provided multiple times.
        """

    @abc.abstractmethod
    def resolve_query(self, query_data):
        """ Resolve cases where query parameters are provided multiple times.
        """

    @abc.abstractmethod
    def resolve_path(self, path):
        """ Resolve cases where path parameters include lists
        """

    @abc.abstractmethod
    def _resolve_param_duplicates(self, values, param_defn, _in):
        """ Resolve cases where query parameters are provided multiple times.
            For example, if the query string is '?a=1,2,3&a=4,5,6' the value of
            `a` could be "4,5,6", or "1,2,3" or "1,2,3,4,5,6" depending on the
            implementation.
        """

    @abc.abstractmethod
    def _split(self, value, param_defn, _in):
        """
        takes a string, a parameter definition, and a parameter type
        and returns an array that has been constructed according to
        the parameter definition.
        """

    def resolve_params(self, params, _in):
        """
        takes a dict of parameters, and resolves the values into
        the correct array type handling duplicate values, and splitting
        based on the collectionFormat defined in the spec.
        """
        resolved_param = {}
        for k, values in params.items():
            param_defn = self.param_defns.get(k)
            param_schema = self.param_schemas.get(k)

            if not (param_defn or param_schema):
                # rely on validation
                resolved_param[k] = values
                continue

            if _in == 'path':
                # multiple values in a path is impossible
                values = [values]

            if (param_schema is not None and param_schema['type'] == 'array'):
                # resolve variable re-assignment, handle explode
                values = self._resolve_param_duplicates(values, param_defn, _in)
                # handle array styles
                resolved_param[k] = self._split(values, param_defn, _in)
            else:
                resolved_param[k] = values[-1]

        return resolved_param

    def __call__(self, function):
        """
        :type function: types.FunctionType
        :rtype: types.FunctionType
        """

        @functools.wraps(function)
        def wrapper(request):
            def coerce_dict(md):
                """ MultiDict -> dict of lists
                """
                try:
                    return md.to_dict(flat=False)
                except AttributeError:
                    return dict(md.items())

            query = coerce_dict(request.query)
            path_params = coerce_dict(request.path_params)
            form = coerce_dict(request.form)

            request.query = self.resolve_query(query)
            request.path_params = self.resolve_path(path_params)
            request.form = self.resolve_form(form)
            response = function(request)
            return response

        return wrapper


class OpenAPIURIParser(AbstractURIParser):
    style_defaults = {"path": "simple", "header": "simple",
                      "query": "form", "cookie": "form",
                      "form": "form"}

    @property
    def param_defns(self):
        return self._param_defns

    @property
    def form_defns(self):
        return {k: v for k, v in self._body_schema.get('properties', {}).items()}

    @property
    def param_schemas(self):
        return {k: v.get('schema', {}) for k, v in self.param_defns.items()}

    def resolve_form(self, form_data):
        if self._body_schema is None or self._body_schema.get('type') != 'object':
            return form_data
        for k in form_data:
            encoding = self._body_encoding.get(k, {"style": "form"})
            defn = self.form_defns.get(k, {})
            # TODO support more form encoding styles
            form_data[k] = \
                self._resolve_param_duplicates(form_data[k], encoding, 'form')
            if defn and defn["type"] == "array":
                form_data[k] = self._split(form_data[k], encoding, 'form')
            elif 'contentType' in encoding and utils.all_json([encoding.get('contentType')]):
                form_data[k] = json.loads(form_data[k])
        return form_data

    @staticmethod
    def _make_deep_object(k, v):
        """ consumes keys, value pairs like (a[foo][bar], "baz")
            returns (a, {"foo": {"bar": "baz"}}}, is_deep_object)
        """
        root_key = k.split("[", 1)[0]
        if k == root_key:
            return (k, v, False)
        key_path = re.findall(r'\[([^\[\]]*)\]', k)
        root = prev = node = {}
        for k in key_path:
            node[k] = {}
            prev = node
            node = node[k]
        prev[k] = v[0]
        return (root_key, [root], True)

    def _preprocess_deep_objects(self, query_data):
        """ deep objects provide a way of rendering nested objects using query
            parameters.
        """
        deep = [self._make_deep_object(k, v) for k, v in query_data.items()]
        root_keys = [k for k, v, is_deep_object in deep]
        ret = dict.fromkeys(root_keys, [{}])
        for k, v, is_deep_object in deep:
            if is_deep_object:
                ret[k] = [utils.deep_merge(v[0], ret[k][0])]
            else:
                ret[k] = v
        return ret

    def resolve_query(self, query_data):
        query_data = self._preprocess_deep_objects(query_data)
        return self.resolve_params(query_data, 'query')

    def resolve_path(self, path_data):
        return self.resolve_params(path_data, 'path')

    @staticmethod
    def _resolve_param_duplicates(values, param_defn, _in):
        """ Resolve cases where query parameters are provided multiple times.
            The default behavior is to use the first-defined value.
            For example, if the query string is '?a=1,2,3&a=4,5,6' the value of
            `a` would be "4,5,6".
            However, if 'explode' is 'True' then the duplicate values
            are concatenated together and `a` would be "1,2,3,4,5,6".
        """
        default_style = OpenAPIURIParser.style_defaults[_in]
        style = param_defn.get('style', default_style)
        delimiter = QUERY_STRING_DELIMITERS.get(style, ',')
        is_form = (style == 'form')
        explode = param_defn.get('explode', is_form)
        if explode:
            return delimiter.join(values)

        # default to last defined value
        return values[-1]

    @staticmethod
    def _split(value, param_defn, _in):
        default_style = OpenAPIURIParser.style_defaults[_in]
        style = param_defn.get('style', default_style)
        delimiter = QUERY_STRING_DELIMITERS.get(style, ',')
        return value.split(delimiter)


class Swagger2URIParser(AbstractURIParser):
    """
    Adheres to the Swagger2 spec,
    Assumes the the last defined query parameter should be used.
    """
    parsable_parameters = ["query", "path", "formData"]

    @property
    def param_defns(self):
        return self._param_defns

    @property
    def param_schemas(self):
        return self._param_defns  # swagger2 conflates defn and schema

    def resolve_form(self, form_data):
        return self.resolve_params(form_data, 'form')

    def resolve_query(self, query_data):
        return self.resolve_params(query_data, 'query')

    def resolve_path(self, path_data):
        return self.resolve_params(path_data, 'path')

    @staticmethod
    def _resolve_param_duplicates(values, param_defn, _in):
        """ Resolve cases where query parameters are provided multiple times.
            The default behavior is to use the first-defined value.
            For example, if the query string is '?a=1,2,3&a=4,5,6' the value of
            `a` would be "4,5,6".
            However, if 'collectionFormat' is 'multi' then the duplicate values
            are concatenated together and `a` would be "1,2,3,4,5,6".
        """
        if param_defn.get('collectionFormat') == 'multi':
            return ','.join(values)
        # default to last defined value
        return values[-1]

    @staticmethod
    def _split(value, param_defn, _in):
        if param_defn.get("collectionFormat") == 'pipes':
            return value.split('|')
        return value.split(',')


class FirstValueURIParser(Swagger2URIParser):
    """
    Adheres to the Swagger2 spec
    Assumes that the first defined query parameter should be used
    """

    @staticmethod
    def _resolve_param_duplicates(values, param_defn, _in):
        """ Resolve cases where query parameters are provided multiple times.
            The default behavior is to use the first-defined value.
            For example, if the query string is '?a=1,2,3&a=4,5,6' the value of
            `a` would be "1,2,3".
            However, if 'collectionFormat' is 'multi' then the duplicate values
            are concatenated together and `a` would be "1,2,3,4,5,6".
        """
        if param_defn.get('collectionFormat') == 'multi':
            return ','.join(values)
        # default to first defined value
        return values[0]


class AlwaysMultiURIParser(Swagger2URIParser):
    """
    Does not adhere to the Swagger2 spec, but is backwards compatible with
    connexion behavior in version 1.4.2
    """

    @staticmethod
    def _resolve_param_duplicates(values, param_defn, _in):
        """ Resolve cases where query parameters are provided multiple times.
            The default behavior is to join all provided parameters together.
            For example, if the query string is '?a=1,2,3&a=4,5,6' the value of
            `a` would be "1,2,3,4,5,6".
        """
        if param_defn.get('collectionFormat') == 'pipes':
            return '|'.join(values)
        return ','.join(values)
