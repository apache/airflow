import abc
import copy
import pathlib

import jinja2
import yaml
from openapi_spec_validator.exceptions import OpenAPIValidationError
from urllib.parse import urlsplit

from .exceptions import InvalidSpecification
from .json_schema import resolve_refs
from .operations import OpenAPIOperation, Swagger2Operation
from .utils import deep_get

try:
    import collections.abc as collections_abc  # python 3.3+
except ImportError:
    import collections as collections_abc


NO_SPEC_VERSION_ERR_MSG = """Unable to get the spec version.
You are missing either '"swagger": "2.0"' or '"openapi": "3.0.0"'
from the top level of your spec."""


def canonical_base_path(base_path):
    """
    Make given "basePath" a canonical base URL which can be prepended to paths starting with "/".
    """
    return base_path.rstrip('/')


class Specification(collections_abc.Mapping):

    def __init__(self, raw_spec):
        self._raw_spec = copy.deepcopy(raw_spec)
        self._set_defaults(raw_spec)
        self._validate_spec(raw_spec)
        self._spec = resolve_refs(raw_spec)

    @classmethod
    @abc.abstractmethod
    def _set_defaults(cls, spec):
        """ set some default values in the spec
        """

    @classmethod
    @abc.abstractmethod
    def _validate_spec(cls, spec):
        """ validate spec against schema
        """

    def get_path_params(self, path):
        return deep_get(self._spec, ["paths", path]).get("parameters", [])

    def get_operation(self, path, method):
        return deep_get(self._spec, ["paths", path, method])

    @property
    def raw(self):
        return self._raw_spec

    @property
    def version(self):
        return self._get_spec_version(self._spec)

    @property
    def security(self):
        return self._spec.get('security')

    def __getitem__(self, k):
        return self._spec[k]

    def __iter__(self):
        return self._spec.__iter__()

    def __len__(self):
        return self._spec.__len__()

    @staticmethod
    def _load_spec_from_file(arguments, specification):
        """
        Loads a YAML specification file, optionally rendering it with Jinja2.
        Takes:
          arguments - passed to Jinja2 renderer
          specification - path to specification
        """
        arguments = arguments or {}

        with specification.open(mode='rb') as openapi_yaml:
            contents = openapi_yaml.read()
            try:
                openapi_template = contents.decode()
            except UnicodeDecodeError:
                openapi_template = contents.decode('utf-8', 'replace')

            openapi_string = jinja2.Template(openapi_template).render(**arguments)
            return yaml.safe_load(openapi_string)

    @classmethod
    def from_file(cls, spec, arguments=None):
        """
        Takes in a path to a YAML file, and returns a Specification
        """
        specification_path = pathlib.Path(spec)
        spec = cls._load_spec_from_file(arguments, specification_path)
        return cls.from_dict(spec)

    @staticmethod
    def _get_spec_version(spec):
        try:
            version_string = spec.get('openapi') or spec.get('swagger')
        except AttributeError:
            raise InvalidSpecification(NO_SPEC_VERSION_ERR_MSG)
        if version_string is None:
            raise InvalidSpecification(NO_SPEC_VERSION_ERR_MSG)
        try:
            version_tuple = tuple(map(int, version_string.split(".")))
        except TypeError:
            err = ('Unable to convert version string to semantic version tuple: '
                   '{version_string}.')
            err = err.format(version_string=version_string)
            raise InvalidSpecification(err)
        return version_tuple

    @classmethod
    def from_dict(cls, spec):
        """
        Takes in a dictionary, and returns a Specification
        """
        def enforce_string_keys(obj):
            # YAML supports integer keys, but JSON does not
            if isinstance(obj, dict):
                return {
                    str(k): enforce_string_keys(v)
                    for k, v
                    in obj.items()
                }
            return obj

        spec = enforce_string_keys(spec)
        version = cls._get_spec_version(spec)
        if version < (3, 0, 0):
            return Swagger2Specification(spec)
        return OpenAPISpecification(spec)

    def clone(self):
        return type(self)(copy.deepcopy(self._raw_spec))

    @classmethod
    def load(cls, spec, arguments=None):
        if not isinstance(spec, dict):
            return cls.from_file(spec, arguments=arguments)
        return cls.from_dict(spec)

    def with_base_path(self, base_path):
        new_spec = self.clone()
        new_spec.base_path = base_path
        return new_spec


class Swagger2Specification(Specification):
    yaml_name = 'swagger.yaml'
    operation_cls = Swagger2Operation

    @classmethod
    def _set_defaults(cls, spec):
        spec.setdefault('produces', [])
        spec.setdefault('consumes', ['application/json'])  # type: List[str]
        spec.setdefault('definitions', {})
        spec.setdefault('parameters', {})
        spec.setdefault('responses', {})

    @property
    def produces(self):
        return self._spec['produces']

    @property
    def consumes(self):
        return self._spec['consumes']

    @property
    def definitions(self):
        return self._spec['definitions']

    @property
    def parameter_definitions(self):
        return self._spec['parameters']

    @property
    def response_definitions(self):
        return self._spec['responses']

    @property
    def security_definitions(self):
        return self._spec.get('securityDefinitions', {})

    @property
    def base_path(self):
        return canonical_base_path(self._spec.get('basePath', ''))

    @base_path.setter
    def base_path(self, base_path):
        base_path = canonical_base_path(base_path)
        self._raw_spec['basePath'] = base_path
        self._spec['basePath'] = base_path

    @classmethod
    def _validate_spec(cls, spec):
        from openapi_spec_validator import validate_v2_spec as validate_spec
        try:
            validate_spec(spec)
        except OpenAPIValidationError as e:
            raise InvalidSpecification.create_from(e)


class OpenAPISpecification(Specification):
    yaml_name = 'openapi.yaml'
    operation_cls = OpenAPIOperation

    @classmethod
    def _set_defaults(cls, spec):
        spec.setdefault('components', {})

    @property
    def security_definitions(self):
        return self._spec['components'].get('securitySchemes', {})

    @property
    def components(self):
        return self._spec['components']

    @classmethod
    def _validate_spec(cls, spec):
        from openapi_spec_validator import validate_v3_spec as validate_spec
        try:
            validate_spec(spec)
        except OpenAPIValidationError as e:
            raise InvalidSpecification.create_from(e)

    @property
    def base_path(self):
        servers = self._spec.get('servers', [])
        try:
            # assume we're the first server in list
            server = copy.deepcopy(servers[0])
            server_vars = server.pop("variables", {})
            server['url'] = server['url'].format(
                **{k: v['default'] for k, v
                   in server_vars.items()}
            )
            base_path = urlsplit(server['url']).path
        except IndexError:
            base_path = ''
        return canonical_base_path(base_path)

    @base_path.setter
    def base_path(self, base_path):
        base_path = canonical_base_path(base_path)
        user_servers = [{'url': base_path}]
        self._raw_spec['servers'] = user_servers
        self._spec['servers'] = user_servers
