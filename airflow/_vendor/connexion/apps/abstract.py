import abc
import logging
import pathlib

from ..options import ConnexionOptions
from ..resolver import Resolver

logger = logging.getLogger('connexion.app')


class AbstractApp(metaclass=abc.ABCMeta):
    def __init__(self, import_name, api_cls, port=None, specification_dir='',
                 host=None, server=None, server_args=None, arguments=None, auth_all_paths=False, debug=None,
                 resolver=None, options=None, skip_error_handlers=False):
        """
        :param import_name: the name of the application package
        :type import_name: str
        :param host: the host interface to bind on.
        :type host: str
        :param port: port to listen to
        :type port: int
        :param specification_dir: directory where to look for specifications
        :type specification_dir: pathlib.Path | str
        :param server: which wsgi server to use
        :type server: str | None
        :param server_args: dictionary of arguments which are then passed to appropriate http server (Flask or aio_http)
        :type server_args: dict | None
        :param arguments: arguments to replace on the specification
        :type arguments: dict | None
        :param auth_all_paths: whether to authenticate not defined paths
        :type auth_all_paths: bool
        :param debug: include debugging information
        :type debug: bool
        :param resolver: Callable that maps operationID to a function
        """
        self.port = port
        self.host = host
        self.debug = debug
        self.resolver = resolver
        self.import_name = import_name
        self.arguments = arguments or {}
        self.api_cls = api_cls
        self.resolver_error = None

        # Options
        self.auth_all_paths = auth_all_paths

        self.options = ConnexionOptions(options)

        self.server = server
        self.server_args = dict() if server_args is None else server_args
        self.app = self.create_app()

        # we get our application root path to avoid duplicating logic
        self.root_path = self.get_root_path()
        logger.debug('Root Path: %s', self.root_path)

        specification_dir = pathlib.Path(specification_dir)  # Ensure specification dir is a Path
        if specification_dir.is_absolute():
            self.specification_dir = specification_dir
        else:
            self.specification_dir = self.root_path / specification_dir

        logger.debug('Specification directory: %s', self.specification_dir)

        if not skip_error_handlers:
            logger.debug('Setting error handlers')
            self.set_errors_handlers()

    @abc.abstractmethod
    def create_app(self):
        """
        Creates the user framework application
        """

    @abc.abstractmethod
    def get_root_path(self):
        """
        Gets the root path of the user framework application
        """

    @abc.abstractmethod
    def set_errors_handlers(self):
        """
        Sets all errors handlers of the user framework application
        """

    def add_api(self, specification, base_path=None, arguments=None,
                auth_all_paths=None, validate_responses=False,
                strict_validation=False, resolver=None, resolver_error=None,
                pythonic_params=False, pass_context_arg_name=None, options=None,
                validator_map=None):
        """
        Adds an API to the application based on a swagger file or API dict

        :param specification: swagger file with the specification | specification dict
        :type specification: pathlib.Path or str or dict
        :param base_path: base path where to add this api
        :type base_path: str | None
        :param arguments: api version specific arguments to replace on the specification
        :type arguments: dict | None
        :param auth_all_paths: whether to authenticate not defined paths
        :type auth_all_paths: bool
        :param validate_responses: True enables validation. Validation errors generate HTTP 500 responses.
        :type validate_responses: bool
        :param strict_validation: True enables validation on invalid request parameters
        :type strict_validation: bool
        :param resolver: Operation resolver.
        :type resolver: Resolver | types.FunctionType
        :param resolver_error: If specified, turns ResolverError into error
            responses with the given status code.
        :type resolver_error: int | None
        :param pythonic_params: When True CamelCase parameters are converted to snake_case
        :type pythonic_params: bool
        :param options: New style options dictionary.
        :type options: dict | None
        :param pass_context_arg_name: Name of argument in handler functions to pass request context to.
        :type pass_context_arg_name: str | None
        :param validator_map: map of validators
        :type validator_map: dict
        :rtype: AbstractAPI
        """
        # Turn the resolver_error code into a handler object
        self.resolver_error = resolver_error
        resolver_error_handler = None
        if self.resolver_error is not None:
            resolver_error_handler = self._resolver_error_handler

        resolver = resolver or self.resolver
        resolver = Resolver(resolver) if hasattr(resolver, '__call__') else resolver

        auth_all_paths = auth_all_paths if auth_all_paths is not None else self.auth_all_paths
        # TODO test if base_path starts with an / (if not none)
        arguments = arguments or dict()
        arguments = dict(self.arguments, **arguments)  # copy global arguments and update with api specfic

        if isinstance(specification, dict):
            specification = specification
        else:
            specification = self.specification_dir / specification

        api_options = self.options.extend(options)

        api = self.api_cls(specification,
                           base_path=base_path,
                           arguments=arguments,
                           resolver=resolver,
                           resolver_error_handler=resolver_error_handler,
                           validate_responses=validate_responses,
                           strict_validation=strict_validation,
                           auth_all_paths=auth_all_paths,
                           debug=self.debug,
                           validator_map=validator_map,
                           pythonic_params=pythonic_params,
                           pass_context_arg_name=pass_context_arg_name,
                           options=api_options.as_dict())
        return api

    def _resolver_error_handler(self, *args, **kwargs):
        from airflow._vendor.connexion.handlers import ResolverErrorHandler
        return ResolverErrorHandler(self.api_cls, self.resolver_error, *args, **kwargs)

    def add_url_rule(self, rule, endpoint=None, view_func=None, **options):
        """
        Connects a URL rule.  Works exactly like the `route` decorator.  If a view_func is provided it will be
        registered with the endpoint.

        Basically this example::

            @app.route('/')
            def index():
                pass

        Is equivalent to the following::

            def index():
                pass
            app.add_url_rule('/', 'index', index)

        If the view_func is not provided you will need to connect the endpoint to a view function like so::

            app.view_functions['index'] = index

        Internally`route` invokes `add_url_rule` so if you want to customize the behavior via subclassing you only need
        to change this method.

        :param rule: the URL rule as string
        :type rule: str
        :param endpoint: the endpoint for the registered URL rule. Flask itself assumes the name of the view function as
                         endpoint
        :type endpoint: str
        :param view_func: the function to call when serving a request to the provided endpoint
        :type view_func: types.FunctionType
        :param options: the options to be forwarded to the underlying `werkzeug.routing.Rule` object.  A change
                        to Werkzeug is handling of method options. methods is a list of methods this rule should be
                        limited to (`GET`, `POST` etc.).  By default a rule just listens for `GET` (and implicitly
                        `HEAD`).
        """
        log_details = {'endpoint': endpoint, 'view_func': view_func.__name__}
        log_details.update(options)
        logger.debug('Adding %s', rule, extra=log_details)
        self.app.add_url_rule(rule, endpoint, view_func, **options)

    def route(self, rule, **options):
        """
        A decorator that is used to register a view function for a
        given URL rule.  This does the same thing as `add_url_rule`
        but is intended for decorator usage::

            @app.route('/')
            def index():
                return 'Hello World'

        :param rule: the URL rule as string
        :type rule: str
        :param endpoint: the endpoint for the registered URL rule.  Flask
                         itself assumes the name of the view function as
                         endpoint
        :param options: the options to be forwarded to the underlying `werkzeug.routing.Rule` object.  A change
                        to Werkzeug is handling of method options.  methods is a list of methods this rule should be
                        limited to (`GET`, `POST` etc.).  By default a rule just listens for `GET` (and implicitly
                        `HEAD`).
        """
        logger.debug('Adding %s with decorator', rule, extra=options)
        return self.app.route(rule, **options)

    @abc.abstractmethod
    def run(self, port=None, server=None, debug=None, host=None, **options):  # pragma: no cover
        """
        Runs the application on a local development server.
        :param host: the host interface to bind on.
        :type host: str
        :param port: port to listen to
        :type port: int
        :param server: which wsgi server to use
        :type server: str | None
        :param debug: include debugging information
        :type debug: bool
        :param options: options to be forwarded to the underlying server
        """

    def __call__(self, environ, start_response):  # pragma: no cover
        """
        Makes the class callable to be WSGI-compliant. As Flask is used to handle requests,
        this is a passthrough-call to the Flask callable class.
        This is an abstraction to avoid directly referencing the app attribute from outside the
        class and protect it from unwanted modification.
        """
        return self.app(environ, start_response)
