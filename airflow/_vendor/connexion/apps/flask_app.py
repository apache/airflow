import datetime
import logging
import pathlib
from decimal import Decimal
from types import FunctionType  # NOQA

import flask
import werkzeug.exceptions
from flask import json

from ..apis.flask_api import FlaskApi
from ..exceptions import ProblemException
from ..problem import problem
from .abstract import AbstractApp

logger = logging.getLogger('connexion.app')


class FlaskApp(AbstractApp):
    def __init__(self, import_name, server='flask', **kwargs):
        super(FlaskApp, self).__init__(import_name, FlaskApi, server=server, **kwargs)

    def create_app(self):
        app = flask.Flask(self.import_name, **self.server_args)
        app.json_encoder = FlaskJSONEncoder
        return app

    def get_root_path(self):
        return pathlib.Path(self.app.root_path)

    def set_errors_handlers(self):
        for error_code in werkzeug.exceptions.default_exceptions:
            self.add_error_handler(error_code, self.common_error_handler)

        self.add_error_handler(ProblemException, self.common_error_handler)

    @staticmethod
    def common_error_handler(exception):
        """
        :type exception: Exception
        """
        if isinstance(exception, ProblemException):
            response = problem(
                status=exception.status, title=exception.title, detail=exception.detail,
                type=exception.type, instance=exception.instance, headers=exception.headers,
                ext=exception.ext)
        else:
            if not isinstance(exception, werkzeug.exceptions.HTTPException):
                exception = werkzeug.exceptions.InternalServerError()

            response = problem(title=exception.name, detail=exception.description,
                               status=exception.code)

        return FlaskApi.get_response(response)

    def add_api(self, specification, **kwargs):
        api = super(FlaskApp, self).add_api(specification, **kwargs)
        self.app.register_blueprint(api.blueprint)
        return api

    def add_error_handler(self, error_code, function):
        # type: (int, FunctionType) -> None
        self.app.register_error_handler(error_code, function)

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
        # this functions is not covered in unit tests because we would effectively testing the mocks

        # overwrite constructor parameter
        if port is not None:
            self.port = port
        elif self.port is None:
            self.port = 5000

        self.host = host or self.host or '0.0.0.0'

        if server is not None:
            self.server = server

        if debug is not None:
            self.debug = debug

        logger.debug('Starting %s HTTP server..', self.server, extra=vars(self))
        if self.server == 'flask':
            self.app.run(self.host, port=self.port, debug=self.debug, **options)
        elif self.server == 'tornado':
            try:
                import tornado.wsgi
                import tornado.httpserver
                import tornado.ioloop
            except ImportError:
                raise Exception('tornado library not installed')
            wsgi_container = tornado.wsgi.WSGIContainer(self.app)
            http_server = tornado.httpserver.HTTPServer(wsgi_container, **options)
            http_server.listen(self.port, address=self.host)
            logger.info('Listening on %s:%s..', self.host, self.port)
            tornado.ioloop.IOLoop.instance().start()
        elif self.server == 'gevent':
            try:
                import gevent.pywsgi
            except ImportError:
                raise Exception('gevent library not installed')
            http_server = gevent.pywsgi.WSGIServer((self.host, self.port), self.app, **options)
            logger.info('Listening on %s:%s..', self.host, self.port)
            http_server.serve_forever()
        else:
            raise Exception('Server {} not recognized'.format(self.server))


class FlaskJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            if o.tzinfo:
                # eg: '2015-09-25T23:14:42.588601+00:00'
                return o.isoformat('T')
            else:
                # No timezone present - assume UTC.
                # eg: '2015-09-25T23:14:42.588601Z'
                return o.isoformat('T') + 'Z'

        if isinstance(o, datetime.date):
            return o.isoformat()

        if isinstance(o, Decimal):
            return float(o)

        return json.JSONEncoder.default(self, o)
