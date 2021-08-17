import asyncio
import logging
import re
import traceback
from contextlib import suppress
from http import HTTPStatus
from urllib.parse import parse_qs

import aiohttp_jinja2
import jinja2
from aiohttp import web
from aiohttp.web_exceptions import HTTPNotFound, HTTPPermanentRedirect
from aiohttp.web_middlewares import normalize_path_middleware
from airflow._vendor.connexion.apis.abstract import AbstractAPI
from airflow._vendor.connexion.exceptions import ProblemException
from airflow._vendor.connexion.handlers import AuthErrorHandler
from airflow._vendor.connexion.jsonifier import JSONEncoder, Jsonifier
from airflow._vendor.connexion.lifecycle import ConnexionRequest, ConnexionResponse
from airflow._vendor.connexion.problem import problem
from airflow._vendor.connexion.utils import yamldumper
from werkzeug.exceptions import HTTPException as werkzeug_HTTPException


logger = logging.getLogger('connexion.apis.aiohttp_api')


def _generic_problem(http_status: HTTPStatus, exc: Exception = None):
    extra = None
    if exc is not None:
        loop = asyncio.get_event_loop()
        if loop.get_debug():
            tb = None
            with suppress(Exception):
                tb = traceback.format_exc()
            if tb:
                extra = {"traceback": tb}

    return problem(
        status=http_status.value,
        title=http_status.phrase,
        detail=http_status.description,
        ext=extra,
    )


@web.middleware
async def problems_middleware(request, handler):
    try:
        response = await handler(request)
    except ProblemException as exc:
        response = problem(status=exc.status, detail=exc.detail, title=exc.title,
                           type=exc.type, instance=exc.instance, headers=exc.headers, ext=exc.ext)
    except (werkzeug_HTTPException, _HttpNotFoundError) as exc:
        response = problem(status=exc.code, title=exc.name, detail=exc.description)
    except web.HTTPError as exc:
        if exc.text == "{}: {}".format(exc.status, exc.reason):
            detail = HTTPStatus(exc.status).description
        else:
            detail = exc.text
        response = problem(status=exc.status, title=exc.reason, detail=detail)
    except (
        web.HTTPException,  # eg raised HTTPRedirection or HTTPSuccessful
        asyncio.CancelledError,  # skipped in default web_protocol
    ):
        # leave this to default handling in aiohttp.web_protocol.RequestHandler.start()
        raise
    except asyncio.TimeoutError as exc:
        # overrides 504 from aiohttp.web_protocol.RequestHandler.start()
        logger.debug('Request handler timed out.', exc_info=exc)
        response = _generic_problem(HTTPStatus.GATEWAY_TIMEOUT, exc)
    except Exception as exc:
        # overrides 500 from aiohttp.web_protocol.RequestHandler.start()
        logger.exception('Error handling request', exc_info=exc)
        response = _generic_problem(HTTPStatus.INTERNAL_SERVER_ERROR, exc)

    if isinstance(response, ConnexionResponse):
        response = await AioHttpApi.get_response(response)
    return response


class AioHttpApi(AbstractAPI):
    def __init__(self, *args, **kwargs):
        # NOTE we use HTTPPermanentRedirect (308) because
        # clients sometimes turn POST requests into GET requests
        # on 301, 302, or 303
        # see https://tools.ietf.org/html/rfc7538
        trailing_slash_redirect = normalize_path_middleware(
            append_slash=True,
            redirect_class=HTTPPermanentRedirect
        )
        self.subapp = web.Application(
            middlewares=[
                problems_middleware,
                trailing_slash_redirect
            ]
        )
        AbstractAPI.__init__(self, *args, **kwargs)

        aiohttp_jinja2.setup(
            self.subapp,
            loader=jinja2.FileSystemLoader(
                str(self.options.openapi_console_ui_from_dir)
            )
        )
        middlewares = self.options.as_dict().get('middlewares', [])
        self.subapp.middlewares.extend(middlewares)

    def _set_base_path(self, base_path):
        AbstractAPI._set_base_path(self, base_path)
        self._api_name = AioHttpApi.normalize_string(self.base_path)

    @staticmethod
    def normalize_string(string):
        return re.sub(r'[^a-zA-Z0-9]', '_', string.strip('/'))

    def _base_path_for_prefix(self, request):
        """
        returns a modified basePath which includes the incoming request's
        path prefix.
        """
        base_path = self.base_path
        if not request.path.startswith(self.base_path):
            prefix = request.path.split(self.base_path)[0]
            base_path = prefix + base_path
        return base_path

    def _spec_for_prefix(self, request):
        """
        returns a spec with a modified basePath / servers block
        which corresponds to the incoming request path.
        This is needed when behind a path-altering reverse proxy.
        """
        base_path = self._base_path_for_prefix(request)
        return self.specification.with_base_path(base_path).raw

    def add_openapi_json(self):
        """
        Adds openapi json to {base_path}/openapi.json
             (or {base_path}/swagger.json for swagger2)
        """
        logger.debug('Adding spec json: %s/%s', self.base_path,
                     self.options.openapi_spec_path)
        self.subapp.router.add_route(
            'GET',
            self.options.openapi_spec_path,
            self._get_openapi_json
        )

    def add_openapi_yaml(self):
        """
        Adds openapi json to {base_path}/openapi.json
             (or {base_path}/swagger.json for swagger2)
        """
        if not self.options.openapi_spec_path.endswith("json"):
            return

        openapi_spec_path_yaml = \
            self.options.openapi_spec_path[:-len("json")] + "yaml"
        logger.debug('Adding spec yaml: %s/%s', self.base_path,
                     openapi_spec_path_yaml)
        self.subapp.router.add_route(
            'GET',
            openapi_spec_path_yaml,
            self._get_openapi_yaml
        )

    async def _get_openapi_json(self, request):
        return web.Response(
            status=200,
            content_type='application/json',
            body=self.jsonifier.dumps(self._spec_for_prefix(request))
        )

    async def _get_openapi_yaml(self, request):
        return web.Response(
            status=200,
            content_type='text/yaml',
            body=yamldumper(self._spec_for_prefix(request))
        )

    def add_swagger_ui(self):
        """
        Adds swagger ui to {base_path}/ui/
        """
        console_ui_path = self.options.openapi_console_ui_path.strip().rstrip('/')
        logger.debug('Adding swagger-ui: %s%s/',
                     self.base_path,
                     console_ui_path)

        for path in (
            console_ui_path + '/',
            console_ui_path + '/index.html',
        ):
            self.subapp.router.add_route(
                'GET',
                path,
                self._get_swagger_ui_home
            )

        if self.options.openapi_console_ui_config is not None:
            self.subapp.router.add_route(
                'GET',
                console_ui_path + '/swagger-ui-config.json',
                self._get_swagger_ui_config
            )

        # we have to add an explicit redirect instead of relying on the
        # normalize_path_middleware because we also serve static files
        # from this dir (below)

        async def redirect(request):
            raise web.HTTPMovedPermanently(
                location=self.base_path + console_ui_path + '/'
            )

        self.subapp.router.add_route(
            'GET',
            console_ui_path,
            redirect
        )

        # this route will match and get a permission error when trying to
        # serve index.html, so we add the redirect above.
        self.subapp.router.add_static(
            console_ui_path,
            path=str(self.options.openapi_console_ui_from_dir),
            name='swagger_ui_static'
        )

    @aiohttp_jinja2.template('index.j2')
    async def _get_swagger_ui_home(self, req):
        base_path = self._base_path_for_prefix(req)
        template_variables = {
            'openapi_spec_url': (base_path + self.options.openapi_spec_path)
        }
        if self.options.openapi_console_ui_config is not None:
            template_variables['configUrl'] = 'swagger-ui-config.json'
        return template_variables

    async def _get_swagger_ui_config(self, req):
        return web.Response(
            status=200,
            content_type='text/json',
            body=self.jsonifier.dumps(self.options.openapi_console_ui_config)
        )

    def add_auth_on_not_found(self, security, security_definitions):
        """
        Adds a 404 error handler to authenticate and only expose the 404 status if the security validation pass.
        """
        logger.debug('Adding path not found authentication')
        not_found_error = AuthErrorHandler(
            self, _HttpNotFoundError(),
            security=security,
            security_definitions=security_definitions
        )
        endpoint_name = "{}_not_found".format(self._api_name)
        self.subapp.router.add_route(
            '*',
            '/{not_found_path}',
            not_found_error.function,
            name=endpoint_name
        )

    def _add_operation_internal(self, method, path, operation):
        method = method.upper()
        operation_id = operation.operation_id or path

        logger.debug('... Adding %s -> %s', method, operation_id,
                     extra=vars(operation))

        handler = operation.function
        endpoint_name = '{}_{}_{}'.format(
            self._api_name,
            AioHttpApi.normalize_string(path),
            method.lower()
        )
        self.subapp.router.add_route(
            method, path, handler, name=endpoint_name
        )

        if not path.endswith('/'):
            self.subapp.router.add_route(
                method, path + '/', handler, name=endpoint_name + '_'
            )

    @classmethod
    async def get_request(cls, req):
        """Convert aiohttp request to connexion

        :param req: instance of aiohttp.web.Request
        :return: connexion request instance
        :rtype: ConnexionRequest
        """
        url = str(req.url)
        logger.debug('Getting data and status code',
                     extra={'has_body': req.has_body, 'url': url})

        query = parse_qs(req.rel_url.query_string)
        headers = req.headers
        body = None
        if req.body_exists:
            body = await req.read()

        return ConnexionRequest(url=url,
                                method=req.method.lower(),
                                path_params=dict(req.match_info),
                                query=query,
                                headers=headers,
                                body=body,
                                json_getter=lambda: cls.jsonifier.loads(body),
                                files={},
                                context=req)

    @classmethod
    async def get_response(cls, response, mimetype=None, request=None):
        """Get response.
        This method is used in the lifecycle decorators

        :type response: aiohttp.web.StreamResponse | (Any,) | (Any, int) | (Any, dict) | (Any, int, dict)
        :rtype: aiohttp.web.Response
        """
        while asyncio.iscoroutine(response):
            response = await response

        url = str(request.url) if request else ''

        return cls._get_response(response, mimetype=mimetype, extra_context={"url": url})

    @classmethod
    def _is_framework_response(cls, response):
        """ Return True if `response` is a framework response class """
        return isinstance(response, web.StreamResponse)

    @classmethod
    def _framework_to_connexion_response(cls, response, mimetype):
        """ Cast framework response class to ConnexionResponse used for schema validation """
        body = None
        if hasattr(response, "body"):  # StreamResponse and FileResponse don't have body
            body = response.body
        return ConnexionResponse(
            status_code=response.status,
            mimetype=mimetype,
            content_type=response.content_type,
            headers=response.headers,
            body=body
        )

    @classmethod
    def _connexion_to_framework_response(cls, response, mimetype, extra_context=None):
        """ Cast ConnexionResponse to framework response class """
        return cls._build_response(
            mimetype=response.mimetype or mimetype,
            status_code=response.status_code,
            content_type=response.content_type,
            headers=response.headers,
            data=response.body,
            extra_context=extra_context,
        )

    @classmethod
    def _build_response(cls, data, mimetype, content_type=None, headers=None, status_code=None, extra_context=None):
        if cls._is_framework_response(data):
            raise TypeError("Cannot return web.StreamResponse in tuple. Only raw data can be returned in tuple.")

        data, status_code, serialized_mimetype = cls._prepare_body_and_status_code(data=data, mimetype=mimetype, status_code=status_code, extra_context=extra_context)

        if isinstance(data, str):
            text = data
            body = None
        else:
            text = None
            body = data

        content_type = content_type or mimetype or serialized_mimetype
        return web.Response(body=body, text=text, headers=headers, status=status_code, content_type=content_type)

    @classmethod
    def _set_jsonifier(cls):
        cls.jsonifier = Jsonifier(cls=JSONEncoder)


class _HttpNotFoundError(HTTPNotFound):
    def __init__(self):
        self.name = 'Not Found'
        self.description = (
            'The requested URL was not found on the server. '
            'If you entered the URL manually please check your spelling and '
            'try again.'
        )
        self.code = type(self).status_code
        self.empty_body = True

        HTTPNotFound.__init__(self, reason=self.name)
