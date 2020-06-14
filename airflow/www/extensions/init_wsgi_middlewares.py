from urllib.parse import urlparse

from flask import Flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.middleware.proxy_fix import ProxyFix

from airflow.configuration import conf


def root_app(env, resp):
    resp(b'404 Not Found', [('Content-Type', 'text/plain')])
    return [b'Apache Airflow is not at this location']


def init_wsg_middleware(flask_app: Flask):
    # Apply DispatcherMiddleware
    base_url = urlparse(conf.get('webserver', 'base_url'))[2]
    if not base_url or base_url == '/':
        base_url = ""
    if base_url:
        flask_app.wsgi_app = DispatcherMiddleware(  # type: ignore
            root_app,
            mounts={base_url: flask_app.wsgi_app}
        )

    # Apply ProxyFix middleware
    if conf.getboolean('webserver', 'ENABLE_PROXY_FIX'):
        flask_app.wsgi_app = ProxyFix(  # type: ignore
            flask_app.wsgi_app,
            x_for=conf.getint("webserver", "PROXY_FIX_X_FOR", fallback=1),
            x_proto=conf.getint("webserver", "PROXY_FIX_X_PROTO", fallback=1),
            x_host=conf.getint("webserver", "PROXY_FIX_X_HOST", fallback=1),
            x_port=conf.getint("webserver", "PROXY_FIX_X_PORT", fallback=1),
            x_prefix=conf.getint("webserver", "PROXY_FIX_X_PREFIX", fallback=1)
        )
