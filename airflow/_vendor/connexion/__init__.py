import sys

import werkzeug.exceptions as exceptions  # NOQA

from .apis import AbstractAPI  # NOQA
from .apps import AbstractApp  # NOQA
from .decorators.produces import NoContent  # NOQA
from .exceptions import ProblemException  # NOQA
# add operation for backwards compatability
from .operations import compat
from .problem import problem  # NOQA
from .resolver import Resolution, Resolver, RestyResolver  # NOQA

full_name = '{}.operation'.format(__package__)
sys.modules[full_name] = sys.modules[compat.__name__]


def not_installed_error(exc):  # pragma: no cover
    import functools

    def _required_lib(exc, *args, **kwargs):
        raise exc

    return functools.partial(_required_lib, exc)


try:
    from .apis.flask_api import FlaskApi, context  # NOQA
    from .apps.flask_app import FlaskApp
    from flask import request  # NOQA
except ImportError as e:  # pragma: no cover
    _flask_not_installed_error = not_installed_error(e)
    FlaskApi = _flask_not_installed_error
    FlaskApp = _flask_not_installed_error

App = FlaskApp
Api = FlaskApi

try:
    from .apis.aiohttp_api import AioHttpApi
    from .apps.aiohttp_app import AioHttpApp
except ImportError as e:  # pragma: no cover
    _aiohttp_not_installed_error = not_installed_error(e)
    AioHttpApi = _aiohttp_not_installed_error
    AioHttpApp = _aiohttp_not_installed_error

# This version is replaced during release process.
__version__ = '2.7.0'
