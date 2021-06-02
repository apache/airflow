import logging
import pathlib
from typing import Optional  # NOQA

try:
    from swagger_ui_bundle import (swagger_ui_2_path,
                                   swagger_ui_3_path)
except ImportError:
    swagger_ui_2_path = swagger_ui_3_path = None

MODULE_PATH = pathlib.Path(__file__).absolute().parent
NO_UI_MSG = """The swagger_ui directory could not be found.
    Please install connexion with extra install: pip install connexion[swagger-ui]
    or provide the path to your local installation by passing swagger_path=<your path>
"""

logger = logging.getLogger("connexion.options")


class ConnexionOptions(object):

    def __init__(self, options=None, oas_version=(2,)):
        self._options = {}
        self.oas_version = oas_version
        if self.oas_version >= (3, 0, 0):
            self.openapi_spec_name = '/openapi.json'
            self.swagger_ui_local_path = swagger_ui_3_path
        else:
            self.openapi_spec_name = '/swagger.json'
            self.swagger_ui_local_path = swagger_ui_2_path

        if options:
            self._options.update(filter_values(options))

    def extend(self, new_values=None):
        # type: (Optional[dict]) -> ConnexionOptions
        """
        Return a new instance of `ConnexionOptions` using as default the currently
        defined options.
        """
        if new_values is None:
            new_values = {}

        options = dict(self._options)
        options.update(filter_values(new_values))
        return ConnexionOptions(options, self.oas_version)

    def as_dict(self):
        return self._options

    @property
    def openapi_spec_available(self):
        # type: () -> bool
        """
        Whether to make available the OpenAPI Specification under
        `openapi_spec_path`.

        Default: True
        """
        deprecated_option = self._options.get('swagger_json', True)
        serve_spec = self._options.get('serve_spec', deprecated_option)
        if 'swagger_json' in self._options:
            deprecation_warning = ("The 'swagger_json' option is deprecated. "
                                   "Please use 'serve_spec' instead")
            logger.warning(deprecation_warning)
        return serve_spec

    @property
    def openapi_console_ui_available(self):
        # type: () -> bool
        """
        Whether to make the OpenAPI Console UI available under the path
        defined in `openapi_console_ui_path` option.

        Default: True
        """
        if (self._options.get('swagger_ui', True) and
                self.openapi_console_ui_from_dir is None):
            logger.warning(NO_UI_MSG)
            return False
        return self._options.get('swagger_ui', True)

    @property
    def openapi_spec_path(self):
        # type: () -> str
        """
        Path to mount the OpenAPI Console UI and make it accessible via a browser.

        Default: /openapi.json for openapi3, otherwise /swagger.json
        """
        return self._options.get('openapi_spec_path', self.openapi_spec_name)

    @property
    def openapi_console_ui_path(self):
        # type: () -> str
        """
        Path to mount the OpenAPI Console UI and make it accessible via a browser.

        Default: /ui
        """
        return self._options.get('swagger_url', '/ui')

    @property
    def openapi_console_ui_from_dir(self):
        # type: () -> str
        """
        Custom OpenAPI Console UI directory from where Connexion will serve
        the static files.

        Default: Connexion's vendored version of the OpenAPI Console UI.
        """
        return self._options.get('swagger_path', self.swagger_ui_local_path)

    @property
    def openapi_console_ui_config(self):
        # type: () -> dict
        """
        Custom OpenAPI Console UI config.

        Default: None
        """
        return self._options.get('swagger_ui_config', None)

    @property
    def uri_parser_class(self):
        # type: () -> AbstractURIParser
        """
        The class to use for parsing URIs into path and query parameters.
        Default: None
        """
        return self._options.get('uri_parser_class', None)


def filter_values(dictionary):
    # type: (dict) -> dict
    """
    Remove `None` value entries in the dictionary.

    :param dictionary:
    :return:
    """
    return dict([(key, value)
                 for key, value in dictionary.items()
                 if value is not None])
