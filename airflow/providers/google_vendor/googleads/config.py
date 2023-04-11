# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A set of functions to help load configuration from various locations."""

from distutils.util import strtobool
import functools
import json
import logging.config
import os
import re
import yaml


_logger = logging.getLogger(__name__)


_ENV_PREFIX = "GOOGLE_ADS_"
_REQUIRED_KEYS = ("developer_token", "use_proto_plus")
_OPTIONAL_KEYS = (
    "login_customer_id",
    "endpoint",
    "logging",
    "linked_customer_id",
    "http_proxy",
)
_CONFIG_FILE_PATH_KEY = ("configuration_file_path",)
_OAUTH2_INSTALLED_APP_KEYS = ("client_id", "client_secret", "refresh_token")
_OAUTH2_SERVICE_ACCOUNT_KEYS = ("json_key_file_path", "impersonated_email")
# These keys are deprecated environment variables that can be used in place of
# the primary OAuth2 service account keys for backwards compatibility. They will
# be removed in favor of the primary keys at some point.
_SECONDARY_OAUTH2_SERVICE_ACCOUNT_KEYS = (
    "path_to_private_key_file",
    "delegated_account",
)
_KEYS_ENV_VARIABLES_MAP = {
    key: _ENV_PREFIX + key.upper()
    for key in _REQUIRED_KEYS
    + _OPTIONAL_KEYS
    + _OAUTH2_INSTALLED_APP_KEYS
    + _CONFIG_FILE_PATH_KEY
    + _OAUTH2_SERVICE_ACCOUNT_KEYS
    + _SECONDARY_OAUTH2_SERVICE_ACCOUNT_KEYS
}


def _config_validation_decorator(func):
    """A decorator used to easily run validations on configs loaded into dicts.

    Add this decorator to any method that returns the config as a dict.

    Raises:
        ValueError: If the configuration fails validation
    """

    @functools.wraps(func)
    def validation_wrapper(*args, **kwargs):
        config_dict = func(*args, **kwargs)
        validate_dict(config_dict)
        return config_dict

    return validation_wrapper


def _config_parser_decorator(func):
    """A decorator used to easily parse config values.

    Since configs can be loaded from different locations such as env vars or
    from YAML files it's possible that they may have inconsistent types that
    need to be parsed to a different type. Add this decorator to any method
    that returns the config as a dict.
    """

    @functools.wraps(func)
    def parser_wrapper(*args, **kwargs):
        config_dict = func(*args, **kwargs)
        parsed_config = convert_login_customer_id_to_str(config_dict)
        parsed_config = convert_linked_customer_id_to_str(parsed_config)

        config_keys = parsed_config.keys()

        if "logging" in config_keys:
            logging_config = parsed_config["logging"]
            # If the logging config is a dict then it is already in the format
            # that needs to be returned by this method.
            if type(logging_config) is not dict:
                try:
                    parsed_config["logging"] = json.loads(logging_config)
                    # The logger is configured here in case deprecation warnings
                    # need to be logged further down in this method. The logger
                    # is otherwise configured by the GoogleAdsClient class.
                    logging.config.dictConfig(parsed_config["logging"])
                except json.JSONDecodeError:
                    raise ValueError(
                        "Could not configure the client because the logging "
                        "configuration defined in the 'logging' key or "
                        "'GOOGLE_ADS_LOGGING' environment variable is invalid. "
                        "The configuration value should be a valid JSON string."
                    )

        if "path_to_private_key_file" in config_keys:
            _logger.warning(
                "The 'path_to_private_key_file' configuration key and "
                "'GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE' environment variable "
                "are deprecated and support will be removed at some point in "
                "the future. Please use 'json_key_file_path' configuration key "
                "or 'GOOGLE_ADS_JSON_KEY_FILE_PATH' environment variable "
                "instead."
            )
            if "json_key_file_path" not in config_keys:
                parsed_config["json_key_file_path"] = parsed_config[
                    "path_to_private_key_file"
                ]

            del parsed_config["path_to_private_key_file"]

        if "delegated_account" in config_keys:
            _logger.warning(
                "The 'delegated_account' configuration key and "
                "'GOOGLE_ADS_DELEGATED_PATH' environment variable are "
                "deprecated and support will be removed at some point in "
                "the future. Please use 'impersonated_email' configuration key "
                "or 'GOOGLE_ADS_IMPERSONATED_EMAIL' environment variable "
                "instead."
            )
            if "impersonated_email" not in config_keys:
                parsed_config["impersonated_email"] = parsed_config[
                    "delegated_account"
                ]

            del parsed_config["delegated_account"]

        if "use_proto_plus" in config_keys:
            # When loaded from YAML, YAML string or a dict, this value is
            # evaluated as a bool. If it's loaded from an environment variable
            # it's evaluated as a string. If set to "False" as an environment
            # variable we need to manually change it to the bool False because
            # the string "False" is truthy and can easily be incorrectly
            # converted to the boolean True.
            value = parsed_config.get("use_proto_plus", False)
            parsed_config["use_proto_plus"] = disambiguate_string_bool(value)

        return parsed_config

    return parser_wrapper


def validate_dict(config_data):
    """Validates the given configuration dict.

    Validations that are performed include:
        1. Ensuring all required keys are present.
        2. If a login_customer_id is present ensure it's valid
        3. If a linked_customer_id is present ensure it's valid

    Args:
        config_data: a dict with configuration data.

    Raises:
        ValueError: If the dict does not contain all required config keys.
    """
    if not "use_proto_plus" in config_data.keys():
        raise ValueError(
            "The client library configuration is missing the required "
            '"use_proto_plus" key. Please set this option to either "True" '
            'or "False". For more information about this option see the '
            "Protobuf Messages guide: "
            "https://developers.google.com/google-ads/api/docs/client-libs/python/protobuf-messages"
        )

    if not all(key in config_data for key in _REQUIRED_KEYS):
        raise ValueError(
            "A required field in the configuration data was not "
            "found. The required fields are: {}".format(str(_REQUIRED_KEYS))
        )

    if "login_customer_id" in config_data:
        validate_login_customer_id(config_data["login_customer_id"])

    if "linked_customer_id" in config_data:
        validate_linked_customer_id(config_data["linked_customer_id"])


def _validate_customer_id(customer_id, id_type):
    """Validates a customer ID.

    Args:
        customer_id: a str from config indicating a login customer ID or
            linked customer ID.
        id_type: a str of the type of customer ID, either "login" or "linked".

    Raises:
        ValueError: If the customer ID is not a str representing a ten-digit
            non-negative integer.
    """
    if customer_id is not None:
        # Checks that the string is comprised only of 10 digits.
        pattern = re.compile("^\d{10}", re.ASCII)
        if not pattern.fullmatch(customer_id):
            raise ValueError(
                f"The specified {id_type} customer ID is invalid. It must be a "
                "ten digit number represented as a string, i.e. '1234567890'"
            )


def validate_login_customer_id(login_customer_id):
    """Validates a login customer ID.
    Args:
        login_customer_id: a str from config indicating a login customer ID.
    Raises:
        ValueError: If the login customer ID is not an int in the
            range 0 - 9999999999.
    """
    _validate_customer_id(login_customer_id, "login")


def validate_linked_customer_id(linked_customer_id):
    """Validates a linked customer ID.
    Args:
        linked_customer_id: a str from config indicating a linked customer ID.
    Raises:
        ValueError: If the linked customer ID is not an int in the
            range 0 - 9999999999.
    """
    _validate_customer_id(linked_customer_id, "linked")


@_config_validation_decorator
@_config_parser_decorator
def load_from_yaml_file(path=None):
    """Loads configuration data from a YAML file and returns it as a dict.

    Args:
        path: a str indicating the path to a YAML file containing
            configuration data used to initialize a GoogleAdsClient.

    Returns:
        A dict with configuration from the specified YAML file.

    Raises:
        FileNotFoundError: If the specified configuration file doesn't exist.
        IOError: If the configuration file can't be loaded.
    """
    if path is None:
        # If no path is specified then we check for the environment variable
        # that may define the path. If that is not defined then we use the
        # default path.
        path_from_env_var = os.environ.get(
            _ENV_PREFIX + _CONFIG_FILE_PATH_KEY[0].upper()
        )
        path = (
            path_from_env_var
            if path_from_env_var
            else os.path.join(os.path.expanduser("~"), "google-ads.yaml")
        )

    if not os.path.isabs(path):
        path = os.path.expanduser(path)

    with open(path, "rb") as handle:
        yaml_doc = handle.read()

    return parse_yaml_document_to_dict(yaml_doc)


@_config_validation_decorator
@_config_parser_decorator
def load_from_dict(config_dict):
    """Check if the argument is dictionary or not. If successful it calls the parsing decorator,
    followed by validation decorator. This validates the keys used in the config_dict, before
    returning to its caller.

    Args:
        config_dict: a dict containing client configuration.

    Returns:
        The same input dictionary that is passed into the function.

    Raises:
        A value error if the argument (config_dict) is not a dict.
    """
    if isinstance(config_dict, dict):
        return config_dict
    else:
        raise ValueError(
            "The configuration object passed to function load_from_dict must be of type dict."
        )


@_config_validation_decorator
@_config_parser_decorator
def parse_yaml_document_to_dict(yaml_doc):
    """Parses a YAML document to a dict.

    Args:
        yaml_doc: a str (in Python 2) or bytes (in Python 3) containing YAML
            configuration data.

    Returns:
        A dict of the key/value pairs from the given YAML document.

    Raises:
        yaml.YAMLError: If there is a problem parsing the YAML document.
    """
    return yaml.safe_load(yaml_doc) or {}


@_config_validation_decorator
@_config_parser_decorator
def load_from_env():
    """Loads configuration data from the environment and returns it as a dict.

    Returns:
        A dict with configuration from the environment.

    Raises:
        ValueError: If the configuration
    """
    config_data = {
        key: os.environ.get(env_variable)
        for key, env_variable in _KEYS_ENV_VARIABLES_MAP.items()
        if env_variable in os.environ
    }

    # If configuration_file_path is set by the environment then configuration
    # is retrieved from the yaml file specified in the given path.
    if "configuration_file_path" in config_data.keys():
        return load_from_yaml_file(config_data["configuration_file_path"])

    return config_data


def get_oauth2_installed_app_keys():
    """A getter that returns the required OAuth2 installed application keys.

    Returns:
        A tuple containing the required keys as strs.
    """
    return _OAUTH2_INSTALLED_APP_KEYS


def get_oauth2_service_account_keys():
    """A getter that returns the required OAuth2 service account keys.

    Returns:
        A tuple containing the required keys as strs.
    """
    return _OAUTH2_SERVICE_ACCOUNT_KEYS


def convert_login_customer_id_to_str(config_data):
    """Parses a config dict's login_customer_id attr value to a str.

    Like many values from YAML it's possible for login_customer_id to
    either be a str or an int. Since we actually run validations on this
    value before making requests it's important to parse it to a str.

    Args:
        config_data: A config dict object.

    Returns:
        The same config dict object with a mutated login_customer_id attr.
    """
    login_customer_id = config_data.get("login_customer_id")

    if login_customer_id:
        config_data["login_customer_id"] = str(login_customer_id)

    return config_data


def convert_linked_customer_id_to_str(config_data):
    """Parses a config dict's linked_customer_id attr value to a str.

    Like many values from YAML it's possible for linked_customer_id to
    either be a str or an int. Since we actually run validations on this
    value before making requests it's important to parse it to a str.

    Args:
        config_data: A config dict object.

    Returns:
        The same config dict object with a mutated linked_customer_id attr.
    """
    linked_customer_id = config_data.get("linked_customer_id")

    if linked_customer_id:
        config_data["linked_customer_id"] = str(linked_customer_id)

    return config_data


def disambiguate_string_bool(value):
    """Converts a stringified boolean to its bool representation.

    Args:
        value: A boolean or a string representing a boolean.

    Returns:
        A boolean.

    Raises:
        TypeError: If the string is not a valid boolean representation.
    """
    if isinstance(value, bool):
        return value
    elif isinstance(value, str):
        try:
            return bool(strtobool(value))
        except ValueError:
            raise ValueError(
                'The "use_proto_plus" configuration key value should be'
                f'explicitly set to "True" or "False" but "{value}" '
                "was given."
            )
    else:
        raise TypeError(
            'The "use_proto_plus" configuration key is invalid. Expected '
            f"Union[bool, str] but received {type(value)}"
        )
