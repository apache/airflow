# Copyright 2020 Google LLC
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
"""A client and common configurations for the Google Ads API."""

from importlib import import_module
import logging.config
import pkg_resources

from google.api_core.gapic_v1.client_info import ClientInfo
import grpc.experimental
import proto
from proto.enums import ProtoEnumMeta

from airflow.providers.google_vendor.googleads import config, oauth2, util
from airflow.providers.google_vendor.googleads.interceptors import (
    MetadataInterceptor,
    ExceptionInterceptor,
    LoggingInterceptor,
)

_logger = logging.getLogger(__name__)

_SERVICE_CLIENT_TEMPLATE = "{}Client"

_VALID_API_VERSIONS = ["v12"]
_DEFAULT_VERSION = _VALID_API_VERSIONS[0]

# Retrieve the version of this client library to be sent in the user-agent
# information of API calls.
try:
    _CLIENT_INFO = ClientInfo(
        client_library_version=pkg_resources.get_distribution(
            "google-ads",
        ).version,
    )
except pkg_resources.DistributionNotFound:
    _CLIENT_INFO = ClientInfo()

# See options at grpc.github.io/grpc/core/group__grpc__arg__keys.html
_GRPC_CHANNEL_OPTIONS = [
    ("grpc.max_metadata_size", 16 * 1024 * 1024),
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
]

unary_stream_single_threading_option = util.get_nested_attr(
    grpc, "experimental.ChannelOptions.SingleThreadedUnaryStream", None
)

if unary_stream_single_threading_option:
    _GRPC_CHANNEL_OPTIONS.append((unary_stream_single_threading_option, 1))


class _EnumGetter:
    """An intermediate getter for retrieving enums from service clients.

    Acts as the "enum" property of a service client and dynamically loads enum
    class instances when accessed.
    """

    def __init__(self, client):
        """Initializer for the _EnumGetter class.

        Args:
            version: a str indicating the version of the Google Ads API to be
              used.
        """
        self._client = client
        self._version = client.version or _DEFAULT_VERSION
        self._enums = None
        self._use_proto_plus = client.use_proto_plus

    def __dir__(self):
        """Overrides behavior when dir() is called on instances of this class.

        It's useful to use dir() to see a list of available attrbutes. Since
        this class exposes all the enums in the API it borrows the __all__
        property from the corresponding enums module.
        """
        if not self._enums:
            self._enums = import_module(
                f"airflow.providers.google_vendor.googleads.{self._version}.enums"
            ).__all__

        return self._enums

    def __getattr__(self, name):
        """Dynamically loads the given enum class instance.

        Args:
            name: a str of the name of the enum to load, i.e. "AdTypeEnum."

        Returns:
            An instance of the enum proto message class.
        """
        if not name in self.__dir__():
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )
        try:
            enum_class = self._client.get_type(name)

            if self._use_proto_plus == True:
                for attr in dir(enum_class):
                    attr_val = getattr(enum_class, attr)
                    if isinstance(attr_val, ProtoEnumMeta):
                        return attr_val
            else:
                return enum_class
        except ValueError:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )

    def __getstate__(self):
        """Returns self serialized as a dict.

        Since this class overrides __getattr__ we define this method to help
        with pickling, which is imporant to avoid recursion depth errors when
        pickling this class or the GoogleAdsClient or using multiprocessing.

        Returns:
            a dict of this object's state
        """
        return self.__dict__.copy()

    def __setstate__(self, d):
        """Deserializes self with the given dictionary.

        Since this class overrides __getattr__ we define this method to help
        with pickling, which is imporant to avoid recursion depth errors when
        pickling this class or the GoogleAdsClient or using multiprocessing.

        Args:
            d: a dict of this object's state
        """
        self.__dict__.update(d)


class GoogleAdsClient:
    """Google Ads client used to configure settings and fetch services."""

    @classmethod
    def copy_from(cls, destination, origin):
        """Copies protobuf and proto-plus messages into one-another.

        This method consolidates the CopyFrom logic of protobuf and proto-plus
        messages into a single helper method. The destination message will be
        updated with the exact state of the origin message.

        Args:
            destination: The message where changes are being copied.
            origin: The message where changes are being copied from.
        """
        return util.proto_copy_from(destination, origin)

    @classmethod
    def _get_client_kwargs(cls, config_data):
        """Converts configuration dict into kwargs required by the client.

        Args:
            config_data: a dict containing client configuration.

        Returns:
            A dict containing kwargs that will be provided to the
            GoogleAdsClient initializer.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        return {
            "credentials": oauth2.get_credentials(config_data),
            "developer_token": config_data.get("developer_token"),
            "endpoint": config_data.get("endpoint"),
            "login_customer_id": config_data.get("login_customer_id"),
            "logging_config": config_data.get("logging"),
            "linked_customer_id": config_data.get("linked_customer_id"),
            "http_proxy": config_data.get("http_proxy"),
            "use_proto_plus": config_data.get("use_proto_plus"),
        }

    @classmethod
    def _get_api_services_by_version(cls, version):
        """Returns a module with all services and types for a given API version.

        Args:
            version: a str indicating the API version.

        Returns:
            A module containing all services and types for the a API version.
        """
        try:
            version_module = import_module(f"airflow.providers.google_vendor.googleads.{version}")
        except ImportError:
            raise ValueError(
                'Specified Google Ads API version "{}" does not '
                'exist. Valid API versions are: "{}"'.format(
                    version, '", "'.join(_VALID_API_VERSIONS)
                )
            )
        return version_module

    @classmethod
    def load_from_env(cls, version=None):
        """Creates a GoogleAdsClient with data stored in the env variables.

        Args:
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A GoogleAdsClient initialized with the values specified in the
            env variables.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        config_data = config.load_from_env()
        kwargs = cls._get_client_kwargs(config_data)
        return cls(**dict(version=version, **kwargs))

    @classmethod
    def load_from_string(cls, yaml_str, version=None):
        """Creates a GoogleAdsClient with data stored in the YAML string.

        Args:
            yaml_str: a str containing YAML configuration data used to
              initialize a GoogleAdsClient.
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A GoogleAdsClient initialized with the values specified in the
            string.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        config_data = config.parse_yaml_document_to_dict(yaml_str)
        kwargs = cls._get_client_kwargs(config_data)
        return cls(**dict(version=version, **kwargs))

    @classmethod
    def load_from_dict(cls, config_dict, version=None):
        """Creates a GoogleAdsClient with data stored in the config_dict.

        Args:
            config_dict: a dict consisting of configuration data used to
              initialize a GoogleAdsClient.
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A GoogleAdsClient initialized with the values specified in the
                dict.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        config_data = config.load_from_dict(config_dict)
        kwargs = cls._get_client_kwargs(config_data)
        return cls(**dict(version=version, **kwargs))

    @classmethod
    def load_from_storage(cls, path=None, version=None):
        """Creates a GoogleAdsClient with data stored in the specified file.

        Args:
            path: a str indicating the path to a YAML file containing
              configuration data used to initialize a GoogleAdsClient.
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A GoogleAdsClient initialized with the values in the specified file.

        Raises:
            FileNotFoundError: If the specified configuration file doesn't
                exist.
            IOError: If the configuration file can't be loaded.
            ValueError: If the configuration file lacks a required field.
        """
        config_data = config.load_from_yaml_file(path)
        kwargs = cls._get_client_kwargs(config_data)
        return cls(**dict(version=version, **kwargs))

    def __init__(
        self,
        credentials,
        developer_token,
        endpoint=None,
        login_customer_id=None,
        logging_config=None,
        linked_customer_id=None,
        version=None,
        http_proxy=None,
        use_proto_plus=False,
    ):
        """Initializer for the GoogleAdsClient.

        Args:
            credentials: a google.oauth2.credentials.Credentials instance.
            developer_token: a str developer token.
            endpoint: a str specifying an optional alternative API endpoint.
            login_customer_id: a str specifying a login customer ID.
            logging_config: a dict specifying logging config options.
            linked_customer_id: a str specifying a linked customer ID.
            version: a str indicating the Google Ads API version to be used.
            http_proxy: a str specifying the proxy URI through which to connect.
        """
        if logging_config:
            logging.config.dictConfig(logging_config)

        self.credentials = credentials
        self.developer_token = developer_token
        self.endpoint = endpoint
        self.login_customer_id = login_customer_id
        self.linked_customer_id = linked_customer_id
        self.version = version
        self.http_proxy = http_proxy
        self.use_proto_plus = use_proto_plus
        self.enums = _EnumGetter(self)

        # If given, write the http_proxy channel option for GRPC to use
        if http_proxy:
            _GRPC_CHANNEL_OPTIONS.append(("grpc.http_proxy", http_proxy))

    def get_service(self, name, version=_DEFAULT_VERSION, interceptors=None):
        """Returns a service client instance for the specified service_name.

        Args:
            name: a str indicating the name of the service for which a service
              client is being retrieved; e.g. you may specify "CampaignService"
              to retrieve a CampaignServiceClient instance.
            version: a str indicating the version of the Google Ads API to be
              used.
            interceptors: an optional list of interceptors to include in
              requests. NOTE: this parameter is not intended for non-Google use
              and is not officially supported.

        Returns:
            A service client instance associated with the given service_name.

        Raises:
            AttributeError: If the specified name doesn't exist.
        """
        # If version is specified when the instance is created,
        # override any version specified as an argument.
        version = self.version if self.version else version
        api_module = self._get_api_services_by_version(version)
        interceptors = interceptors or []

        try:
            service_client_class = getattr(
                api_module, _SERVICE_CLIENT_TEMPLATE.format(name)
            )
        except AttributeError:
            raise ValueError(
                'Specified service {}" does not exist in Google '
                "Ads API {}.".format(name, version)
            )

        service_transport_class = service_client_class.get_transport_class()

        endpoint = (
            self.endpoint
            if self.endpoint
            else service_client_class.DEFAULT_ENDPOINT
        )

        channel = service_transport_class.create_channel(
            host=endpoint,
            credentials=self.credentials,
            options=_GRPC_CHANNEL_OPTIONS,
        )

        interceptors = interceptors + [
            MetadataInterceptor(
                self.developer_token,
                self.login_customer_id,
                self.linked_customer_id,
            ),
            LoggingInterceptor(_logger, version, endpoint),
            ExceptionInterceptor(version, use_proto_plus=self.use_proto_plus),
        ]

        channel = grpc.intercept_channel(channel, *interceptors)

        service_transport = service_transport_class(
            channel=channel, client_info=_CLIENT_INFO
        )

        return service_client_class(transport=service_transport)

    def get_type(self, name, version=_DEFAULT_VERSION):
        """Returns the specified common, enum, error, or resource type.

        Args:
            name: a str indicating the name of the type that is being retrieved;
              e.g. you may specify "CampaignOperation" to retrieve a
              CampaignOperation instance.
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A Message instance representing the desired type.

        Raises:
            ValueError: If the type for the specified name doesn't exist
                in the given version.
        """
        # check that the name isn't a literal pb2 file name.
        if name.lower().endswith("pb2"):
            raise ValueError(
                f"Specified type '{name}' must be a class, not a module"
            )

        # check that a service or transport class isn't being requested
        # because they are initialized differently than normal message types.
        if name.lower().endswith("serviceclient") or name.lower().endswith(
            "transport"
        ):
            raise ValueError(
                f"Specified type '{name}' must not be a service "
                "or transport class."
            )

        # If version is specified when the instance is created,
        # override any version specified as an argument.
        version = self.version if self.version else version

        try:
            type_classes = self._get_api_services_by_version(version)
            message_class = getattr(type_classes, name)
        except AttributeError:
            raise ValueError(
                f"Specified type '{name}' does not exist in "
                f"Google Ads API {version}"
            )

        if self.use_proto_plus == True:
            return message_class()
        else:
            return util.convert_proto_plus_to_protobuf(message_class())
