from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.sdk import Connection


class OpenDALAirflowConnectionParser(ABC):
    """OpenDALConnectionParser is a base class for OpenDAL connection parsers to parse Airflow connections."""

    airflow_conn_type: str

    @abstractmethod
    def parse(self, conn: Connection) -> dict[str, Any]:
        pass

    @staticmethod
    def filter_none(params: dict[str, Any]) -> dict[str, Any]:
        """

        Filter out None values from the dictionary.
        """

        return {k: v for k, v in params.items() if v is not None}




class S3ConnectionParser(OpenDALAirflowConnectionParser):
    """S3ConnectionParser to parse AWS S3 connection."""

    airflow_conn_type = "aws"

    def parse(self, conn: Connection) -> dict[str, Any]:
        """Parse the connection and return an OpenDAL operator args for s3 scheme."""

        params = {}

        params.update({
            "bucket": conn.extra_dejson.get("bucket"),
            "access_key_id": conn.login or conn.extra_dejson.get("aws_access_key_id"),
            "secret_access_key": conn.password or conn.extra_dejson.get("aws_secret_access_key"),
            "region": conn.extra_dejson.get("region"),
            "endpoint": conn.extra_dejson.get("endpoint"),
        })

        return {"scheme": "s3",  **self.filter_none(params)}


class GCSConnectionParser(OpenDALAirflowConnectionParser):
    """GCSConnectionParser to parse google cloud storage connection."""

    airflow_conn_type = "google_cloud_platform"

    def parse(self, conn: Connection) -> dict[str, Any]:
        """Parse the connection and return an OpenDAL operator args for gcs scheme."""

        from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
        gcs_conn = GoogleBaseHook(gcp_conn_id=conn.conn_id)

        params = {}

        params.update({
            "bucket": conn.extra_dejson.get("bucket"),
            "credential_path": conn.extra_dejson.get("key_path"),
            "scope": conn.extra_dejson.get("scope"),
            "service_account": conn.extra_dejson.get("service_account"),
            "token": gcs_conn._get_access_token(),
        })


        return {"scheme": "gcs", **self.filter_none(params)}


class OpenDALConnectionFactory:
    """OpenDALConnectionFactory for OpenDAL connection parsers."""

    _connection_parsers = []

    def __init__(self):
        self._register_default_connection_parser()

    def _register_default_connection_parser(self):
        default_parsers = [
            S3ConnectionParser(),
            GCSConnectionParser(),
        ]
        [self.register_connection_parser(parser) for parser in default_parsers]

    @classmethod
    def register_connection_parser(cls, parser: OpenDALAirflowConnectionParser):
        cls._connection_parsers.append(parser)


    def get_opendal_operator_args(self, conn: Connection,
                                  opendal_config_operator_args: dict[str, Any] = None,
                                  config_type: str = "source",
                                  ) -> dict[str, Any]:
        """
        Get operator args from the connection and input opendal config.

        We will merge the connection parameters with the opendal config input parameters.

        :param conn: The connection object.
        :param opendal_config_operator_args: The opendal config operator args from the task input.
        :param config_type: The type of the config, either "source" or "destination".

        """
        conn_scheme = conn.conn_type.lower()

        # Fetch the secrets from the connection
        conn_params = {}

        # If any connection have operator_args defined in source/destination_config, we consider them.
        conn_operator_args = {}

        for parser in self._connection_parsers:
            if conn_scheme in parser.airflow_conn_type:
                conn_params = parser.parse(conn)
                break

        if config_type == "destination":
            conn_operator_args = conn.extra_dejson.get("destination_config", {}).get("operator_args", {})

        if config_type == "source":
            conn_operator_args = conn.extra_dejson.get("source_config", {}).get("operator_args", {})

        return {**conn_params, **conn_operator_args, **opendal_config_operator_args}
