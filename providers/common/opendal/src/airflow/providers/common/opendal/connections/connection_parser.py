from abc import ABC, abstractmethod
from typing import Any

from airflow.sdk import Connection
from opendal import Operator, AsyncOperator


class OpenDALAirflowConnectionParser(ABC):
    """
    OpenDALConnectionParser is a connection parser for OpenDAL.
    """
    airflow_conn_type: str

    @abstractmethod
    def parse(self, conn: Connection) -> dict[str, Any]:
        pass




class S3ConnectionParser(OpenDALAirflowConnectionParser):
    """
    S3ConnectionParser is a connection parser for S3.
    """
    airflow_conn_type = "aws"

    def parse(self, conn: Connection) -> dict[str, Any]:
        """Parse the connection and return an OpenDAL operator."""
        params = conn.extra_dejson

        params.update({
            "bucket": conn.extra_dejson.get("bucket"),
            "access_key_id": conn.login or conn.extra_dejson.get("aws_access_key_id"),
            "secret_access_key": conn.password or conn.extra_dejson.get("aws_secret_access_key"),
            "region": conn.extra_dejson.get("region"),
            "endpoint": conn.extra_dejson.get("endpoint"),
        })

        params = {k: v for k, v in params.items() if v is not None}
        return {"scheme": "s3",  **params}


class OpenDALOperatorFactory:
    def __init__(self):
        self._connection_parsers = []
        self._register_default_connection_parser()

    def _register_default_connection_parser(self):
        self.register_connection_parser(S3ConnectionParser())

    def register_connection_parser(self, parser: OpenDALAirflowConnectionParser):
        self._connection_parsers.append(parser)


    def get_opendal_operator_args(self, conn: Connection, opendal_config_operator_args: dict[str, str] = None,
                                  config_type: str = "source",
                                  ) -> dict[str, Any]:
        """Get the operator args from the connection and OpenDAL config."""
        conn_scheme = conn.conn_type.lower()
        params = {}
        op_args = {}
        for parser in self._connection_parsers:
            if conn_scheme in parser.airflow_conn_type:
                params = parser.parse(conn)
                break

        if config_type == "destination":
            op_args = conn.extra_dejson.get("destination_config", {}).get("operator_args", {})

        if config_type == "source":
            op_args = conn.extra_dejson.get("source_config", {}).get("operator_args", {})

        print(f"params: {params}")
        return {**params, **op_args, **opendal_config_operator_args}
