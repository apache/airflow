from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from asgiref.sync import sync_to_async
from opendal import AsyncOperator, Operator

from airflow.hooks.base import BaseHook
from airflow.providers.common.opendal.connections.connection_parser import (
    OpenDALAirflowConnectionParser,
    OpenDALConnectionFactory,
)

if TYPE_CHECKING:
    from airflow.sdk import Connection


class OpenDALHook(BaseHook):
    """
    OpenDALHook to parse connection.

    :param opendal_conn_id: The connection ID for OpenDAL. This is the default opendal_default.
    :param config: The configuration for the OpenDAL operator. This will either source_config or destination_config.
                   source_config is referred to as source operator and destination_config is referred to as destination operator.

    :param config_type: The type of the config, either "source" or "destination".
    """

    conn_name_attr = "opendal_conn_id"
    default_conn_name = "opendal_default"
    conn_type = "opendal"
    hook_name = "OpenDAL Hook"

    _opendal_conn_factory = OpenDALConnectionFactory()

    def __init__(self,
                 opendal_conn_id: str | None = default_conn_name,
                 config: dict[str, Any] = None,
                 config_type: str = "source"):
        super().__init__()
        self.opendal_conn_id = opendal_conn_id
        self.config_type = config_type
        self.config = config or {}

    def fetch_conn(self) -> Connection:
        """

        Fetch the connection object from the Airflow connection.

        A sample config:
            {
                "conn_id": "opendal_test",
                "operator_args": {},
                "path": "/tmp/file/hello.txt"
            }
        if the conn_id is not provided in the task input opendal_config, it will use the `opendal_conn_id`

        The `opendal_conn_id` connection also has similar config as input opendal input config.
        {
            "source_config": {
                "conn_id": "opendal_test",
                "operator_args": {},
                "path": "/tmp/file/hello.txt"
            },
            "destination_config": {
                "conn_id": "opendal_test",
                "operator_args": {},
                "path": "/tmp/file/hello.txt"
            }
        }
        """
        conn_id_from_opendal_input_config = self.config.get("conn_id")

        conn_id = None

        if not conn_id_from_opendal_input_config:
            conn = self.get_connection(self.opendal_conn_id)
            connection_id_from_opendal_connection = conn.extra_dejson.get(
                f"{self.config_type}_config", {}).get("conn_id")

            conn_id = connection_id_from_opendal_connection or self.opendal_conn_id


        return self.get_connection(conn_id_from_opendal_input_config or conn_id)


    @cached_property
    def get_operator(self) -> Operator:

        conn = self.fetch_conn()

        op_args = self._opendal_conn_factory.get_opendal_operator_args(
            conn,
            self.config.get("operator_args", {}),
            self.config_type,
        )

        return Operator(**op_args)

    @cached_property
    async def async_get_operator(self) -> Operator:
        conn = await sync_to_async(self.fetch_conn)()
        op_args = await sync_to_async(self._opendal_conn_factory.get_opendal_operator_args)(
            conn,
            self.config.get("operator_args"),
            self.config_type,
        )
        return AsyncOperator(**op_args)

    @classmethod
    def register_parsers(cls, parser: OpenDALAirflowConnectionParser):
        """Register a custom connection parsers."""
        cls._opendal_conn_factory.register_connection_parser(parser)
