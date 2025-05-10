from functools import cached_property
from typing import Any

from airflow.hooks.base import BaseHook
from asgiref.sync import sync_to_async

from airflow.providers.common.opendal.connections.connection_parser import OpenDALOperatorFactory, \
    OpenDALAirflowConnectionParser
from opendal import Operator, AsyncOperator


class OpenDALHook(BaseHook):
    """
    OpenDALHook is a base hook for OpenDAL tasks.

    """
    conn_name_attr = "opendal_conn_id"
    default_conn_name = "opendal_default"
    conn_type = "opendal"
    hook_name = "OpenDAL Hook"

    def __init__(self, config: dict[str, Any] = None, config_type: str = "source"):
        super().__init__()
        self._factory = OpenDALOperatorFactory()
        self.config_type = config_type
        self.config = config or {}
        self.conn_id = self.config.get("conn_id") or self.default_conn_name

    @cached_property
    def get_operator(self) -> Operator:
        conn = self.get_connection(self.conn_id)
        op_args = OpenDALOperatorFactory().get_opendal_operator_args(
            conn,
            self.config.get("operator_args"),
            self.config_type,
        )
        print(f"op_args: {op_args}")
        return Operator(**op_args)

    @cached_property
    async def async_get_operator(self) -> Operator:
        conn = self.get_connection(self.conn_id)
        op_args = await sync_to_async(OpenDALOperatorFactory().get_opendal_operator_args)(
            conn,
            self.config.get("operator_args"),
            self.config_type,
        )
        return AsyncOperator(**op_args)

    def register_parsers(self, parser: OpenDALAirflowConnectionParser):
        """Register a custom connection parsers."""
        self._factory.register_connection_parser(parser)
