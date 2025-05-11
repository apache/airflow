from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from airflow.providers.common.opendal.hooks.opendal import OpenDALHook
from airflow.sdk.bases.operator import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.opendal.filesystem.opendal_fs import OpenDALConfig
    from airflow.sdk import Context


class OpenDALTaskOperator(BaseOperator):
    """
    OpenDALTaskOperator is a base operator for OpenDAL tasks.

    :param opendal_config: The OpenDAL input configuration. either source_config or destination_config.
    :param action: The action to be performed. This can be one of the following: read, write, copy, delete, move.
    :param opendal_conn_id: The connection ID for OpenDAL. This is the default opendal_default.
    :param data: The data to be used in the OpenDAL task. This can be either a string or bytes.
    """

    def __init__(self,
                 *,
                 opendal_config: OpenDALConfig,
                 action: str,
                 opendal_conn_id: str = "opendal_default",
                 data: str | bytes = None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.opendal_config = opendal_config
        self.action = action
        self.data = data
        self.opendal_conn_id = opendal_conn_id
        self.source_operator = None
        self.destination_operator = None

    def execute(self, context: Context) -> Any:


        self.source_operator = self.hook(self.opendal_config.get("source_config")).get_operator
        self.destination_operator = self.hook(self.opendal_config.get("destination_config"), "destination").get_operator if self.opendal_config.get("destination_config") else None

        module = importlib.import_module("airflow.providers.common.opendal.filesystem.opendal_fs")
        operator_class = getattr(module, f"OpenDAL{self.action.capitalize()}")


        opendal_operator = operator_class(
            opendal_config=self.opendal_config,
            source_operator=self.source_operator,
            destination_operator=self.destination_operator,
            data=self.data,
        )

        return opendal_operator.execute_opendal_task()



    def hook(self, config, config_type: str="source") -> OpenDALHook:
        """
        Create a hook for OpenDAL tasks.

        :param config: The OpenDAL input configuration. either source_config or destination_config.
        :param config_type: The type of OpenDAL configuration (source or destination).
        :return: The OpenDAL hook.

        """
        return OpenDALHook(config=config,
                            opendal_conn_id=self.opendal_conn_id,
                           config_type=config_type)




