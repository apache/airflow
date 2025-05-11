import importlib
from typing import AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow.providers.common.opendal.filesystem.opendal_fs import OpenDALConfig
from airflow.providers.common.opendal.hooks.opendal import OpenDALHook


class OpenDALTrigger(BaseTrigger):
    """
    OpenDAL Trigger class for Airflow.

    This class is used to trigger OpenDAL operations in Airflow.
    It inherits from the BaseTrigger class and implements the required methods.
    """

    def __init__(self,
                 opendal_config: OpenDALConfig,
                 action: str,
                 opendal_conn_id: str = "opendal_default",
                 data: str | bytes = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.opendal_config = opendal_config
        self.action = action
        self.opendal_conn_id = opendal_conn_id
        self.data = data

    def serialize(self) -> tuple:
        return  (
            "airflow.providers.common.opendal.triggers.opendal.OpenDALTrigger",
            {
                "opendal_config": self.opendal_config,
                "action": self.action,
                "opendal_conn_id": self.opendal_conn_id,
                "data": self.data,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:

        source_operator_hook = await self.hook(self.opendal_config.get("source_config"))

        destination_operator = await self.hook(self.opendal_config.get("destination_config"),
                                              "destination") if self.opendal_config.get(
            "destination_config") else None

        module = importlib.import_module("airflow.providers.common.opendal.filesystem.opendal_fs")
        operator_class = getattr(module, f"OpenDAL{self.action.capitalize()}")

        opendal_operator = operator_class(
            opendal_config=self.opendal_config,
            source_operator= await source_operator_hook.async_get_operator,
            destination_operator= await destination_operator.async_get_operator if destination_operator else None,
            data=self.data,
        )

        result = await opendal_operator.async_execute_opendal_task()

        yield TriggerEvent(result)

    async def hook(self, config, config_type: str = "source") -> OpenDALHook:
        """
        Create a hook for OpenDAL tasks.

        :param config: The OpenDAL input configuration. either source_config or destination_config.
        :param config_type: The type of OpenDAL configuration (source or destination).
        :return: The OpenDAL hook.

        """
        return OpenDALHook(config=config,
                           opendal_conn_id=self.opendal_conn_id,
                           config_type=config_type)
