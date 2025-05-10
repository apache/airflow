import importlib
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.sdk import Context
from airflow.sdk.bases.operator import BaseOperator

from airflow.providers.common.opendal.connections.connection_parser import OpenDALOperatorFactory
from airflow.providers.common.opendal.filesystem.opendal_fs import OpenDALConfig
from airflow.providers.common.opendal.hooks.opendal import OpenDALHook


class OpenDALTaskOperator(BaseOperator):

    """
    OpenDALTaskOperator is a base operator for OpenDAL tasks.

    :param task_id: The task ID.
    :param dag_id: The DAG ID.
    :param open_dal_task: The OpenDAL task to execute.
    :param open_dal_config: The OpenDAL configuration.
    """

    def __init__(self,
                 *,
                 opendal_config: OpenDALConfig,
                 data: str | bytes = None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.opendal_config = opendal_config
        self.data = data

    def execute(self, context: Context) -> Any:

        action = self.opendal_config.get("action")

        source_operator = self.hook(self.opendal_config.get("source_config")).get_operator
        destination_operator = self.hook(self.opendal_config.get("destination_config"), "destination").get_operator if self.opendal_config.get("destination_config") else None

        module = importlib.import_module(f"airflow.providers.common.opendal.filesystem.opendal_fs")
        operator_class = getattr(module, f"OpenDAL{action.capitalize()}")

        opendal_operator = operator_class(
            opendal_config=self.opendal_config,
            source_operator=source_operator,
            destination_operator=destination_operator,
            data=self.data,
        )

        return opendal_operator.execute_opendal_task()



    def hook(self, config, config_type: str="source") -> OpenDALHook:
        """
        Create a hook for OpenDAL tasks.

        :param config: The OpenDAL configuration.
        :param config_type: The type of OpenDAL configuration (source or destination).
        :return: The OpenDAL hook.

        """
        return OpenDALHook(config, config_type=config_type)




