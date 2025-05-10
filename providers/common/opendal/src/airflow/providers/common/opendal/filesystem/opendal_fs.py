from typing import Any, Optional, Literal
from opendal import Operator, AsyncOperator
from pydantic import BaseModel


class SourceConfig(BaseModel):
    conn_id: str = "opendal_default"
    operator_args: Optional[dict[str, str]] = None
    path: str = None


class DestinationConfig(BaseModel):
    conn_id: str = None
    operator_args: Optional[dict[str, str]] = None
    path: str = None


class OpenDALConfig(BaseModel):
    action: Literal["read", "write", "copy"]
    source_config: SourceConfig
    destination_config: Optional[DestinationConfig] = None


class OpenDALBaseFileSystem:
    def __init__(self,
                 opendal_config: OpenDALConfig,
                 source_operator: Operator | AsyncOperator,
                 destination_operator: Operator | AsyncOperator = None,
                 data: bytes = None
        ):
        self.opendal_config = opendal_config
        self.source_operator = source_operator
        self.destination_operator = destination_operator
        self.data = data

    def execute_opendal_task(self):
        pass

    async def async_execute_opendal_task(self):
        pass


class OpenDALRead(OpenDALBaseFileSystem):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute_opendal_task(self):
        return self.source_operator.read(self.opendal_config.get("source_config",{}).get("path", "/")).decode("utf-8")


class OpenDALWrite(OpenDALBaseFileSystem):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute_opendal_task(self):
        if self.data and isinstance(self.data, str):
            self.data = self.data.encode("utf-8")

        return self.source_operator.write(self.opendal_config.get("source_config", {}).get("path"), self.data)
