from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from opendal import AsyncOperator, Operator

from pydantic import BaseModel


class SourceConfig(BaseModel):
    """SourceConfig for source operator configuration."""

    conn_id: str = "opendal_default"
    operator_args: dict[str, Any] | None = None
    path: str = None


class DestinationConfig(BaseModel):
    """DestinationConfig for destination operator configuration."""

    conn_id: str = None
    operator_args: dict[str, Any] | None = None
    path: str = None


class OpenDALConfig(BaseModel):
    """OpenDALConfig for source and destination configurations."""

    source_config: SourceConfig
    destination_config: DestinationConfig | None = None


class OpenDALBaseFileSystem:
    """OpenDALBaseFileSystem is a base class for OpenDAL file system operations."""

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

    @property
    def source_path(self) -> str:
        """Get the path from the source operator."""

        return self.opendal_config.get("source_config", {}).get("path")

    @property
    def destination_path(self) -> str | None:
        """Get the path from the destination operator."""

        if self.destination_operator:
            return self.opendal_config.get("destination_config", {}).get("path")
        return None



class OpenDALRead(OpenDALBaseFileSystem):
    """OpenDALReader to read from file."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute_opendal_task(self):

        return self.source_operator.read(self.source_path).decode("utf-8")

    async def async_execute_opendal_task(self):

        data = await self.source_operator.read(self.source_path)
        return data.decode("utf-8")


class OpenDALWrite(OpenDALBaseFileSystem):
    """OpenDALWriter to write to file."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute_opendal_task(self):
        if self.data and isinstance(self.data, str):
            self.data = self.data.encode("utf-8")

        return self.source_operator.write(self.source_path, self.data)

    async def async_execute_opendal_task(self):
        if self.data and isinstance(self.data, str):
            self.data = self.data.encode("utf-8")

        return await self.source_operator.write(self.source_path, self.data)


class OpenDALCopy(OpenDALBaseFileSystem):
    """OpenDALCopy to copy file."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute_opendal_task(self):

        with self.destination_operator.open(self.destination_path, "wb") as f:
            f.write(self.source_operator.read(self.source_path))

