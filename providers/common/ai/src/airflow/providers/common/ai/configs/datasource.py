from dataclasses import dataclass


@dataclass
class DataSourceConfig:
    connection_id: str
    uri: str | None = None
    format: str | None = None
    table_name: str | None = None
    schema: str | None = None
