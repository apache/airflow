from .connection import AsyncConnection, Connection, IsolationLevel  # noqa: F401
from .cursor import AsyncCursor, Cursor, YdbQuery  # noqa: F401
from .errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)


class YdbDBApi:
    def __init__(self):
        self.paramstyle = "pyformat"
        self.threadsafety = 0
        self.apilevel = "1.0"
        self._init_dbapi_attributes()

    def _init_dbapi_attributes(self):
        for name, value in {
            "Warning": Warning,
            "Error": Error,
            "InterfaceError": InterfaceError,
            "DatabaseError": DatabaseError,
            "DataError": DataError,
            "OperationalError": OperationalError,
            "IntegrityError": IntegrityError,
            "InternalError": InternalError,
            "ProgrammingError": ProgrammingError,
            "NotSupportedError": NotSupportedError,
        }.items():
            setattr(self, name, value)

    def connect(self, *args, **kwargs) -> Connection:
        return Connection(*args, **kwargs)

    def async_connect(self, *args, **kwargs) -> AsyncConnection:
        return AsyncConnection(*args, **kwargs)
