import collections.abc
import posixpath
from typing import Any, List, NamedTuple, Optional

import sqlalchemy.util as util
import ydb

from .cursor import AsyncCursor, Cursor
from .errors import InterfaceError, InternalError, NotSupportedError


class IsolationLevel:
    SERIALIZABLE = "SERIALIZABLE"
    ONLINE_READONLY = "ONLINE READONLY"
    ONLINE_READONLY_INCONSISTENT = "ONLINE READONLY INCONSISTENT"
    STALE_READONLY = "STALE READONLY"
    SNAPSHOT_READONLY = "SNAPSHOT READONLY"
    AUTOCOMMIT = "AUTOCOMMIT"


class Connection:
    _await = staticmethod(util.await_only)

    _is_async = False
    _ydb_driver_class = ydb.Driver
    _ydb_session_pool_class = ydb.SessionPool
    _ydb_table_client_class = ydb.TableClient
    _cursor_class = Cursor

    def __init__(
        self,
        host: str = "",
        port: str = "",
        database: str = "",
        **conn_kwargs: Any,
    ):
        self.endpoint = f"grpc://{host}:{port}"
        self.database = database
        self.conn_kwargs = conn_kwargs
        self.credentials = self.conn_kwargs.pop("credentials", None)
        self.table_path_prefix = self.conn_kwargs.pop("ydb_table_path_prefix", "")

        if "ydb_session_pool" in self.conn_kwargs:  # Use session pool managed manually
            self._shared_session_pool = True
            self.session_pool: ydb.SessionPool = self.conn_kwargs.pop("ydb_session_pool")
            self.driver = (
                self.session_pool._driver
                if hasattr(self.session_pool, "_driver")
                else self.session_pool._pool_impl._driver
            )
            self.driver.table_client = self._ydb_table_client_class(self.driver, self._get_table_client_settings())
        else:
            self._shared_session_pool = False
            self.driver = self._create_driver()
            self.session_pool = self._ydb_session_pool_class(self.driver, size=5)

        self.interactive_transaction: bool = False  # AUTOCOMMIT
        self.tx_mode: ydb.AbstractTransactionModeBuilder = ydb.SerializableReadWrite()
        self.tx_context: Optional[ydb.TxContext] = None

    def cursor(self):
        return self._cursor_class(
            self.driver, self.session_pool, self.tx_mode, self.tx_context, self.table_path_prefix
        )

    def describe(self, table_path: str) -> ydb.TableDescription:
        abs_table_path = posixpath.join(self.database, self.table_path_prefix, table_path)
        cursor = self.cursor()
        return cursor.describe_table(abs_table_path)

    def check_exists(self, table_path: str) -> ydb.SchemeEntry:
        abs_table_path = posixpath.join(self.database, self.table_path_prefix, table_path)
        cursor = self.cursor()
        return cursor.check_exists(abs_table_path)

    def get_table_names(self) -> List[str]:
        abs_dir_path = posixpath.join(self.database, self.table_path_prefix)
        cursor = self.cursor()
        return [posixpath.relpath(path, abs_dir_path) for path in cursor.get_table_names(abs_dir_path)]

    def set_isolation_level(self, isolation_level: str):
        class IsolationSettings(NamedTuple):
            ydb_mode: ydb.AbstractTransactionModeBuilder
            interactive: bool

        ydb_isolation_settings_map = {
            IsolationLevel.AUTOCOMMIT: IsolationSettings(ydb.SerializableReadWrite(), interactive=False),
            IsolationLevel.SERIALIZABLE: IsolationSettings(ydb.SerializableReadWrite(), interactive=True),
            IsolationLevel.ONLINE_READONLY: IsolationSettings(ydb.OnlineReadOnly(), interactive=False),
            IsolationLevel.ONLINE_READONLY_INCONSISTENT: IsolationSettings(
                ydb.OnlineReadOnly().with_allow_inconsistent_reads(), interactive=False
            ),
            IsolationLevel.STALE_READONLY: IsolationSettings(ydb.StaleReadOnly(), interactive=False),
            IsolationLevel.SNAPSHOT_READONLY: IsolationSettings(ydb.SnapshotReadOnly(), interactive=True),
        }
        ydb_isolation_settings = ydb_isolation_settings_map[isolation_level]
        if self.tx_context and self.tx_context.tx_id:
            raise InternalError("Failed to set transaction mode: transaction is already began")
        self.tx_mode = ydb_isolation_settings.ydb_mode
        self.interactive_transaction = ydb_isolation_settings.interactive

    def get_isolation_level(self) -> str:
        if self.tx_mode.name == ydb.SerializableReadWrite().name:
            if self.interactive_transaction:
                return IsolationLevel.SERIALIZABLE
            else:
                return IsolationLevel.AUTOCOMMIT
        elif self.tx_mode.name == ydb.OnlineReadOnly().name:
            if self.tx_mode.settings.allow_inconsistent_reads:
                return IsolationLevel.ONLINE_READONLY_INCONSISTENT
            else:
                return IsolationLevel.ONLINE_READONLY
        elif self.tx_mode.name == ydb.StaleReadOnly().name:
            return IsolationLevel.STALE_READONLY
        elif self.tx_mode.name == ydb.SnapshotReadOnly().name:
            return IsolationLevel.SNAPSHOT_READONLY
        else:
            raise NotSupportedError(f"{self.tx_mode.name} is not supported")

    def begin(self):
        self.tx_context = None
        if self.interactive_transaction and not self.use_scan_query:
            session = self._maybe_await(self.session_pool.acquire)
            self.tx_context = session.transaction(self.tx_mode)
            self._maybe_await(self.tx_context.begin)

    def commit(self):
        if self.tx_context and self.tx_context.tx_id:
            self._maybe_await(self.tx_context.commit)
            self._maybe_await(self.session_pool.release, self.tx_context.session)
            self.tx_context = None

    def rollback(self):
        if self.tx_context and self.tx_context.tx_id:
            self._maybe_await(self.tx_context.rollback)
            self._maybe_await(self.session_pool.release, self.tx_context.session)
            self.tx_context = None

    def close(self):
        self.rollback()
        if not self._shared_session_pool:
            self._maybe_await(self.session_pool.stop)
            self._stop_driver()

    @classmethod
    def _maybe_await(cls, callee: collections.abc.Callable, *args, **kwargs) -> Any:
        if cls._is_async:
            return cls._await(callee(*args, **kwargs))
        return callee(*args, **kwargs)

    def _get_table_client_settings(self) -> ydb.TableClientSettings:
        return (
            ydb.TableClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_timestamp_in_result_sets(True)
            .with_native_interval_in_result_sets(True)
            .with_native_json_in_result_sets(False)
        )

    def _create_driver(self):
        driver_config = ydb.DriverConfig(
            endpoint=self.endpoint,
            database=self.database,
            table_client_settings=self._get_table_client_settings(),
            credentials=self.credentials,
        )
        driver = self._ydb_driver_class(driver_config)
        try:
            self._maybe_await(driver.wait, timeout=5, fail_fast=True)
        except ydb.Error as e:
            raise InterfaceError(e.message, original_error=e) from e
        except Exception as e:
            self._maybe_await(driver.stop)
            raise InterfaceError(f"Failed to connect to YDB, details {driver.discovery_debug_details()}") from e
        return driver

    def _stop_driver(self):
        self._maybe_await(self.driver.stop)


class AsyncConnection(Connection):
    _is_async = True
    _ydb_driver_class = ydb.aio.Driver
    _ydb_session_pool_class = ydb.aio.SessionPool
    _ydb_table_client_class = ydb.aio.table.TableClient
    _cursor_class = AsyncCursor
