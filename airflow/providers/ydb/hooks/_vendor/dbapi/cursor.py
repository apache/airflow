import collections.abc
from collections.abc import AsyncIterator
import dataclasses
import functools
import hashlib
import itertools
import logging
import posixpath
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

import ydb
import ydb.aio
from sqlalchemy import util

from .errors import (
    DatabaseError,
    DataError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

logger = logging.getLogger(__name__)


def get_column_type(type_obj: Any) -> str:
    return str(ydb.convert.type_to_native(type_obj))


@dataclasses.dataclass
class YdbQuery:
    yql_text: str
    parameters_types: Dict[str, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]] = dataclasses.field(
        default_factory=dict
    )
    is_ddl: bool = False
    use_scan_query: bool = False


def _handle_ydb_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ydb.issues.AlreadyExists, ydb.issues.PreconditionFailed) as e:
            raise IntegrityError(e.message, original_error=e) from e
        except (ydb.issues.Unsupported, ydb.issues.Unimplemented) as e:
            raise NotSupportedError(e.message, original_error=e) from e
        except (ydb.issues.BadRequest, ydb.issues.SchemeError) as e:
            raise ProgrammingError(e.message, original_error=e) from e
        except (
            ydb.issues.TruncatedResponseError,
            ydb.issues.ConnectionError,
            ydb.issues.Aborted,
            ydb.issues.Unavailable,
            ydb.issues.Overloaded,
            ydb.issues.Undetermined,
            ydb.issues.Timeout,
            ydb.issues.Cancelled,
            ydb.issues.SessionBusy,
            ydb.issues.SessionExpired,
            ydb.issues.SessionPoolEmpty,
        ) as e:
            raise OperationalError(e.message, original_error=e) from e
        except ydb.issues.GenericError as e:
            raise DataError(e.message, original_error=e) from e
        except ydb.issues.InternalError as e:
            raise InternalError(e.message, original_error=e) from e
        except ydb.Error as e:
            raise DatabaseError(e.message, original_error=e) from e
        except Exception as e:
            raise DatabaseError("Failed to execute query") from e

    return wrapper


class Cursor:
    def __init__(
        self,
        driver: Union[ydb.Driver, ydb.aio.Driver],
        session_pool: Union[ydb.SessionPool, ydb.aio.SessionPool],
        tx_mode: ydb.AbstractTransactionModeBuilder,
        tx_context: Optional[ydb.BaseTxContext] = None,
        table_path_prefix: str = "",
    ):
        self.driver = driver
        self.session_pool = session_pool
        self.tx_mode = tx_mode
        self.tx_context = tx_context
        self.description = None
        self.arraysize = 1
        self.rows = None
        self._rows_prefetched = None
        self.root_directory = table_path_prefix

    @_handle_ydb_errors
    def describe_table(self, abs_table_path: str) -> ydb.TableDescription:
        return self._retry_operation_in_pool(self._describe_table, abs_table_path)

    def check_exists(self, abs_table_path: str) -> bool:
        try:
            self._retry_operation_in_pool(self._describe_path, abs_table_path)
            return True
        except ydb.SchemeError:
            return False

    @_handle_ydb_errors
    def get_table_names(self, abs_dir_path: str) -> List[str]:
        directory: ydb.Directory = self._retry_operation_in_pool(self._list_directory, abs_dir_path)
        result = []
        for child in directory.children:
            child_abs_path = posixpath.join(abs_dir_path, child.name)
            if child.is_table():
                result.append(child_abs_path)
            elif child.is_directory() and not child.name.startswith("."):
                result.extend(self.get_table_names(child_abs_path))
        return result

    def execute(self, operation: YdbQuery, parameters: Optional[Mapping[str, Any]] = None):
        query = self._get_ydb_query(operation)

        logger.info("execute sql: %s, params: %s", query, parameters)
        if operation.is_ddl:
            chunks = self._execute_ddl(query)
        elif operation.use_scan_query:
            chunks = self._execute_scan_query(query, parameters)
        else:
            chunks = self._execute_dml(query, parameters)

        rows = self._rows_iterable(chunks)
        # Prefetch the description:
        try:
            first_row = next(rows)
        except StopIteration:
            pass
        else:
            rows = itertools.chain((first_row,), rows)
        if self.rows is not None:
            rows = itertools.chain(self.rows, rows)

        self.rows = rows

    def _get_ydb_query(self, operation: YdbQuery) -> Union[ydb.DataQuery, str]:
        pragma = ""
        if self.root_directory:
            pragma = f'PRAGMA TablePathPrefix = "{self.root_directory}";\n'

        yql_with_pragma = pragma + operation.yql_text

        if operation.is_ddl or not operation.parameters_types:
            return yql_with_pragma

        return self._make_data_query(yql_with_pragma, operation.parameters_types)

    def _make_data_query(
        self,
        yql_text: str,
        parameters_types: Dict[str, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]],
    ) -> ydb.DataQuery:
        """
        ydb.DataQuery uses hashed SQL text as cache key, which may cause issues if parameters change type within
        the same session, so we include parameter types to the key to prevent false positive cache hit.
        """

        sorted_parameters = sorted(parameters_types.items())  # dict keys are unique, so the sorting is stable

        yql_with_params = yql_text + "".join([k + str(v) for k, v in sorted_parameters])
        name = hashlib.sha256(yql_with_params.encode("utf-8")).hexdigest()
        return ydb.DataQuery(yql_text, parameters_types, name=name)

    @_handle_ydb_errors
    def _execute_scan_query(
        self, query: Union[ydb.DataQuery, str], parameters: Optional[Mapping[str, Any]] = None
    ) -> Generator[ydb.convert.ResultSet, None, None]:
        prepared_query = query
        if isinstance(query, str) and parameters:
            prepared_query: ydb.DataQuery = self._retry_operation_in_pool(self._prepare, query)

        if isinstance(query, str):
            scan_query = ydb.ScanQuery(query, None)
        else:
            scan_query = ydb.ScanQuery(prepared_query.yql_text, prepared_query.parameters_types)

        return self._execute_scan_query_in_driver(scan_query, parameters)

    @_handle_ydb_errors
    def _execute_dml(
        self, query: Union[ydb.DataQuery, str], parameters: Optional[Mapping[str, Any]] = None
    ) -> ydb.convert.ResultSets:
        prepared_query = query
        if isinstance(query, str) and parameters:
            if self.tx_context:
                prepared_query = self._run_operation_in_session(self._prepare, query)
            else:
                prepared_query = self._retry_operation_in_pool(self._prepare, query)

        if self.tx_context:
            return self._run_operation_in_tx(self._execute_in_tx, prepared_query, parameters)

        return self._retry_operation_in_pool(self._execute_in_session, self.tx_mode, prepared_query, parameters)

    @_handle_ydb_errors
    def _execute_ddl(self, query: str) -> ydb.convert.ResultSets:
        return self._retry_operation_in_pool(self._execute_scheme, query)

    @staticmethod
    def _execute_scheme(session: ydb.Session, query: str) -> ydb.convert.ResultSets:
        return session.execute_scheme(query)

    @staticmethod
    def _describe_table(session: ydb.Session, abs_table_path: str) -> ydb.TableDescription:
        return session.describe_table(abs_table_path)

    @staticmethod
    def _describe_path(session: ydb.Session, table_path: str) -> ydb.SchemeEntry:
        return session._driver.scheme_client.describe_path(table_path)

    @staticmethod
    def _list_directory(session: ydb.Session, abs_dir_path: str) -> ydb.Directory:
        return session._driver.scheme_client.list_directory(abs_dir_path)

    @staticmethod
    def _prepare(session: ydb.Session, query: str) -> ydb.DataQuery:
        return session.prepare(query)

    @staticmethod
    def _execute_in_tx(
        tx_context: ydb.TxContext, prepared_query: ydb.DataQuery, parameters: Optional[Mapping[str, Any]]
    ) -> ydb.convert.ResultSets:
        return tx_context.execute(prepared_query, parameters, commit_tx=False)

    @staticmethod
    def _execute_in_session(
        session: ydb.Session,
        tx_mode: ydb.AbstractTransactionModeBuilder,
        prepared_query: ydb.DataQuery,
        parameters: Optional[Mapping[str, Any]],
    ) -> ydb.convert.ResultSets:
        return session.transaction(tx_mode).execute(prepared_query, parameters, commit_tx=True)

    def _execute_scan_query_in_driver(
        self,
        scan_query: ydb.ScanQuery,
        parameters: Optional[Mapping[str, Any]],
    ) -> Generator[ydb.convert.ResultSet, None, None]:
        chunk: ydb.ScanQueryResult
        for chunk in self.driver.table_client.scan_query(scan_query, parameters):
            yield chunk.result_set

    def _run_operation_in_tx(self, callee: collections.abc.Callable, *args, **kwargs):
        return callee(self.tx_context, *args, **kwargs)

    def _run_operation_in_session(self, callee: collections.abc.Callable, *args, **kwargs):
        return callee(self.tx_context.session, *args, **kwargs)

    def _retry_operation_in_pool(self, callee: collections.abc.Callable, *args, **kwargs):
        return self.session_pool.retry_operation_sync(callee, None, *args, **kwargs)

    def _rows_iterable(self, chunks_iterable: ydb.convert.ResultSets):
        try:
            for chunk in chunks_iterable:
                self.description = [
                    (
                        col.name,
                        get_column_type(col.type),
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    for col in chunk.columns
                ]
                for row in chunk.rows:
                    # returns tuple to be compatible with SqlAlchemy and because
                    #  of this PEP to return a sequence: https://www.python.org/dev/peps/pep-0249/#fetchmany
                    yield row[::]
        except ydb.Error as e:
            raise DatabaseError(e.message, original_error=e) from e

    def _ensure_prefetched(self):
        if self.rows is not None and self._rows_prefetched is None:
            self._rows_prefetched = list(self.rows)
            self.rows = iter(self._rows_prefetched)
        return self._rows_prefetched

    def executemany(self, operation: YdbQuery, seq_of_parameters: Optional[Sequence[Mapping[str, Any]]]):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def executescript(self, script):
        return self.execute(script)

    def fetchone(self):
        return next(self.rows or [], None)

    def fetchmany(self, size=None):
        return list(itertools.islice(self.rows, size or self.arraysize))

    def fetchall(self):
        return list(self.rows)

    def nextset(self):
        self.fetchall()

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, column=None):
        pass

    def close(self):
        self.rows = None
        self._rows_prefetched = None

    @property
    def rowcount(self):
        return len(self._ensure_prefetched())


class AsyncCursor(Cursor):
    _await = staticmethod(util.await_only)

    @staticmethod
    async def _describe_table(session: ydb.aio.table.Session, abs_table_path: str) -> ydb.TableDescription:
        return await session.describe_table(abs_table_path)

    @staticmethod
    async def _describe_path(session: ydb.aio.table.Session, abs_table_path: str) -> ydb.SchemeEntry:
        return await session._driver.scheme_client.describe_path(abs_table_path)

    @staticmethod
    async def _list_directory(session: ydb.aio.table.Session, abs_dir_path: str) -> ydb.Directory:
        return await session._driver.scheme_client.list_directory(abs_dir_path)

    @staticmethod
    async def _execute_scheme(session: ydb.aio.table.Session, query: str) -> ydb.convert.ResultSets:
        return await session.execute_scheme(query)

    @staticmethod
    async def _prepare(session: ydb.aio.table.Session, query: str) -> ydb.DataQuery:
        return await session.prepare(query)

    @staticmethod
    async def _execute_in_tx(
        tx_context: ydb.aio.table.TxContext, prepared_query: ydb.DataQuery, parameters: Optional[Mapping[str, Any]]
    ) -> ydb.convert.ResultSets:
        return await tx_context.execute(prepared_query, parameters, commit_tx=False)

    @staticmethod
    async def _execute_in_session(
        session: ydb.aio.table.Session,
        tx_mode: ydb.AbstractTransactionModeBuilder,
        prepared_query: ydb.DataQuery,
        parameters: Optional[Mapping[str, Any]],
    ) -> ydb.convert.ResultSets:
        return await session.transaction(tx_mode).execute(prepared_query, parameters, commit_tx=True)

    def _execute_scan_query_in_driver(
        self,
        scan_query: ydb.ScanQuery,
        parameters: Optional[Mapping[str, Any]],
    ) -> Generator[ydb.convert.ResultSet, None, None]:
        iterator: AsyncIterator[ydb.ScanQueryResult] = self._await(
            self.driver.table_client.scan_query(scan_query, parameters)
        )
        while True:
            try:
                result = self._await(iterator.__anext__())
                yield result.result_set
            except StopAsyncIteration:
                break

    def _run_operation_in_tx(self, callee: collections.abc.Coroutine, *args, **kwargs):
        return self._await(callee(self.tx_context, *args, **kwargs))

    def _run_operation_in_session(self, callee: collections.abc.Coroutine, *args, **kwargs):
        return self._await(callee(self.tx_context.session, *args, **kwargs))

    def _retry_operation_in_pool(self, callee: collections.abc.Coroutine, *args, **kwargs):
        return self._await(self.session_pool.retry_operation(callee, *args, **kwargs))
