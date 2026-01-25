#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from collections.abc import Generator, Iterable, Mapping
from copy import deepcopy
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast
from urllib import parse
from warnings import warn

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from elasticsearch.helpers import bulk, parallel_bulk, reindex, scan, streaming_bulk

from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    import pandas as pd
    from elastic_transport import ObjectApiResponse

    from airflow.models.connection import Connection as AirflowConnection


def connect(
    host: str = "localhost",
    port: int = 9200,
    user: str | None = None,
    password: str | None = None,
    scheme: str = "http",
    **kwargs: Any,
) -> ESConnection:
    return ESConnection(host, port, user, password, scheme, **kwargs)


class ElasticsearchSQLCursor:
    """A PEP 249-like Cursor class for Elasticsearch SQL API."""

    def __init__(self, es: Elasticsearch, **kwargs):
        self.es = es
        self.body = {
            "fetch_size": kwargs.get("fetch_size", 1000),
            "field_multi_value_leniency": kwargs.get("field_multi_value_leniency", False),
        }
        self._response: ObjectApiResponse | None = None

    @property
    def response(self) -> ObjectApiResponse:
        return self._response or {}  # type: ignore

    @response.setter
    def response(self, value):
        self._response = value

    @property
    def cursor(self):
        return self.response.get("cursor")

    @property
    def rows(self):
        return self.response.get("rows", [])

    @property
    def rowcount(self) -> int:
        return len(self.rows)

    @property
    def description(self) -> list[tuple]:
        return [(column["name"], column["type"]) for column in self.response.get("columns", [])]

    def execute(
        self, statement: str, params: Iterable | Mapping[str, Any] | None = None
    ) -> ObjectApiResponse:
        self.body["query"] = statement
        if params:
            self.body["params"] = params
        self.response = self.es.sql.query(body=self.body)
        if self.cursor:
            self.body["cursor"] = self.cursor
        else:
            self.body.pop("cursor", None)
        return self.response

    def fetchone(self):
        if self.rows:
            return self.rows[0]
        return None

    def fetchmany(self, size: int | None = None):
        raise NotImplementedError()

    def fetchall(self):
        results = self.rows
        while self.cursor:
            self.execute(statement=self.body["query"])
            results.extend(self.rows)
        return results

    def close(self):
        self._response = None


class ESConnection:
    """wrapper class for elasticsearch.Elasticsearch."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        user: str | None = None,
        password: str | None = None,
        scheme: str = "http",
        **kwargs: Any,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.scheme = scheme
        self.kwargs = deepcopy(kwargs)
        kwargs.pop("fetch_size", None)
        kwargs.pop("field_multi_value_leniency", None)
        netloc = f"{host}:{port}"
        self.url = parse.urlunparse((scheme, netloc, "/", None, None, None))
        if user and password:
            self.es = Elasticsearch(self.url, http_auth=(user, password), **kwargs)
        else:
            self.es = Elasticsearch(self.url, **kwargs)

    def cursor(self) -> ElasticsearchSQLCursor:
        return ElasticsearchSQLCursor(self.es, **self.kwargs)

    def close(self):
        self.es.close()

    def commit(self):
        pass

    def execute_sql(
        self, query: str, params: Iterable | Mapping[str, Any] | None = None
    ) -> ObjectApiResponse:
        return self.cursor().execute(query, params)


class ElasticsearchSQLHook(DbApiHook):
    """
    Interact with Elasticsearch through the elasticsearch-dbapi.

    This hook uses the Elasticsearch conn_id.

    :param elasticsearch_conn_id: The :ref:`ElasticSearch connection id <howto/connection:elasticsearch>`
        used for Elasticsearch credentials.
    """

    conn_name_attr = "elasticsearch_conn_id"
    default_conn_name = "elasticsearch_default"
    connector = ESConnection  # type: ignore[assignment]
    conn_type = "elasticsearch"
    hook_name = "Elasticsearch"

    def __init__(self, schema: str = "http", connection: AirflowConnection | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = schema

    def get_conn(self) -> ESConnection:
        """Return an elasticsearch connection object."""
        conn = self.connection

        conn_args = {
            "host": cast("str", conn.host),
            "port": cast("int", conn.port),
            "user": conn.login or None,
            "password": conn.password or None,
            "scheme": conn.schema or "http",
        }

        conn_args.update(conn.extra_dejson)

        if conn_args.get("http_compress", False):
            conn_args["http_compress"] = bool(conn_args["http_compress"])

        return connect(**conn_args)  # type: ignore[arg-type]

    def get_uri(self) -> str:
        conn = self.connection

        login = ""
        if conn.login:
            login = f"{conn.login}:{conn.password}@"
        host = conn.host or ""
        if conn.port is not None:
            host += f":{conn.port}"
        uri = f"{conn.conn_type}+{conn.schema}://{login}{host}/"

        extras_length = len(conn.extra_dejson)
        if not extras_length:
            return uri

        uri += "?"

        for arg_key, arg_value in conn.extra_dejson.items():
            extras_length -= 1
            uri += f"{arg_key}={arg_value}"

            if extras_length:
                uri += "&"

        return uri

    def _get_polars_df(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs,
    ):
        # TODO: Custom ElasticsearchSQLCursor is incompatible with polars.read_database.
        # To support: either adapt cursor to polars._executor interface or create custom polars reader.
        # https://github.com/apache/airflow/pull/50454
        raise NotImplementedError("Polars is not supported for Elasticsearch")


class ElasticsearchPythonHook(BaseHook):
    """
    Interacts with Elasticsearch. This hook uses the official Elasticsearch Python Client.

    .. deprecated:: 2.10.0
        This hook is deprecated. Use :class:`~airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchHook` instead.

    :param hosts: list: A list of a single or many Elasticsearch instances. Example: ["http://localhost:9200"]
    :param es_conn_args: dict: Additional arguments you might need to enter to connect to Elasticsearch.
                                Example: {"ca_cert":"/path/to/cert", "basic_auth": "(user, pass)"}
    """

    def __init__(self, hosts: list[Any], es_conn_args: dict | None = None) -> None:
        super().__init__()
        warn(
            "ElasticsearchPythonHook is deprecated. "
            "Use airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchHook instead.",
            stacklevel=2,
        )
        self.hosts = hosts
        self.es_conn_args = es_conn_args or {}

    def _get_elastic_connection(self):
        """Return the Elasticsearch client."""
        client = Elasticsearch(self.hosts, **self.es_conn_args)
        return client

    @cached_property
    def get_conn(self):
        """Return the Elasticsearch client (cached)."""
        return self._get_elastic_connection()

    def search(self, query: dict[Any, Any], index: str = "_all") -> dict:
        """
        Return results matching a query using Elasticsearch DSL.

        :param index: str: The index you want to query
        :param query: dict: The query you want to run

        :returns: dict: The response 'hits' object from Elasticsearch
        """
        es_client = self.get_conn
        result = es_client.search(index=index, body=query)
        return result["hits"]


class ElasticsearchHook(BaseHook):
    """
    Hook for interacting with Elasticsearch.

    This hook provides a convenient interface to Elasticsearch operations
    including search, bulk operations, and data management.

    .. :no-index:
    """

    conn_name_attr = "elasticsearch_conn_id"
    default_conn_name = "elasticsearch_default"
    conn_type = "elasticsearch"
    hook_name = "Elasticsearch"

    def __init__(
        self, elasticsearch_conn_id: str = "elasticsearch_default", log_query: bool = False, **kwargs: Any
    ) -> None:
        """
        Initialize the Elasticsearch Hook.

        :param elasticsearch_conn_id: The Airflow connection ID for Elasticsearch.
            Default: *elasticsearch_default*.
        :param log_query: If *True*, queries will be logged for debugging purposes.
            Default: *False*.
        :param kwargs: Additional arguments passed to the parent BaseHook class.
        """
        import os

        super().__init__()
        self.conn_id = elasticsearch_conn_id
        self.log_query = log_query

        # Environment variables as fallback configuration
        self.vars: dict[str, str | bool | None] = {
            "host": os.getenv("ELASTICSEARCH_HOST"),
            "port": os.getenv("ELASTICSEARCH_PORT"),
            "username": os.getenv("ELASTICSEARCH_USERNAME"),
            "password": os.getenv("ELASTICSEARCH_PASSWORD"),
            "use_ssl": os.getenv("ELASTICSEARCH_USE_SSL", "false").lower() == "true",
            "verify_certs": os.getenv("ELASTICSEARCH_VERIFY_CERTS", "true").lower() == "true",
            "timeout": os.getenv("ELASTICSEARCH_TIMEOUT"),
            "max_retries": os.getenv("ELASTICSEARCH_MAX_RETRIES"),
        }

    @cached_property
    def conn(self) -> AirflowConnection:
        """
        Get the Airflow connection object for Elasticsearch.

        .. :no-index:
        """
        return cast("AirflowConnection", self.get_connection(self.conn_id))

    @cached_property
    def client(self) -> Elasticsearch:
        """
        Create and return an Elasticsearch client with environment variable fallback.

        Configuration priority:
        1. Airflow connection (highest priority)
        2. Environment variables (fallback)
        3. Default values (last resort)

        :return: Configured Elasticsearch client instance.
        :raises AirflowConfigException: If client creation fails due to configuration issues.

        .. :no-index:
        """
        # Get Airflow connection
        conn = self.get_connection(self.conn_id)

        # Configuration with fallback priority: connection -> env vars -> defaults
        host = conn.host or self.vars.get("host") or "localhost"
        port_env = self.vars.get("port")
        port = conn.port or (int(port_env) if port_env and isinstance(port_env, str) else None) or 9200
        schema = conn.schema or ("https" if self.vars.get("use_ssl") else None) or "http"

        # Build hosts list
        hosts = [f"{schema}://{host}:{port}"]
        client_args: dict[str, Any] = {"hosts": hosts}

        # Authentication: prioritize Airflow connection
        if conn.login and conn.password:
            client_args["basic_auth"] = (conn.login, conn.password)
        elif self.vars.get("username") and self.vars.get("password"):
            username = self.vars.get("username")
            password = self.vars.get("password")
            if username and password:
                client_args["basic_auth"] = (str(username), str(password))

        # Handle extra configuration from connection
        extra = conn.extra_dejson or {}

        # SSL configuration with fallback
        use_ssl = extra.get("use_ssl", self.vars.get("use_ssl"))
        if use_ssl:
            client_args["use_ssl"] = True
            verify_certs_value = extra.get("verify_certs", self.vars.get("verify_certs"))
            if verify_certs_value is not None:
                client_args["verify_certs"] = bool(verify_certs_value)

        # Add timeout and retry configuration
        timeout_extra = extra.get("timeout")
        timeout_env = self.vars.get("timeout")
        if timeout_extra:
            client_args["timeout"] = timeout_extra
        elif timeout_env and isinstance(timeout_env, str):
            client_args["timeout"] = int(timeout_env)

        max_retries_extra = extra.get("max_retries")
        max_retries_env = self.vars.get("max_retries")
        if max_retries_extra:
            client_args["max_retries"] = max_retries_extra
        elif max_retries_env and isinstance(max_retries_env, str):
            client_args["max_retries"] = int(max_retries_env)

        # Add other extra configuration, excluding already handled fields
        excluded_fields = ["use_ssl", "verify_certs", "timeout", "max_retries"]
        other_extra = {k: v for k, v in extra.items() if k not in excluded_fields}
        client_args.update(other_extra)

        self._log_config_source()

        try:
            return Elasticsearch(**client_args)
        except Exception as e:
            raise AirflowConfigException(
                f"Failed to create Elasticsearch client with connection '{self.conn_id}': {e}"
            )

    def _log_config_source(self) -> None:
        """Log the source of configuration for debugging purposes."""
        conn = self.get_connection(self.conn_id)
        config_sources: list[str] = []

        if conn.host:
            config_sources.append("Airflow connection")
        if self.vars.get("host"):
            config_sources.append("Environment variables")

        if config_sources:
            self.log.info("Elasticsearch configuration loaded from: %s", ", ".join(config_sources))
        else:
            self.log.info("Elasticsearch configuration using default values")

    def get_conn(self) -> Elasticsearch:
        """
        Get the Elasticsearch client connection.

        :return: Configured Elasticsearch client instance.

        .. :no-index:
        """
        return self.client

    def test_connection(self) -> bool:
        """
        Test the Elasticsearch connection and raise AirflowException for critical failures.

        :return: *True* if connection is successful, *False* otherwise.
        :raises AirflowException: If connection fails due to configuration issues.

        .. :no-index:
        """
        try:
            info = self.client.info()
            self.log.info("Successfully connected to Elasticsearch: %s", info.body)
            return True
        except ESConnectionError as e:
            raise AirflowException(
                f"Cannot connect to Elasticsearch cluster: {e}. Check your connection '{self.conn_id}' configuration or environment variables."
            )
        except Exception as e:
            self.log.error("Failed to connect to Elasticsearch: %s", e)
            return False

    def search(self, query: dict[str, Any], index_name: str, **kwargs: Any) -> dict[str, Any]:
        """
        Execute a search query against the specified Elasticsearch index.

        :param query: The Elasticsearch query dictionary containing the search criteria.
        :param index_name: The name of the index to search against.
        :param kwargs: Additional search parameters to pass to the Elasticsearch client.
        :return: Dictionary containing the search results from Elasticsearch.

        .. :no-index:
        """
        if self.log_query:
            self.log.info("Searching %s with Query: %s", index_name, query)

        response = self.client.search(index=index_name, body=query, **kwargs)
        return response.body if hasattr(response, "body") else dict(response)

    def bulk(self, actions: Iterable[Any], **kwargs: Any) -> tuple[int, list[Any]]:
        """
        Execute bulk operations on Elasticsearch.

        :param actions: An iterable of actions to execute in the bulk operation.
        :param kwargs: Additional bulk parameters to pass to the Elasticsearch client.
        :return: A tuple containing (success_count, failed_operations).

        .. :no-index:
        """
        self.log.info(
            "Executing bulk operation with %d actions",
            len(list(actions)) if hasattr(actions, "__len__") else "unknown number of",
        )
        result = bulk(self.client, actions, **kwargs)
        return (result[0], list(result[1]) if isinstance(result[1], (list, tuple)) else [])

    def streaming_bulk(
        self, actions: Iterable[Any], **kwargs: Any
    ) -> Generator[tuple[bool, dict[str, Any]], None, None]:
        """
        Execute streaming bulk operations on Elasticsearch.

        :param actions: An iterable of actions to execute in the streaming bulk operation.
        :param kwargs: Additional streaming bulk parameters to pass to the Elasticsearch client.
        :return: Generator yielding results of individual operations.

        .. :no-index:
        """
        self.log.info("Executing streaming bulk operation")
        yield from streaming_bulk(self.client, actions, **kwargs)

    def parallel_bulk(
        self, actions: Iterable[Any], **kwargs: Any
    ) -> Generator[tuple[bool, dict[str, Any]], None, None]:
        """
        Execute parallel bulk operations on Elasticsearch.

        :param actions: An iterable of actions to execute in the parallel bulk operation.
        :param kwargs: Additional parallel bulk parameters to pass to the Elasticsearch client.
        :return: Generator yielding results of individual operations.

        .. :no-index:
        """
        self.log.info("Executing parallel bulk operation")
        yield from parallel_bulk(self.client, actions, **kwargs)

    def scan(
        self,
        index: str | list[str] | None = None,
        query: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Generator[dict[str, Any], None, None]:
        """
        Scan and return all documents matching the query using the scroll API.

        :param index: The index name(s) to scan. Can be a single index or list of indices.
        :param query: Optional query dictionary to filter documents during the scan.
        :param kwargs: Additional scan parameters to pass to the Elasticsearch client.
        :return: Generator of all matching documents from the scan operation.

        .. :no-index:
        """
        self.log.info("Scanning index: %s", index)
        scan_kwargs = kwargs.copy()
        if query:
            scan_kwargs["query"] = query

        yield from scan(self.client, index=index, **scan_kwargs)

    def reindex(
        self,
        source_index: str | list[str],
        target_index: str,
        query: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, list[Any]]:
        """
        Reindex documents from source index(es) to a target index.

        :param source_index: The source index name(s) to reindex from.
            Can be a single index or list of indices.
        :param target_index: The target index name to reindex documents to.
        :param query: Optional query dictionary to filter which documents to reindex.
        :param kwargs: Additional reindex parameters to pass to the Elasticsearch client.
        :return: A tuple containing (success_count, failed_operations).

        .. :no-index:
        """
        self.log.info("Reindexing from %s to %s", source_index, target_index)
        reindex_kwargs = kwargs.copy()
        if query:
            reindex_kwargs["query"] = query

        result = reindex(self.client, source_index=source_index, target_index=target_index, **reindex_kwargs)
        return (result[0], list(result[1]) if isinstance(result[1], (list, tuple)) else [])

    def search_to_pandas(self, index: str, query: dict[str, Any], **kwargs: Any) -> pd.DataFrame:
        """
        Execute a search query and return results as a pandas DataFrame.

        :param index: The index name to search against.
        :param query: The Elasticsearch query dictionary containing the search criteria.
        :param kwargs: Additional search parameters to pass to the Elasticsearch client.
        :return: A pandas DataFrame containing the search results with _source data normalized.

        .. :no-index:
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for search_to_pandas method. "
                "Install it with: pip install apache-airflow-providers-elasticsearch[pandas]"
            ) from e

        res = self.client.search(index=index, body=query, **kwargs)
        hits = res["hits"]["hits"]

        if not hits:
            self.log.info("No results found for query")
            return pd.DataFrame()

        # Extract _source data and normalize to DataFrame
        source_data = [hit["_source"] for hit in hits]
        df = pd.json_normalize(source_data)

        self.log.info("Converted %d search results to DataFrame", len(hits))
        return df

    def scan_to_pandas(
        self,
        index: str | list[str] | None = None,
        query: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Scan all documents matching the query and return results as a pandas DataFrame.

        This method is useful for extracting large datasets from Elasticsearch
        that exceed the search API limits. It uses the scroll API to retrieve
        all matching documents.

        :param index: The index name(s) to scan. Can be a single index or list of indices.
        :param query: Optional query dictionary to filter documents during the scan.
        :param kwargs: Additional scan parameters such as scroll timeout and batch size.
        :return: A pandas DataFrame with all matching documents, including metadata columns.

        .. note::
            This method loads all documents into memory. For very large datasets,
            consider using the scan() method directly and processing in chunks.

        .. :no-index:
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for scan_to_pandas method. "
                "Install it with: pip install apache-airflow-providers-elasticsearch[pandas]"
            ) from e

        self.log.info("Scanning index: %s and converting to DataFrame", index)

        # Get all documents using scan
        scan_kwargs = kwargs.copy()
        if query:
            scan_kwargs["query"] = query

        docs = list(scan(self.client, index=index, **scan_kwargs))

        if not docs:
            self.log.info("No documents found for scan query")
            return pd.DataFrame()

        # Extract _source data from each document
        source_data: list[dict[str, Any]] = []
        for doc in docs:
            if "_source" in doc:
                # Add document metadata as columns if needed
                doc_data = doc["_source"].copy()
                # Optionally add metadata fields
                doc_data["_index"] = doc.get("_index")
                doc_data["_id"] = doc.get("_id")
                source_data.append(doc_data)
            else:
                # Fallback: use the document as-is if no _source
                source_data.append(doc)

        # Convert to DataFrame
        df = pd.json_normalize(source_data)

        self.log.info("Converted %d scanned documents to DataFrame with shape %s", len(docs), df.shape)
        return df

    def create_index(
        self,
        index_name: str,
        mappings: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        Create a new Elasticsearch index with proper error handling.

        :param index_name: The name of the index to create.
        :param mappings: Optional dictionary containing the index field mappings.
        :param settings: Optional dictionary containing the index settings
            (shards, replicas, etc.).
        :param kwargs: Additional index creation parameters to pass to the
            Elasticsearch client.
        :return: Response dictionary from index creation, or *None* if
            index already exists.
        :raises AirflowException: If index creation fails due to critical errors
            other than "already exists".

        .. :no-index:
        """
        try:
            body: dict[str, Any] = {}
            if mappings:
                body["mappings"] = mappings
            if settings:
                body["settings"] = settings

            self.log.info("Creating index: %s", index_name)
            response = self.client.indices.create(index=index_name, body=body, **kwargs)
            return response.body if hasattr(response, "body") else dict(response)

        except Exception as e:
            if "already exists" in str(e).lower():
                self.log.warning("Index %s already exists", index_name)
                return None
            # Let other Elasticsearch exceptions pass through for proper error handling
            raise

    def delete_index(self, index_name: str, **kwargs: Any) -> dict[str, Any]:
        """
        Delete an Elasticsearch index.

        :param index_name: The name of the index to delete.
        :param kwargs: Additional deletion parameters to pass to the Elasticsearch client.
        :return: Response dictionary from the index deletion operation.

        .. :no-index:
        """
        self.log.info("Deleting index: %s", index_name)
        response = self.client.indices.delete(index=index_name, **kwargs)
        return response.body if hasattr(response, "body") else dict(response)

    def index_exists(self, index_name: str) -> bool:
        """
        Check if an Elasticsearch index exists.

        :param index_name: The name of the index to check for existence.
        :return: *True* if the index exists, *False* otherwise.

        .. :no-index:
        """
        response = self.client.indices.exists(index=index_name)
        return bool(response)

    def close(self) -> None:
        """
        Close the Elasticsearch client connection and clean up resources.

        This method safely closes the transport connection and removes the cached
        client instance to ensure proper cleanup and prevent connection leaks.

        .. :no-index:
        """
        if hasattr(self, "client") and self.client is not None:
            try:
                self.client.transport.close()
                self.log.info("Elasticsearch client connection closed")
            except Exception as e:
                self.log.warning("Error closing Elasticsearch client: %s", e)
            finally:
                # Remove cached client to force recreation on next access
                if "client" in self.__dict__:
                    del self.__dict__["client"]

    def __enter__(self) -> ElasticsearchHook:
        """
        Context manager entry point.

        :return: The ElasticsearchHook instance for use in context management.
        """
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any | None
    ) -> None:
        """
        Context manager exit point.

        Automatically closes the Elasticsearch connection when exiting the context.

        :param exc_type: Exception type if an exception occurred, *None* otherwise.
        :param exc_val: Exception value if an exception occurred, *None* otherwise.
        :param exc_tb: Exception traceback if an exception occurred, *None* otherwise.
        """
        self.close()
