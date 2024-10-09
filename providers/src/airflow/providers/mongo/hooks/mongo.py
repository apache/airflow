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
"""Hook for Mongo DB."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Iterable, overload
from urllib.parse import quote_plus, urlunsplit

import pymongo
from pymongo import MongoClient, ReplaceOne

from airflow.exceptions import AirflowConfigException, AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from types import TracebackType

    from pymongo.collection import Collection as MongoCollection
    from pymongo.command_cursor import CommandCursor
    from typing_extensions import Literal

    from airflow.models import Connection


class MongoHook(BaseHook):
    """
    PyMongo wrapper to interact with MongoDB.

    Mongo Connection Documentation
    https://docs.mongodb.com/manual/reference/connection-string/index.html
    You can specify connection string options in extra field of your connection
    https://docs.mongodb.com/manual/reference/connection-string/index.html#connection-string-options

    If you want use DNS seedlist, set `srv` to True.

    ex.
        {"srv": true, "replicaSet": "test", "ssl": true, "connectTimeoutMS": 30000}

    For enabling SSL, the `"ssl": true` option can be used within the connection string options, under extra.
    In scenarios where SSL is enabled, `allow_insecure` option is not included by default in the connection
    unless specified. This is so that we ensure a secure medium while handling connections to MongoDB.

    The `allow_insecure` only makes sense in ssl context and is configurable and can be used in one of
    the following scenarios:

    HTTP (ssl = False)
    Here, `ssl` is disabled and using `allow_insecure` doesn't make sense.
    Example connection extra: {"ssl": false}

    HTTPS, but insecure (ssl = True, allow_insecure = True)
    Here, `ssl` is enabled, and the connection allows insecure connections.
    Example connection extra: {"ssl": true, "allow_insecure": true}

    HTTPS, but secure (ssl = True, allow_insecure = False - default when SSL enabled):
    Here, `ssl` is enabled, and the connection does not allow insecure connections (default behavior when
    SSL is enabled). Example connection extra: {"ssl": true} or {"ssl": true, "allow_insecure": false}

    Note: `tls` is an alias to `ssl` and can be used in place of `ssl`. Example: {"ssl": false} or
    {"tls": false}.

    :param mongo_conn_id: The :ref:`Mongo connection id <howto/connection:mongo>` to use
        when connecting to MongoDB.
    """

    conn_name_attr = "mongo_conn_id"
    default_conn_name = "mongo_default"
    conn_type = "mongo"
    hook_name = "MongoDB"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_babel import lazy_gettext
        from wtforms import BooleanField

        return {
            "srv": BooleanField(
                label=lazy_gettext("SRV Connection"),
                description="Check if using an SRV/seed list connection, i.e. one that begins with 'mongdb+srv://' (if so, the port field should be left empty)",
            ),
            "ssl": BooleanField(
                label=lazy_gettext("Use SSL"), description="Check to enable SSL/TLS for the connection"
            ),
            "allow_insecure": BooleanField(
                label=lazy_gettext("Allow Invalid Certificates"),
                description="Check to bypass verification of certificates during SSL/TLS connections (has no effect for non-SSL/TLS connections)",
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": [],
            "relabeling": {"login": "Username", "schema": "Default DB"},
            "placeholders": {
                "port": "Note: port should not be set for SRV connections",
            },
        }

    def __init__(self, mongo_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__()
        if conn_id := kwargs.pop("conn_id", None):
            warnings.warn(
                "Parameter `conn_id` is deprecated and will be removed in a future releases. "
                "Please use `mongo_conn_id` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            mongo_conn_id = conn_id

        self.mongo_conn_id = mongo_conn_id

        conn = self.get_connection(self.mongo_conn_id)
        self._validate_connection(conn)
        self.connection = conn

        self.extras = self.connection.extra_dejson.copy()
        self.client: MongoClient | None = None
        self.uri = self._create_uri()

        self.allow_insecure = self.extras.pop("allow_insecure", "false").lower() == "true"
        self.ssl_enabled = (
            self.extras.get("ssl", "false").lower() == "true"
            or self.extras.get("tls", "false").lower() == "true"
        )

        if self.ssl_enabled and not self.allow_insecure:
            # Case: HTTPS
            self.allow_insecure = False
        elif self.ssl_enabled and self.allow_insecure:
            # Case: HTTPS + allow_insecure
            self.allow_insecure = True
            self.extras.pop("ssl", None)
        elif not self.ssl_enabled and "allow_insecure" in self.extras:
            # Case: HTTP (ssl=False) with allow_insecure specified
            self.log.warning("allow_insecure is only applicable when ssl is set")
            self.extras.pop("allow_insecure", None)
        elif not self.ssl_enabled:
            # Case: HTTP (ssl=False) with allow_insecure not specified
            self.allow_insecure = False

    @staticmethod
    def _validate_connection(conn: Connection):
        conn_type = conn.conn_type
        if conn_type != "mongo":
            if conn_type == "mongodb+srv":
                raise AirflowConfigException(
                    "Mongo SRV connections should have the conn_type 'mongo' and set 'use_srv=true' in extras"
                )
            raise AirflowConfigException(
                f"conn_type '{conn_type}' not allowed for MongoHook; conn_type must be 'mongo'"
            )

        if conn.port and conn.extra_dejson.get("srv"):
            raise AirflowConfigException("srv URI should not specify a port")

    def __enter__(self):
        """Return the object when a context manager is created."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close mongo connection when exiting the context manager."""
        if self.client is not None:
            self.client.close()
            self.client = None

    def get_conn(self) -> MongoClient:
        """Fetch PyMongo Client."""
        if self.client is not None:
            return self.client

        # Mongo Connection Options dict that is unpacked when passed to MongoClient
        options = self.extras

        # Set tlsAllowInvalidCertificates based on allow_insecure
        if self.allow_insecure:
            options["tlsAllowInvalidCertificates"] = True

        self.client = MongoClient(self.uri, **options)
        return self.client

    def _create_uri(self) -> str:
        """
        Create URI string from the given credentials.

        :return: URI string.
        """
        srv = self.extras.pop("srv", False)
        scheme = "mongodb+srv" if srv else "mongodb"
        login = self.connection.login
        password = self.connection.password
        netloc = self.connection.host
        if login is not None and password is not None:
            netloc = f"{quote_plus(login)}:{quote_plus(password)}@{netloc}"
        if self.connection.port:
            netloc = f"{netloc}:{self.connection.port}"
        path = f"/{self.connection.schema}"
        return urlunsplit((scheme, netloc, path, "", ""))

    def get_collection(self, mongo_collection: str, mongo_db: str | None = None) -> MongoCollection:
        """
        Fetch a mongo collection object for querying.

        Uses connection schema as DB unless specified.
        """
        mongo_db = mongo_db or self.connection.schema
        mongo_conn: MongoClient = self.get_conn()

        return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)

    def aggregate(
        self, mongo_collection: str, aggregate_query: list, mongo_db: str | None = None, **kwargs
    ) -> CommandCursor:
        """
        Run an aggregation pipeline and returns the results.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
        https://pymongo.readthedocs.io/en/stable/examples/aggregation.html
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.aggregate(aggregate_query, **kwargs)

    @overload
    def find(
        self,
        mongo_collection: str,
        query: dict,
        find_one: Literal[False],
        mongo_db: str | None = None,
        projection: list | dict | None = None,
        **kwargs,
    ) -> pymongo.cursor.Cursor: ...

    @overload
    def find(
        self,
        mongo_collection: str,
        query: dict,
        find_one: Literal[True],
        mongo_db: str | None = None,
        projection: list | dict | None = None,
        **kwargs,
    ) -> Any | None: ...

    def find(
        self,
        mongo_collection: str,
        query: dict,
        find_one: bool = False,
        mongo_db: str | None = None,
        projection: list | dict | None = None,
        **kwargs,
    ) -> pymongo.cursor.Cursor | Any | None:
        """
        Run a mongo find query and returns the results.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.find
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if find_one:
            return collection.find_one(query, projection, **kwargs)
        else:
            return collection.find(query, projection, **kwargs)

    def insert_one(
        self, mongo_collection: str, doc: dict, mongo_db: str | None = None, **kwargs
    ) -> pymongo.results.InsertOneResult:
        """
        Insert a single document into a mongo collection.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.insert_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_one(doc, **kwargs)

    def insert_many(
        self, mongo_collection: str, docs: Iterable[dict], mongo_db: str | None = None, **kwargs
    ) -> pymongo.results.InsertManyResult:
        """
        Insert many docs into a mongo collection.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.insert_many
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_many(docs, **kwargs)

    def update_one(
        self,
        mongo_collection: str,
        filter_doc: dict,
        update_doc: dict,
        mongo_db: str | None = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Update a single document in a mongo collection.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.update_one

        :param mongo_collection: The name of the collection to update.
        :param filter_doc: A query that matches the documents to update.
        :param update_doc: The modifications to apply.
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.

        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.update_one(filter_doc, update_doc, **kwargs)

    def update_many(
        self,
        mongo_collection: str,
        filter_doc: dict,
        update_doc: dict,
        mongo_db: str | None = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Update one or more documents in a mongo collection.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.update_many

        :param mongo_collection: The name of the collection to update.
        :param filter_doc: A query that matches the documents to update.
        :param update_doc: The modifications to apply.
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.update_many(filter_doc, update_doc, **kwargs)

    def replace_one(
        self,
        mongo_collection: str,
        doc: dict,
        filter_doc: dict | None = None,
        mongo_db: str | None = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Replace a single document in a mongo collection.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.replace_one

        .. note::
            If no ``filter_doc`` is given, it is assumed that the replacement
            document contain the ``_id`` field which is then used as filters.

        :param mongo_collection: The name of the collection to update.
        :param doc: The new document.
        :param filter_doc: A query that matches the documents to replace.
            Can be omitted; then the _id field from doc will be used.
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if not filter_doc:
            filter_doc = {"_id": doc["_id"]}

        return collection.replace_one(filter_doc, doc, **kwargs)

    def replace_many(
        self,
        mongo_collection: str,
        docs: list[dict],
        filter_docs: list[dict] | None = None,
        mongo_db: str | None = None,
        upsert: bool = False,
        collation: pymongo.collation.Collation | None = None,
        **kwargs,
    ) -> pymongo.results.BulkWriteResult:
        """
        Replace many documents in a mongo collection.

        Uses bulk_write with multiple ReplaceOne operations
        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.bulk_write

        .. note::
            If no ``filter_docs``are given, it is assumed that all
            replacement documents contain the ``_id`` field which are then
            used as filters.

        :param mongo_collection: The name of the collection to update.
        :param docs: The new documents.
        :param filter_docs: A list of queries that match the documents to replace.
            Can be omitted; then the _id fields from airflow.docs will be used.
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        :param upsert: If ``True``, perform an insert if no documents
            match the filters for the replace operation.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`. This option is only
            supported on MongoDB 3.4 and above.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if not filter_docs:
            filter_docs = [{"_id": doc["_id"]} for doc in docs]

        requests = [
            ReplaceOne(filter_docs[i], docs[i], upsert=upsert, collation=collation) for i in range(len(docs))
        ]

        return collection.bulk_write(requests, **kwargs)

    def delete_one(
        self, mongo_collection: str, filter_doc: dict, mongo_db: str | None = None, **kwargs
    ) -> pymongo.results.DeleteResult:
        """
        Delete a single document in a mongo collection.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.delete_one

        :param mongo_collection: The name of the collection to delete from.
        :param filter_doc: A query that matches the document to delete.
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.delete_one(filter_doc, **kwargs)

    def delete_many(
        self, mongo_collection: str, filter_doc: dict, mongo_db: str | None = None, **kwargs
    ) -> pymongo.results.DeleteResult:
        """
        Delete one or more documents in a mongo collection.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.delete_many

        :param mongo_collection: The name of the collection to delete from.
        :param filter_doc: A query that matches the documents to delete.
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.delete_many(filter_doc, **kwargs)

    def distinct(
        self,
        mongo_collection: str,
        distinct_key: str,
        filter_doc: dict | None = None,
        mongo_db: str | None = None,
        **kwargs,
    ) -> list[Any]:
        """
        Return a list of distinct values for the given key across a collection.

        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.distinct

        :param mongo_collection: The name of the collection to perform distinct on.
        :param distinct_key: The field to return distinct values from.
        :param filter_doc: A query that matches the documents get distinct values from.
            Can be omitted; then will cover the entire collection.
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.distinct(distinct_key, filter=filter_doc, **kwargs)
