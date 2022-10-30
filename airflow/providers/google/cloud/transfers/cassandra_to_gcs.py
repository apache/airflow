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
"""
This module contains operator for copying
data from Cassandra to Google Cloud Storage in JSON format.
"""
from __future__ import annotations

import json
from base64 import b64encode
from datetime import datetime
from decimal import Decimal
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, Iterable, NewType, Sequence
from uuid import UUID

from cassandra.util import Date, OrderedMapSerializedKey, SortedSet, Time

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

NotSetType = NewType("NotSetType", object)
NOT_SET = NotSetType(object())


class CassandraToGCSOperator(BaseOperator):
    """
    Copy data from Cassandra to Google Cloud Storage in JSON format

    Note: Arrays of arrays are not supported.

    :param cql: The CQL to execute on the Cassandra table.
    :param bucket: The bucket to upload to.
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size.
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MySQL.
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files (see notes in the
        filename param docs above). This param allows developers to specify the
        file size of the splits. Check https://cloud.google.com/storage/quotas
        to see the maximum allowed file size for a single object.
    :param cassandra_conn_id: Reference to a specific Cassandra hook.
    :param gzip: Option to compress file for upload
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param query_timeout: (Optional) The amount of time, in seconds, used to execute the Cassandra query.
        If not set, the timeout value will be set in Session.execute() by Cassandra driver.
        If set to None, there is no timeout.
    :param encode_uuid: (Optional) Option to encode UUID or not when upload from Cassandra to GCS.
        Default is to encode UUID.
    """

    template_fields: Sequence[str] = (
        "cql",
        "bucket",
        "filename",
        "schema_filename",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".cql",)
    ui_color = "#a0e08c"

    def __init__(
        self,
        *,
        cql: str,
        bucket: str,
        filename: str,
        schema_filename: str | None = None,
        approx_max_file_size_bytes: int = 1900000000,
        gzip: bool = False,
        cassandra_conn_id: str = "cassandra_default",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        query_timeout: float | None | NotSetType = NOT_SET,
        encode_uuid: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.cql = cql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.cassandra_conn_id = cassandra_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.impersonation_chain = impersonation_chain
        self.query_timeout = query_timeout
        self.encode_uuid = encode_uuid

    # Default Cassandra to BigQuery type mapping
    CQL_TYPE_MAP = {
        "BytesType": "STRING",
        "DecimalType": "FLOAT",
        "UUIDType": "STRING",
        "BooleanType": "BOOL",
        "ByteType": "INTEGER",
        "AsciiType": "STRING",
        "FloatType": "FLOAT",
        "DoubleType": "FLOAT",
        "LongType": "INTEGER",
        "Int32Type": "INTEGER",
        "IntegerType": "INTEGER",
        "InetAddressType": "STRING",
        "CounterColumnType": "INTEGER",
        "DateType": "TIMESTAMP",
        "SimpleDateType": "DATE",
        "TimestampType": "TIMESTAMP",
        "TimeUUIDType": "STRING",
        "ShortType": "INTEGER",
        "TimeType": "TIME",
        "DurationType": "INTEGER",
        "UTF8Type": "STRING",
        "VarcharType": "STRING",
    }

    def execute(self, context: Context):
        hook = CassandraHook(cassandra_conn_id=self.cassandra_conn_id)

        query_extra = {}
        if self.query_timeout is not NOT_SET:
            query_extra["timeout"] = self.query_timeout

        cursor = hook.get_conn().execute(self.cql, **query_extra)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            self.log.info("Writing local schema file")
            schema_file = self._write_local_schema_file(cursor)

            # Flush file before uploading
            schema_file["file_handle"].flush()

            self.log.info("Uploading schema file to GCS.")
            self._upload_to_gcs(schema_file)
            schema_file["file_handle"].close()

        counter = 0
        self.log.info("Writing local data files")
        for file_to_upload in self._write_local_data_files(cursor):
            # Flush file before uploading
            file_to_upload["file_handle"].flush()

            self.log.info("Uploading chunk file #%d to GCS.", counter)
            self._upload_to_gcs(file_to_upload)

            self.log.info("Removing local file")
            file_to_upload["file_handle"].close()
            counter += 1

        # Close all sessions and connection associated with this Cassandra cluster
        hook.shutdown_cluster()

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        file_no = 0

        tmp_file_handle = NamedTemporaryFile(delete=True)
        file_to_upload = {
            "file_name": self.filename.format(file_no),
            "file_handle": tmp_file_handle,
        }
        for row in cursor:
            row_dict = self.generate_data_dict(row._fields, row)
            content = json.dumps(row_dict).encode("utf-8")
            tmp_file_handle.write(content)

            # Append newline to make dumps BigQuery compatible.
            tmp_file_handle.write(b"\n")

            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1

                yield file_to_upload
                tmp_file_handle = NamedTemporaryFile(delete=True)
                file_to_upload = {
                    "file_name": self.filename.format(file_no),
                    "file_handle": tmp_file_handle,
                }
        yield file_to_upload

    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system.

        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema = []
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)

        for name, type_ in zip(cursor.column_names, cursor.column_types):
            schema.append(self.generate_schema_dict(name, type_))
        json_serialized_schema = json.dumps(schema).encode("utf-8")

        tmp_schema_file_handle.write(json_serialized_schema)
        schema_file_to_upload = {
            "file_name": self.schema_filename,
            "file_handle": tmp_schema_file_handle,
        }
        return schema_file_to_upload

    def _upload_to_gcs(self, file_to_upload):
        """Upload a file (data split or schema .json file) to Google Cloud Storage."""
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        hook.upload(
            bucket_name=self.bucket,
            object_name=file_to_upload.get("file_name"),
            filename=file_to_upload.get("file_handle").name,
            mime_type="application/json",
            gzip=self.gzip,
        )

    def generate_data_dict(self, names: Iterable[str], values: Any) -> dict[str, Any]:
        """Generates data structure that will be stored as file in GCS."""
        return {n: self.convert_value(v) for n, v in zip(names, values)}

    def convert_value(self, value: Any | None) -> Any | None:
        """Convert value to BQ type."""
        if not value:
            return value
        elif isinstance(value, (str, int, float, bool, dict)):
            return value
        elif isinstance(value, bytes):
            return b64encode(value).decode("ascii")
        elif isinstance(value, UUID):
            if self.encode_uuid:
                return b64encode(value.bytes).decode("ascii")
            else:
                return str(value)
        elif isinstance(value, (datetime, Date)):
            return str(value)
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, Time):
            return str(value).split(".")[0]
        elif isinstance(value, (list, SortedSet)):
            return self.convert_array_types(value)
        elif hasattr(value, "_fields"):
            return self.convert_user_type(value)
        elif isinstance(value, tuple):
            return self.convert_tuple_type(value)
        elif isinstance(value, OrderedMapSerializedKey):
            return self.convert_map_type(value)
        else:
            raise AirflowException("Unexpected value: " + str(value))

    def convert_array_types(self, value: list[Any] | SortedSet) -> list[Any]:
        """Maps convert_value over array."""
        return [self.convert_value(nested_value) for nested_value in value]

    def convert_user_type(self, value: Any) -> dict[str, Any]:
        """
        Converts a user type to RECORD that contains n fields, where n is the
        number of attributes. Each element in the user type class will be converted to its
        corresponding data type in BQ.
        """
        names = value._fields
        values = [self.convert_value(getattr(value, name)) for name in names]
        return self.generate_data_dict(names, values)

    def convert_tuple_type(self, values: tuple[Any]) -> dict[str, Any]:
        """
        Converts a tuple to RECORD that contains n fields, each will be converted
        to its corresponding data type in bq and will be named 'field_<index>', where
        index is determined by the order of the tuple elements defined in cassandra.
        """
        names = ["field_" + str(i) for i in range(len(values))]
        return self.generate_data_dict(names, values)

    def convert_map_type(self, value: OrderedMapSerializedKey) -> list[dict[str, Any]]:
        """
        Converts a map to a repeated RECORD that contains two fields: 'key' and 'value',
        each will be converted to its corresponding data type in BQ.
        """
        converted_map = []
        for k, v in zip(value.keys(), value.values()):
            converted_map.append({"key": self.convert_value(k), "value": self.convert_value(v)})
        return converted_map

    @classmethod
    def generate_schema_dict(cls, name: str, type_: Any) -> dict[str, Any]:
        """Generates BQ schema."""
        field_schema: dict[str, Any] = {}
        field_schema.update({"name": name})
        field_schema.update({"type_": cls.get_bq_type(type_)})
        field_schema.update({"mode": cls.get_bq_mode(type_)})
        fields = cls.get_bq_fields(type_)
        if fields:
            field_schema.update({"fields": fields})
        return field_schema

    @classmethod
    def get_bq_fields(cls, type_: Any) -> list[dict[str, Any]]:
        """Converts non simple type value to BQ representation."""
        if cls.is_simple_type(type_):
            return []

        # In case of not simple type
        names: list[str] = []
        types: list[Any] = []
        if cls.is_array_type(type_) and cls.is_record_type(type_.subtypes[0]):
            names = type_.subtypes[0].fieldnames
            types = type_.subtypes[0].subtypes
        elif cls.is_record_type(type_):
            names = type_.fieldnames
            types = type_.subtypes

        if types and not names and type_.cassname == "TupleType":
            names = ["field_" + str(i) for i in range(len(types))]
        elif types and not names and type_.cassname == "MapType":
            names = ["key", "value"]

        return [cls.generate_schema_dict(n, t) for n, t in zip(names, types)]

    @staticmethod
    def is_simple_type(type_: Any) -> bool:
        """Check if type is a simple type."""
        return type_.cassname in CassandraToGCSOperator.CQL_TYPE_MAP

    @staticmethod
    def is_array_type(type_: Any) -> bool:
        """Check if type is an array type."""
        return type_.cassname in ["ListType", "SetType"]

    @staticmethod
    def is_record_type(type_: Any) -> bool:
        """Checks the record type."""
        return type_.cassname in ["UserType", "TupleType", "MapType"]

    @classmethod
    def get_bq_type(cls, type_: Any) -> str:
        """Converts type to equivalent BQ type."""
        if cls.is_simple_type(type_):
            return CassandraToGCSOperator.CQL_TYPE_MAP[type_.cassname]
        elif cls.is_record_type(type_):
            return "RECORD"
        elif cls.is_array_type(type_):
            return cls.get_bq_type(type_.subtypes[0])
        else:
            raise AirflowException("Not a supported type_: " + type_.cassname)

    @classmethod
    def get_bq_mode(cls, type_: Any) -> str:
        """Converts type to equivalent BQ mode."""
        if cls.is_array_type(type_) or type_.cassname == "MapType":
            return "REPEATED"
        elif cls.is_record_type(type_) or cls.is_simple_type(type_):
            return "NULLABLE"
        else:
            raise AirflowException("Not a supported type_: " + type_.cassname)
