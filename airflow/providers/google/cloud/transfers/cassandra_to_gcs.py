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

import json
import warnings
from base64 import b64encode
from datetime import datetime
from decimal import Decimal
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from uuid import UUID

from cassandra.util import Date, OrderedMapSerializedKey, SortedSet, Time

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults


class CassandraToGCSOperator(BaseOperator):
    """
    Copy data from Cassandra to Google Cloud Storage in JSON format

    Note: Arrays of arrays are not supported.

    :param cql: The CQL to execute on the Cassandra table.
    :type cql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MySQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files (see notes in the
        filename param docs above). This param allows developers to specify the
        file size of the splits. Check https://cloud.google.com/storage/quotas
        to see the maximum allowed file size for a single object.
    :type approx_max_file_size_bytes: long
    :param cassandra_conn_id: Reference to a specific Cassandra hook.
    :type cassandra_conn_id: str
    :param gzip: Option to compress file for upload
    :type gzip: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('cql', 'bucket', 'filename', 'schema_filename',)
    template_ext = ('.cql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 cql: str,
                 bucket: str,
                 filename: str,
                 schema_filename: Optional[str] = None,
                 approx_max_file_size_bytes: int = 1900000000,
                 gzip: bool = False,
                 cassandra_conn_id: str = 'cassandra_default',
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.cql = cql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.cassandra_conn_id = cassandra_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.gzip = gzip

    # Default Cassandra to BigQuery type mapping
    CQL_TYPE_MAP = {
        'BytesType': 'BYTES',
        'DecimalType': 'FLOAT',
        'UUIDType': 'BYTES',
        'BooleanType': 'BOOL',
        'ByteType': 'INTEGER',
        'AsciiType': 'STRING',
        'FloatType': 'FLOAT',
        'DoubleType': 'FLOAT',
        'LongType': 'INTEGER',
        'Int32Type': 'INTEGER',
        'IntegerType': 'INTEGER',
        'InetAddressType': 'STRING',
        'CounterColumnType': 'INTEGER',
        'DateType': 'TIMESTAMP',
        'SimpleDateType': 'DATE',
        'TimestampType': 'TIMESTAMP',
        'TimeUUIDType': 'BYTES',
        'ShortType': 'INTEGER',
        'TimeType': 'TIME',
        'DurationType': 'INTEGER',
        'UTF8Type': 'STRING',
        'VarcharType': 'STRING',
    }

    def execute(self, context: Dict[str, str]):
        hook = CassandraHook(cassandra_conn_id=self.cassandra_conn_id)
        cursor = hook.get_conn().execute(self.cql)

        files_to_upload = self._write_local_data_files(cursor)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            files_to_upload.update(self._write_local_schema_file(cursor))

        # Flush all files before uploading
        for file_handle in files_to_upload.values():
            file_handle.flush()

        self._upload_to_gcs(files_to_upload)

        # Close all temp file handles.
        for file_handle in files_to_upload.values():
            file_handle.close()

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
        tmp_file_handles = {self.filename.format(file_no): tmp_file_handle}
        for row in cursor:
            row_dict = self.generate_data_dict(row._fields, row)
            content = json.dumps(row_dict).encode('utf-8')
            tmp_file_handle.write(content)

            # Append newline to make dumps BigQuery compatible.
            tmp_file_handle.write(b'\n')

            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                tmp_file_handles[self.filename.format(file_no)] = tmp_file_handle

        return tmp_file_handles

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
        json_serialized_schema = json.dumps(schema).encode('utf-8')

        tmp_schema_file_handle.write(json_serialized_schema)
        return {self.schema_filename: tmp_schema_file_handle}

    def _upload_to_gcs(self, files_to_upload: Dict[str, Any]):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)
        for obj, tmp_file_handle in files_to_upload.items():
            hook.upload(
                bucket_name=self.bucket,
                object_name=obj,
                filename=tmp_file_handle.name,
                mime_type='application/json',
                gzip=self.gzip
            )

    @classmethod
    def generate_data_dict(cls, names: Iterable[str], values: Any) -> Dict[str, Any]:
        """
        Generates data structure that will be stored as file in GCS.
        """
        return {n: cls.convert_value(v) for n, v in zip(names, values)}

    @classmethod
    def convert_value(  # pylint: disable=too-many-return-statements
        cls,
        value: Optional[Any]
    ) -> Optional[Any]:
        """
        Convert value to BQ type.
        """
        if not value:
            return value
        elif isinstance(value, (str, int, float, bool, dict)):
            return value
        elif isinstance(value, bytes):
            return b64encode(value).decode('ascii')
        elif isinstance(value, UUID):
            return b64encode(value.bytes).decode('ascii')
        elif isinstance(value, (datetime, Date)):
            return str(value)
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, Time):
            return str(value).split('.')[0]
        elif isinstance(value, (list, SortedSet)):
            return cls.convert_array_types(value)
        elif hasattr(value, '_fields'):
            return cls.convert_user_type(value)
        elif isinstance(value, tuple):
            return cls.convert_tuple_type(value)
        elif isinstance(value, OrderedMapSerializedKey):
            return cls.convert_map_type(value)
        else:
            raise AirflowException('Unexpected value: ' + str(value))

    @classmethod
    def convert_array_types(cls, value: Union[List[Any], SortedSet]) -> List[Any]:
        """
        Maps convert_value over array.
        """
        return [cls.convert_value(nested_value) for nested_value in value]

    @classmethod
    def convert_user_type(cls, value: Any) -> Dict[str, Any]:
        """
        Converts a user type to RECORD that contains n fields, where n is the
        number of attributes. Each element in the user type class will be converted to its
        corresponding data type in BQ.
        """
        names = value._fields
        values = [cls.convert_value(getattr(value, name)) for name in names]
        return cls.generate_data_dict(names, values)

    @classmethod
    def convert_tuple_type(cls, values: Tuple[Any]) -> Dict[str, Any]:
        """
        Converts a tuple to RECORD that contains n fields, each will be converted
        to its corresponding data type in bq and will be named 'field_<index>', where
        index is determined by the order of the tuple elements defined in cassandra.
        """
        names = ['field_' + str(i) for i in range(len(values))]
        return cls.generate_data_dict(names, values)

    @classmethod
    def convert_map_type(cls, value: OrderedMapSerializedKey) -> List[Dict[str, Any]]:
        """
        Converts a map to a repeated RECORD that contains two fields: 'key' and 'value',
        each will be converted to its corresponding data type in BQ.
        """
        converted_map = []
        for k, v in zip(value.keys(), value.values()):
            converted_map.append({
                'key': cls.convert_value(k),
                'value': cls.convert_value(v)
            })
        return converted_map

    @classmethod
    def generate_schema_dict(cls, name: str, type_: Any) -> Dict[str, Any]:
        """
        Generates BQ schema.
        """
        field_schema: Dict[str, Any] = {}
        field_schema.update({'name': name})
        field_schema.update({'type_': cls.get_bq_type(type_)})
        field_schema.update({'mode': cls.get_bq_mode(type_)})
        fields = cls.get_bq_fields(type_)
        if fields:
            field_schema.update({'fields': fields})
        return field_schema

    @classmethod
    def get_bq_fields(cls, type_: Any) -> List[Dict[str, Any]]:
        """
        Converts non simple type value to BQ representation.
        """
        if cls.is_simple_type(type_):
            return []

        # In case of not simple type
        names: List[str] = []
        types: List[Any] = []
        if cls.is_array_type(type_) and cls.is_record_type(type_.subtypes[0]):
            names = type_.subtypes[0].fieldnames
            types = type_.subtypes[0].subtypes
        elif cls.is_record_type(type_):
            names = type_.fieldnames
            types = type_.subtypes

        if types and not names and type_.cassname == 'TupleType':
            names = ['field_' + str(i) for i in range(len(types))]
        elif types and not names and type_.cassname == 'MapType':
            names = ['key', 'value']

        return [cls.generate_schema_dict(n, t) for n, t in zip(names, types)]

    @staticmethod
    def is_simple_type(type_: Any) -> bool:
        """
        Check if type is a simple type.
        """
        return type_.cassname in CassandraToGCSOperator.CQL_TYPE_MAP

    @staticmethod
    def is_array_type(type_: Any) -> bool:
        """
        Check if type is an array type.
        """
        return type_.cassname in ['ListType', 'SetType']

    @staticmethod
    def is_record_type(type_: Any) -> bool:
        """
        Checks the record type.
        """
        return type_.cassname in ['UserType', 'TupleType', 'MapType']

    @classmethod
    def get_bq_type(cls, type_: Any) -> str:
        """
        Converts type to equivalent BQ type.
        """
        if cls.is_simple_type(type_):
            return CassandraToGCSOperator.CQL_TYPE_MAP[type_.cassname]
        elif cls.is_record_type(type_):
            return 'RECORD'
        elif cls.is_array_type(type_):
            return cls.get_bq_type(type_.subtypes[0])
        else:
            raise AirflowException('Not a supported type_: ' + type_.cassname)

    @classmethod
    def get_bq_mode(cls, type_: Any) -> str:
        """
        Converts type to equivalent BQ mode.
        """
        if cls.is_array_type(type_) or type_.cassname == 'MapType':
            return 'REPEATED'
        elif cls.is_record_type(type_) or cls.is_simple_type(type_):
            return 'NULLABLE'
        else:
            raise AirflowException('Not a supported type_: ' + type_.cassname)
