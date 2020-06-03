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
from typing import List, Optional

from marshmallow import post_dump
from marshmallow_sqlalchemy import SQLAlchemySchema

from airflow.exceptions import AirflowException


class BaseSchema(SQLAlchemySchema):

    """ Base Schema for sqlalchemy models

    :param COLLECTION_NAME: A name to use to return serialized data if the data is a list
    :type COLLECTION_NAME: str

    :param FIELDS_FROM_NONE_TO_EMPTY_STRING: A list of fields to convert to empty string if value is None
        after serialization
    :type FIELDS_FROM_NONE_TO_EMPTY_STRING: List[str]

    :param FIELDS_FROM_NONE_TO_ZERO: A list of fields to convert to integer zero if value is None
        after serialization
    :type FIELDS_FROM_NONE_TO_ZERO: List[str]
    """

    COLLECTION_NAME: Optional[str] = None
    FIELDS_FROM_NONE_TO_EMPTY_STRING: List[str] = []
    FIELDS_FROM_NONE_TO_ZERO: List[str] = []

    def check_collection_name(self):
        """
        Method to check that COLLLECTION_NAME attribute is not None
        """
        if not self.COLLECTION_NAME:
            raise AirflowException("The COLLECTION_NAME attribute is missing in the schema")

    @post_dump(pass_many=True)
    def wrap_with_envelope(self, data, many, **kwargs):
        """
        Checks if data is a list and use the envelope key to return it together
        with total_entries meta
        :param data: The deserialized data
        :param many: Whether the data is a collection
        """
        self.check_collection_name()
        if many:
            data = self._process_list_data(data)
            return {self.COLLECTION_NAME: data, 'total_entries': len(data)}
        data = self._process_data(data)
        return data

    def _process_list_data(self, data):
        return [self._process_data(item) for item in data]

    def _process_data(self, data):
        d_data = {}
        for k, v in data.items():
            if v is None:
                if k in self.FIELDS_FROM_NONE_TO_EMPTY_STRING:
                    v = ''
                elif k in self.FIELDS_FROM_NONE_TO_ZERO:
                    v = 0
            d_data[k] = v
        return d_data
