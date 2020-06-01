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

from marshmallow import post_dump, post_load
from marshmallow_sqlalchemy import SQLAlchemySchema

from airflow.exceptions import AirflowException


class BaseSchema(SQLAlchemySchema):
    """ Base Schema for models """
    __envelope__ = {"many": None}

    def get_envelope_key(self, many):
        """Helper to get the envelope key. Key can be None

        :param many: String used to return deserialized result for
            a collection.
        :type many: str
        """
        if many is None:
            raise AirflowException("You must add the 'many' envelope key to your schema")
        key = self.__envelope__.get('many', None)
        return key

    @post_dump(pass_many=True)
    def wrap_with_envelope(self, data, many, **kwargs):
        """
        Checks if data is a list and use the envelope key to return it together
        with total_entries meta
        :param data: The deserialized data
        :param many: The envelope key to use in returning the data
        """
        key = self.get_envelope_key(many)
        if isinstance(data, list):
            return {key: data, 'total_entries': len(data)}
        return data
