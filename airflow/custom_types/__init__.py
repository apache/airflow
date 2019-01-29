# -*- coding: utf-8 -*-
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

import json
import pickle

from sqlalchemy import types

from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin


class XComSerializer(types.TypeDecorator):
    """
    A custom SQLAlchemy "types.TypeDecorator" class that handles XCom
    serialization and deserialization when inserting/updating and reading the
    "values" column from the xcom table.
    """
    impl = types.LargeBinary

    def process_bind_param(self, value, dialect):
        """
        Serialize value before passing it to the database
        """
        enable_pickling = configuration.getboolean('core', 'enable_xcom_pickling')
        if enable_pickling:
            value = pickle.dumps(value)
        else:
            try:
                value = json.dumps(value).encode('UTF-8')
            except ValueError:
                log = LoggingMixin().log
                log.error("Could not serialize the XCOM value into JSON. "
                          "If you are using pickles instead of JSON "
                          "for XCOM, then you need to enable pickle "
                          "support for XCOM in your airflow config.")
                raise
        return value

    """
    TODO: "pickling" has been deprecated and JSON is preferred.
          "pickling" will be removed in Airflow 2.0.
    """
    def process_result_value(self, value, dialect):
        """
        Deserialize result value from database
        """
        enable_pickling = configuration.getboolean('core', 'enable_xcom_pickling')
        if enable_pickling:
            value = pickle.loads(value)
        else:
            try:
                value = json.loads(value.decode('UTF-8'))
            except (UnicodeEncodeError, ValueError):
                # For backward-compatibility.
                # Preventing errors in webserver
                # due to XComs mixed with pickled and unpickled.
                value = pickle.loads(value)
        return value
