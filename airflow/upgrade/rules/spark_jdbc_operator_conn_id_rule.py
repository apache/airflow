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

from airflow.models import Connection
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils.db import provide_session


class SparkJDBCOperatorConnIdRule(BaseRule):
    title = "Check Spark JDBC Operator default connection name"

    description = """\
In Airflow 1.10.x, the default value for SparkJDBCOperator class 'conn_id' is 'spark-default'.
From Airflow 2.0, it has been changed to 'spark_default' to conform with the naming conventions
of all other connection names.
    """

    @provide_session
    def check(self, session=None):
        for conn in session.query(Connection.conn_id):
            if conn.conn_id == 'spark-default':
                return (
                    "Deprecation Warning: From Airflow 2.0, the default value of 'conn_id' argument of "
                    "SparkJDBCOperator class has been changed to 'spark_default' to conform with the naming "
                    "conventions of all other connection names. Please rename the connection with "
                    "id 'spark-default' to 'spark_default' or explicitly pass 'spark-default' "
                    "to the operator. See the link below for details: "
                    "https://github.com/apache/airflow/blob/2.0.0/"
                    "UPDATING.md#sparkjdbchook-default-connection"
                )
