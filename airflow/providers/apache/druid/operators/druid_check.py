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

import warnings

from airflow.providers.common.sql.operators.sql import SQLCheckOperator


class DruidCheckOperator(SQLCheckOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.common.sql.operators.sql.SQLCheckOperator`.
    """

    def __init__(self, druid_broker_conn_id: str = 'druid_broker_default', **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.common.sql.operators.sql.SQLCheckOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(conn_id=druid_broker_conn_id, **kwargs)
