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
"""This module is deprecated. Please use :mod:`airflow.operators.sql`."""

import warnings

from airflow.operators.sql import SQLCheckOperator, SQLIntervalCheckOperator, SQLValueCheckOperator

warnings.warn(
    "This module is deprecated. Please use `airflow.operators.sql`.", DeprecationWarning, stacklevel=2
)


class PrestoCheckOperator(SQLCheckOperator):
    """
    This class is deprecated.
    Please use `airflow.operators.sql.SQLCheckOperator`.
    """

    def __init__(self, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.operators.sql.SQLCheckOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)


class PrestoIntervalCheckOperator(SQLIntervalCheckOperator):
    """
    This class is deprecated.
    Please use `airflow.operators.sql.SQLIntervalCheckOperator`.
    """

    def __init__(self, **kwargs):
        warnings.warn(
            """
            This class is deprecated.l
            Please use `airflow.operators.sql.SQLIntervalCheckOperator`.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)


class PrestoValueCheckOperator(SQLValueCheckOperator):
    """
    This class is deprecated.
    Please use `airflow.operators.sql.SQLValueCheckOperator`.
    """

    def __init__(self, **kwargs):
        warnings.warn(
            """
            This class is deprecated.l
            Please use `airflow.operators.sql.SQLValueCheckOperator`.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)
