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
This module is deprecated. Please use `airflow.providers.google.cloud.operators.bigtable`
or `airflow.providers.google.cloud.sensors.bigtable`.
"""

import warnings

from airflow.providers.google.cloud.operators.bigtable import (
    BigtableCreateInstanceOperator,
    BigtableCreateTableOperator,
    BigtableDeleteInstanceOperator,
    BigtableDeleteTableOperator,
    BigtableUpdateClusterOperator,
)
from airflow.providers.google.cloud.sensors.bigtable import BigtableTableReplicationCompletedSensor

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.bigtable`"
    " or `airflow.providers.google.cloud.sensors.bigtable`.",
    DeprecationWarning,
    stacklevel=2,
)


class BigtableClusterUpdateOperator(BigtableUpdateClusterOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class BigtableInstanceCreateOperator(BigtableCreateInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class BigtableInstanceDeleteOperator(BigtableDeleteInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class BigtableTableCreateOperator(BigtableCreateTableOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.bigtable.BigtableCreateTableOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.bigtable.BigtableCreateTableOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class BigtableTableDeleteOperator(BigtableDeleteTableOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


# pylint: disable=too-many-ancestors
class BigtableTableWaitForReplicationSensor(BigtableTableReplicationCompletedSensor):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.sensors.bigtable.BigtableTableReplicationCompletedSensor`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.sensors.bigtable.BigtableTableReplicationCompletedSensor`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
