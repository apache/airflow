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
"""This module is deprecated. Please use `airflow.providers.google.cloud.operators.spanner`."""

import warnings

from airflow.providers.google.cloud.operators.spanner import (
    SpannerDeleteDatabaseInstanceOperator,
    SpannerDeleteInstanceOperator,
    SpannerDeployDatabaseInstanceOperator,
    SpannerDeployInstanceOperator,
    SpannerQueryDatabaseInstanceOperator,
    SpannerUpdateDatabaseInstanceOperator,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.spanner`.",
    DeprecationWarning,
    stacklevel=2,
)


class CloudSpannerInstanceDatabaseDeleteOperator(SpannerDeleteDatabaseInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            self.__doc__,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudSpannerInstanceDatabaseDeployOperator(SpannerDeployDatabaseInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.spanner.SpannerDeployDatabaseInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            self.__doc__,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudSpannerInstanceDatabaseQueryOperator(SpannerQueryDatabaseInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            self.__doc__,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudSpannerInstanceDatabaseUpdateOperator(SpannerUpdateDatabaseInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.spanner.SpannerUpdateDatabaseInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            self.__doc__,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudSpannerInstanceDeleteOperator(SpannerDeleteInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            self.__doc__,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudSpannerInstanceDeployOperator(SpannerDeployInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.spanner.SpannerDeployInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            self.__doc__,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
