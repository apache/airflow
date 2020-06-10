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
This module is deprecated.
Please use `airflow.providers.google.cloud.operators.cloud_storage_transfer_service`.
"""

import warnings

from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCancelOperationOperator, CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator, CloudDataTransferServiceGCSToGCSOperator,
    CloudDataTransferServiceGetOperationOperator, CloudDataTransferServiceListOperationsOperator,
    CloudDataTransferServicePauseOperationOperator, CloudDataTransferServiceResumeOperationOperator,
    CloudDataTransferServiceS3ToGCSOperator, CloudDataTransferServiceUpdateJobOperator,
)

warnings.warn(
    "This module is deprecated. "
    "Please use `airflow.providers.google.cloud.operators.cloud_storage_transfer_service`",
    DeprecationWarning, stacklevel=2
)


class GcpTransferServiceJobCreateOperator(CloudDataTransferServiceCreateJobOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.data_transfer.CloudDataTransferServiceCreateJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceCreateJobOperator`.""",
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class GcpTransferServiceJobDeleteOperator(CloudDataTransferServiceDeleteJobOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.data_transfer.CloudDataTransferServiceDeleteJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceDeleteJobOperator`.""",
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class GcpTransferServiceJobUpdateOperator(CloudDataTransferServiceUpdateJobOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.data_transfer.CloudDataTransferServiceUpdateJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceUpdateJobOperator`.""",
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class GcpTransferServiceOperationCancelOperator(CloudDataTransferServiceCancelOperationOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.data_transfer.CloudDataTransferServiceCancelOperationOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceCancelOperationOperator`.
            """,
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class GcpTransferServiceOperationGetOperator(CloudDataTransferServiceGetOperationOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.data_transfer
    .CloudDataTransferServiceGetOperationOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceGetOperationOperator`.""",
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class GcpTransferServiceOperationPauseOperator(CloudDataTransferServicePauseOperationOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.data_transfer
    .CloudDataTransferServicePauseOperationOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServicePauseOperationOperator`.
            """,
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class GcpTransferServiceOperationResumeOperator(CloudDataTransferServiceResumeOperationOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.data_transfer
    .CloudDataTransferServiceResumeOperationOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceResumeOperationOperator`.
            """,
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class GcpTransferServiceOperationsListOperator(CloudDataTransferServiceListOperationsOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.data_transfer
    .CloudDataTransferServiceListOperationsOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceListOperationsOperator`.
            """,
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class GoogleCloudStorageToGoogleCloudStorageTransferOperator(CloudDataTransferServiceGCSToGCSOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.data_transfe
    r.CloudDataTransferServiceGCSToGCSOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceGCSToGCSOperator`.
            """,
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)


class S3ToGoogleCloudStorageTransferOperator(CloudDataTransferServiceS3ToGCSOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.data_transfer
    .CloudDataTransferServiceS3ToGCSOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """"This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.data_transfer
            .CloudDataTransferServiceS3ToGCSOperator`.
            """,
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)
