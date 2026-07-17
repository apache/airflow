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


class Constants:
    """Define constants for Teradata Provider."""

    CC_CREATE_OPR = "CREATE"
    CC_CREATE_SUSPEND_OPR = "CREATE_SUSPEND"
    CC_DROP_OPR = "DROP"
    CC_SUSPEND_OPR = "SUSPEND"
    CC_RESUME_OPR = "RESUME"
    CC_INITIALIZE_DB_STATUS = "Initializing"
    CC_SUSPEND_DB_STATUS = "Suspended"
    CC_RESUME_DB_STATUS = "Running"
    CC_OPR_SUCCESS_STATUS_MSG = "Compute Cluster %s  %s operation completed successfully."
    CC_OPR_EMPTY_PROFILE_ERROR_MSG = "Failed to %s the Vantage Cloud Lake Compute Cluster Instance due to an invalid compute cluster profile name."
    CC_GRP_PRP_NON_EXISTS_MSG = "Failed to %s the Vantage Cloud Lake Compute Cluster Instance because the specified compute cluster does not exist or the user lacks the necessary permissions to access the Compute Cluster Instance."
    CC_GRP_LAKE_SUPPORT_ONLY_MSG = "Failed to %s the Vantage Cloud Lake Compute Cluster Instance  because the Compute Cluster feature is supported only on the Vantage Cloud Lake system."
    CC_OPR_TIMEOUT_ERROR = "Failed to %s the Vantage Cloud Lake Compute Cluster Instance `%s`. Please contact the administrator for assistance."
    CC_ERR_VERSION_GET = "Failed to manage the Vantage Cloud Lake Compute Cluster Instance due to an error while getting the Teradata database version."
    BTEQ_REMOTE_ERROR_MSG = (
        "Failed to establish a SSH connection to the remote machine for executing the BTEQ script."
    )
    BTEQ_UNEXPECTED_ERROR_MSG = "Failure while executing BTEQ script due to unexpected error."
    BTEQ_TIMEOUT_ERROR_MSG = "Failed to execute BTEQ script due to timeout after %s seconds."
    BTEQ_MISSED_PARAMS = "Failed to execute BTEQ script due to missing required parameters: either 'sql' or 'file_path' must be provided."
    BTEQ_INVALID_PATH = (
        "Failed to execute BTEQ script due to invalid file path: '%s' does not exist or is inaccessible."
    )
    BTEQ_INVALID_CHARSET = "Failed to execute BTEQ script because the provided file '%s' encoding differs from the specified BTEQ I/O encoding %s"
    BTEQ_REMOTE_FILE_PATH_INVALID = "Failed to execute BTEQ script due to invalid remote file path: '%s' does not exist or is inaccessible on the remote machine."
    CC_OPR_TIME_OUT = 1200
    CC_POLL_INTERVAL = 60
