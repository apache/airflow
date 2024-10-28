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
    CC_OPR_FAILURE_STATUS_MSG = "Compute Cluster %s  %s operation has failed."
    CC_OPR_INITIALIZING_STATUS_MSG = (
        "The environment is currently initializing. Please wait."
    )
    CC_OPR_EMPTY_PROFILE_ERROR_MSG = (
        "Please provide a valid name for the compute cluster profile."
    )
    CC_GRP_PRP_NON_EXISTS_MSG = "The specified Compute cluster is not present or The user doesn't have permission to access compute cluster."
    CC_GRP_PRP_UN_AUTHORIZED_MSG = "The %s operation is not authorized for the user."
    CC_GRP_LAKE_SUPPORT_ONLY_MSG = (
        "Compute Groups is supported only on Vantage Cloud Lake."
    )
    CC_OPR_TIMEOUT_ERROR = "There is an issue with the %s operation. Kindly consult the administrator for assistance."
    CC_GRP_PRP_EXISTS_MSG = "The specified Compute cluster is already exists."
    CC_OPR_EMPTY_COPY_PROFILE_ERROR_MSG = (
        "Please provide a valid name for the source and target compute profile."
    )
    CC_OPR_TIME_OUT = 1200
    CC_POLL_INTERVAL = 60
