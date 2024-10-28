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

from airflow.providers.teradata.utils.constants import Constants


def test_create_operations():
    assert Constants.CC_CREATE_OPR == "CREATE"


def test_create_suspend_operations():
    assert Constants.CC_CREATE_SUSPEND_OPR == "CREATE_SUSPEND"


def test_drop_operations():
    assert Constants.CC_DROP_OPR == "DROP"


def test_suspend_operations():
    assert Constants.CC_SUSPEND_OPR == "SUSPEND"


def test_resume_operations():
    assert Constants.CC_RESUME_OPR == "RESUME"


def test_initialize_db_status():
    assert Constants.CC_INITIALIZE_DB_STATUS == "Initializing"


def test_suspend_db_status():
    assert Constants.CC_SUSPEND_DB_STATUS == "Suspended"


def test_resume_db_status():
    assert Constants.CC_RESUME_DB_STATUS == "Running"


def test_operation_success_message():
    expected_msg = "Compute Cluster %s  %s operation completed successfully."
    assert Constants.CC_OPR_SUCCESS_STATUS_MSG == expected_msg


def test_operation_failure_message():
    expected_msg = "Compute Cluster %s  %s operation has failed."
    assert Constants.CC_OPR_FAILURE_STATUS_MSG == expected_msg


def test_initializing_status_message():
    expected_msg = "The environment is currently initializing. Please wait."
    assert Constants.CC_OPR_INITIALIZING_STATUS_MSG == expected_msg


def test_empty_profile_error_message():
    expected_msg = "Please provide a valid name for the compute cluster profile."
    assert Constants.CC_OPR_EMPTY_PROFILE_ERROR_MSG == expected_msg


def test_non_exists_message():
    expected_msg = "The specified Compute cluster is not present or The user doesn't have permission to access compute cluster."
    assert Constants.CC_GRP_PRP_NON_EXISTS_MSG == expected_msg


def test_unauthorized_message():
    expected_msg = "The %s operation is not authorized for the user."
    assert Constants.CC_GRP_PRP_UN_AUTHORIZED_MSG == expected_msg


def test_lake_support_only_message():
    expected_msg = "Compute Groups is supported only on Vantage Cloud Lake."
    assert Constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG == expected_msg


def test_timeout_error_message():
    expected_msg = "There is an issue with the %s operation. Kindly consult the administrator for assistance."
    assert Constants.CC_OPR_TIMEOUT_ERROR == expected_msg


def test_exists_message():
    expected_msg = "The specified Compute cluster is already exists."
    assert Constants.CC_GRP_PRP_EXISTS_MSG == expected_msg


def test_empty_copy_profile_error_message():
    expected_msg = (
        "Please provide a valid name for the source and target compute profile."
    )
    assert Constants.CC_OPR_EMPTY_COPY_PROFILE_ERROR_MSG == expected_msg


def test_timeout_value():
    assert Constants.CC_OPR_TIME_OUT == 1200


def test_poll_interval():
    assert Constants.CC_POLL_INTERVAL == 60
