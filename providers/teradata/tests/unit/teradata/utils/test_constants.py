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
    assert expected_msg == Constants.CC_OPR_SUCCESS_STATUS_MSG


def test_empty_profile_error_message():
    expected_msg = "Failed to %s the Vantage Cloud Lake Compute Cluster Instance due to an invalid compute cluster profile name."
    assert expected_msg == Constants.CC_OPR_EMPTY_PROFILE_ERROR_MSG


def test_non_exists_message():
    expected_msg = "Failed to %s the Vantage Cloud Lake Compute Cluster Instance because the specified compute cluster does not exist or the user lacks the necessary permissions to access the Compute Cluster Instance."
    assert expected_msg == Constants.CC_GRP_PRP_NON_EXISTS_MSG


def test_lake_support_only_message():
    expected_msg = "Failed to %s the Vantage Cloud Lake Compute Cluster Instance  because the Compute Cluster feature is supported only on the Vantage Cloud Lake system."
    assert expected_msg == Constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG


def test_timeout_value():
    assert Constants.CC_OPR_TIME_OUT == 1200


def test_poll_interval():
    assert Constants.CC_POLL_INTERVAL == 60
