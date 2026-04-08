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

from unittest import mock

from airflow.providers.salesforce.operators.bulk import SalesforceBulkOperator


def _make_op(**kwargs):
    defaults = dict(
        task_id="test_task",
        operation="insert",
        object_name="Contact",
        payload=[{"FirstName": "Ada"}, {"FirstName": "Grace"}],
    )
    defaults.update(kwargs)
    return SalesforceBulkOperator(**defaults)


def _transient_failure(status_code="UNABLE_TO_LOCK_ROW"):
    return {
        "success": False,
        "errors": [{"statusCode": status_code, "message": "locked", "fields": []}],
    }


def _permanent_failure():
    return {
        "success": False,
        "errors": [
            {
                "statusCode": "REQUIRED_FIELD_MISSING",
                "message": "missing",
                "fields": ["Name"],
            }
        ],
    }


def _success():
    return {"success": True, "errors": []}


class TestSalesforceBulkOperatorRetry:
    def test_no_retry_when_max_retries_zero(self):
        op = _make_op(max_retries=0)
        assert op.max_retries == 0

        bulk_mock = mock.MagicMock()
        bulk_mock.__getattr__("Contact").insert.return_value = [_success(), _success()]

        with mock.patch("airflow.providers.salesforce.operators.bulk.SalesforceHook") as hook_cls:
            hook_cls.return_value.get_conn.return_value.bulk = bulk_mock
            result = op.execute(context={})

        assert result == [_success(), _success()]
        assert bulk_mock.__getattr__("Contact").insert.call_count == 1

    def test_transient_failure_is_retried(self):
        op = _make_op(max_retries=2, bulk_retry_delay=0)

        first_result = [_transient_failure(), _success()]
        second_result = [_success()]

        run_mock = mock.MagicMock(side_effect=[first_result, second_result])

        with mock.patch.object(op, "_run_operation", run_mock):
            with mock.patch("airflow.providers.salesforce.operators.bulk.time.sleep"):
                final = op._retry_transient_failures(
                    bulk=mock.MagicMock(),
                    payload=[{"FirstName": "Ada"}, {"FirstName": "Grace"}],
                    result=first_result,
                )

        assert final[0] == _success()
        assert final[1] == _success()
        assert run_mock.call_count == 2
        retry_call = run_mock.call_args_list[1]
        assert retry_call == mock.call(mock.ANY, [{"FirstName": "Ada"}])

    def test_permanent_failure_is_not_retried(self):
        op = _make_op(max_retries=3, bulk_retry_delay=0)
        result = [_permanent_failure(), _success()]

        run_mock = mock.MagicMock()

        with mock.patch.object(op, "_run_operation", run_mock):
            final = op._retry_transient_failures(
                bulk=mock.MagicMock(),
                payload=[{"FirstName": "Ada"}, {"FirstName": "Grace"}],
                result=result,
            )

        run_mock.assert_not_called()
        assert final[0] == _permanent_failure()

    def test_retries_stop_after_max_retries(self):
        op = _make_op(max_retries=2, bulk_retry_delay=0)

        always_transient = [_transient_failure()]
        run_mock = mock.MagicMock(return_value=always_transient)

        with mock.patch.object(op, "_run_operation", run_mock):
            with mock.patch("airflow.providers.salesforce.operators.bulk.time.sleep"):
                final = op._retry_transient_failures(
                    bulk=mock.MagicMock(),
                    payload=[{"FirstName": "Ada"}],
                    result=always_transient,
                )

        assert run_mock.call_count == 2
        assert final[0]["success"] is False

    def test_retry_delay_is_respected(self):
        op = _make_op(max_retries=1, bulk_retry_delay=30.0)

        run_mock = mock.MagicMock(return_value=[_success()])

        with mock.patch.object(op, "_run_operation", run_mock):
            with mock.patch("airflow.providers.salesforce.operators.bulk.time.sleep") as sleep_mock:
                op._retry_transient_failures(
                    bulk=mock.MagicMock(),
                    payload=[{"FirstName": "Ada"}],
                    result=[_transient_failure()],
                )

        sleep_mock.assert_called_once_with(30.0)

    def test_custom_transient_error_codes(self):
        op = _make_op(max_retries=1, bulk_retry_delay=0, transient_error_codes=["MY_CUSTOM_ERROR"])
        assert op.transient_error_codes == frozenset({"MY_CUSTOM_ERROR"})

        custom_failure = {
            "success": False,
            "errors": [{"statusCode": "MY_CUSTOM_ERROR", "message": "custom"}],
        }
        run_mock = mock.MagicMock(return_value=[_success()])

        with mock.patch.object(op, "_run_operation", run_mock):
            with mock.patch("airflow.providers.salesforce.operators.bulk.time.sleep"):
                final = op._retry_transient_failures(
                    bulk=mock.MagicMock(),
                    payload=[{"FirstName": "Ada"}],
                    result=[custom_failure],
                )

        run_mock.assert_called_once()
        assert final[0] == _success()

    def test_api_temporarily_unavailable_is_retried(self):
        op = _make_op(max_retries=1, bulk_retry_delay=0)
        failure = _transient_failure("API_TEMPORARILY_UNAVAILABLE")
        run_mock = mock.MagicMock(return_value=[_success()])

        with mock.patch.object(op, "_run_operation", run_mock):
            with mock.patch("airflow.providers.salesforce.operators.bulk.time.sleep"):
                final = op._retry_transient_failures(
                    bulk=mock.MagicMock(),
                    payload=[{"FirstName": "Ada"}],
                    result=[failure],
                )

        run_mock.assert_called_once()
        assert final[0] == _success()

    def test_mixed_failures_only_retries_transient(self):
        op = _make_op(max_retries=1, bulk_retry_delay=0)
        payload = [{"FirstName": "A"}, {"FirstName": "B"}, {"FirstName": "C"}]
        initial = [_transient_failure(), _permanent_failure(), _success()]

        run_mock = mock.MagicMock(return_value=[_success()])

        with mock.patch.object(op, "_run_operation", run_mock):
            with mock.patch("airflow.providers.salesforce.operators.bulk.time.sleep"):
                final = op._retry_transient_failures(
                    bulk=mock.MagicMock(),
                    payload=payload,
                    result=initial,
                )

        run_mock.assert_called_once_with(mock.ANY, [{"FirstName": "A"}])
        assert final[0] == _success()
        assert final[1] == _permanent_failure()
        assert final[2] == _success()
