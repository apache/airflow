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

import time
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, cast

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

if TYPE_CHECKING:
    from typing import Literal

    from simple_salesforce.bulk import SFBulkHandler

    from airflow.providers.common.compat.sdk import Context

# Salesforce error statusCode values that indicate a transient server-side
# condition rather than a permanent data problem. Records that fail with one of
# these codes can reasonably be re-submitted after a short delay.
_DEFAULT_TRANSIENT_ERROR_CODES: frozenset[str] = frozenset(
    {"UNABLE_TO_LOCK_ROW", "API_TEMPORARILY_UNAVAILABLE"}
)


class SalesforceBulkOperator(BaseOperator):
    """
    Execute a Salesforce Bulk API and pushes results to xcom.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SalesforceBulkOperator`

    :param operation: Bulk operation to be performed
        Available operations are in ['insert', 'update', 'upsert', 'delete', 'hard_delete']
    :param object_name: The name of the Salesforce object
    :param payload: list of dict to be passed as a batch
    :param external_id_field: unique identifier field for upsert operations
    :param batch_size: number of records to assign for each batch in the job
    :param use_serial: Process batches in serial mode
    :param salesforce_conn_id: The :ref:`Salesforce Connection id <howto/connection:salesforce>`.
    :param max_retries: Number of times to re-submit records that failed with a
        transient error code such as ``UNABLE_TO_LOCK_ROW`` or
        ``API_TEMPORARILY_UNAVAILABLE``.  Set to ``0`` (the default) to disable
        automatic retries.
    :param bulk_retry_delay: Seconds to wait before each retry attempt within the Bulk API retry loop. Defaults to ``5``.
    :param transient_error_codes: Collection of Salesforce error ``statusCode``
        values that should trigger a retry.  Defaults to
        ``{"UNABLE_TO_LOCK_ROW", "API_TEMPORARILY_UNAVAILABLE"}``.
    """

    template_fields: Sequence[str] = ("object_name", "payload", "external_id_field")

    available_operations = ("insert", "update", "upsert", "delete", "hard_delete")

    def __init__(
        self,
        *,
        operation: Literal["insert", "update", "upsert", "delete", "hard_delete"],
        object_name: str,
        payload: list,
        external_id_field: str = "Id",
        batch_size: int = 10000,
        use_serial: bool = False,
        salesforce_conn_id: str = "salesforce_default",
        max_retries: int = 0,
        bulk_retry_delay: float = 5.0,
        transient_error_codes: Iterable[str] = _DEFAULT_TRANSIENT_ERROR_CODES,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.operation = operation
        self.object_name = object_name
        self.payload = payload
        self.external_id_field = external_id_field
        self.batch_size = batch_size
        self.use_serial = use_serial
        self.salesforce_conn_id = salesforce_conn_id
        self.max_retries = max_retries
        self.bulk_retry_delay = bulk_retry_delay
        if isinstance(transient_error_codes, str):
            raise ValueError(
                "'transient_error_codes' must be a non-string iterable of strings, "
                f"got {transient_error_codes!r}. Wrap it in a list: [{transient_error_codes!r}]"
            )
        self.transient_error_codes = frozenset(transient_error_codes)
        self._validate_inputs()

    def _validate_inputs(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"'max_retries' must be a non-negative integer, got {self.max_retries!r}.")

        if self.bulk_retry_delay < 0:
            raise ValueError(
                f"'bulk_retry_delay' must be a non-negative number, got {self.bulk_retry_delay!r}."
            )

        if not self.object_name:
            raise ValueError("The required parameter 'object_name' cannot have an empty value.")

        if self.operation not in self.available_operations:
            raise ValueError(
                f"Operation {self.operation!r} not found! "
                f"Available operations are {self.available_operations}."
            )

    def _run_operation(self, bulk: SFBulkHandler, payload: list) -> list:
        """Submit *payload* through the configured Bulk API operation and return the result list."""
        obj = bulk.__getattr__(self.object_name)
        if self.operation == "upsert":
            return cast(
                "list",
                obj.upsert(
                    data=payload,
                    external_id_field=self.external_id_field,
                    batch_size=self.batch_size,
                    use_serial=self.use_serial,
                ),
            )
        return cast(
            "list",
            getattr(obj, self.operation)(
                data=payload,
                batch_size=self.batch_size,
                use_serial=self.use_serial,
            ),
        )

    def _retry_transient_failures(self, bulk: SFBulkHandler, payload: list, result: list) -> list:
        """
        Re-submit records that failed with a transient error, up to *max_retries* times.

        Salesforce Bulk API results are ordered identically to the input payload, so
        failed records are located by index and their retry results are written back
        into the same positions.
        """
        final = list(result)

        for attempt in range(1, self.max_retries + 1):
            retry_indices = [
                i
                for i, r in enumerate(final)
                if not r.get("success")
                and {e.get("statusCode") for e in r.get("errors", [])} & self.transient_error_codes
            ]

            if not retry_indices:
                break

            self.log.warning(
                "Salesforce Bulk API %s on %s: retrying %d record(s) with transient errors "
                "(attempt %d/%d, waiting %.1f second(s)).",
                self.operation,
                self.object_name,
                len(retry_indices),
                attempt,
                self.max_retries,
                self.bulk_retry_delay,
            )
            time.sleep(self.bulk_retry_delay)

            retry_result = list(self._run_operation(bulk, [payload[i] for i in retry_indices]))

            for list_pos, original_idx in enumerate(retry_indices):
                final[original_idx] = retry_result[list_pos]

        return final

    def execute(self, context: Context):
        """
        Make an HTTP request to Salesforce Bulk API.

        :param context: The task context during execution.
        :return: API response if do_xcom_push is True
        """
        sf_hook = SalesforceHook(salesforce_conn_id=self.salesforce_conn_id)
        conn = sf_hook.get_conn()
        bulk: SFBulkHandler = cast("SFBulkHandler", conn.__getattr__("bulk"))

        result = self._run_operation(bulk, self.payload)

        if self.max_retries > 0:
            result = self._retry_transient_failures(bulk, self.payload, result)

        if self.do_xcom_push and result:
            return result

        return None
