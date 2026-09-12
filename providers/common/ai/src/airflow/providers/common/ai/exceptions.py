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

from airflow.providers.common.compat.sdk import AirflowException


class HITLMaxIterationsError(AirflowException):
    """Raised when the HITL review loop exhausts max iterations without approval or rejection."""


class LLMFileAnalysisError(ValueError):
    """Base class for file-analysis validation errors."""


class LLMFileAnalysisUnsupportedFormatError(LLMFileAnalysisError):
    """Raised when a file format is not supported by LLM file analysis."""


class LLMFileAnalysisLimitExceededError(LLMFileAnalysisError):
    """Raised when file-analysis safety limits are exceeded."""


class LLMFileAnalysisMultimodalRequiredError(LLMFileAnalysisUnsupportedFormatError):
    """Raised when image/PDF inputs are used without ``multi_modal=True``."""


class ManagedAgentInvocationError(RuntimeError):
    """
    Raised when a managed agent cannot be reached and retrying will not help.

    Reserved for terminal conditions -- bad credentials, a missing agent, a
    revoked quota. Transient failures should propagate unchanged so Airflow's
    task-level retry handles them, and requests the model could fix by
    rephrasing should raise ``pydantic_ai.exceptions.ModelRetry`` instead.
    """


class LLMBatchInputError(ValueError):
    """Base class for ``@task.llm_batch`` errors detected before a batch is submitted."""


class LLMBatchLimitExceededError(LLMBatchInputError):
    """Raised when a batch exceeds a provider's request-count or payload-size limit."""


class LLMBatchModelMismatchError(LLMBatchInputError):
    """Raised when a per-request ``model`` override resolves to a different adapter than the batch."""


class UnsupportedBatchProviderError(LLMBatchInputError):
    """Raised when the connection/model combination has no batch adapter at all."""


class BatchProviderNotYetSupportedError(LLMBatchInputError):
    """
    Raised for a connection type common.ai recognizes but does not yet support for batch.

    Distinct from :class:`UnsupportedBatchProviderError`: that one means "this
    path does not exist"; this one means "this path is known but not built
    yet" (e.g. Azure OpenAI, Bedrock, Vertex).
    """


class LLMBatchStaleStateError(LLMBatchInputError):
    """Raised on retry when the recorded input fingerprint no longer matches and ``on_stale_state="fail"``."""


class LLMBatchOutputTypeError(LLMBatchInputError):
    """Raised when ``output_type`` itself cannot produce a JSON Schema (not a per-item validation failure)."""


class LLMBatchJobError(RuntimeError):
    """Base class for ``@task.llm_batch`` errors detected after a batch is submitted."""


class LLMBatchTimeoutError(LLMBatchJobError):
    """Raised when the wall-clock defer budget is exhausted before the batch reaches a terminal state."""


class LLMBatchOrphanedIntentError(LLMBatchJobError):
    """
    Raised when a Phase A intent record has no matching batch and ``on_orphaned_intent="fail"``.

    A previous attempt crashed between submitting the batch and recording that it succeeded;
    provider-side recovery (:meth:`~airflow.providers.common.ai.batch.base.BatchAdapter.find_orphaned_batch`)
    found nothing. Unlike a stale-fingerprint mismatch, there is no old batch id to cancel as a
    loss-limiting step here -- resubmitting risks paying twice if the original request actually
    reached the provider and simply could not be recovered.
    """


class LLMBatchPartialFailureError(LLMBatchJobError):
    """Raised when ``fail_on_partial_error=True`` and any request errored or failed output validation."""


class LLMBatchStateReadError(LLMBatchJobError):
    """
    Raised when the run-stable state file exists but cannot be read or parsed.

    Distinct from "no recorded state" (a plain, expected ``None`` -- see
    ``batch/state.py``'s ``read_state``): an I/O failure or a corrupt/malformed
    state file must never be silently treated as "safe to submit a new batch",
    since that is exactly the condition that causes a duplicate, billable
    submission. This is deliberately retryable (a transient object-storage
    blip resolves on its own); a task that fails with this error should be
    retried, not have its state file deleted.
    """
