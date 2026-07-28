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


class AnthropicError(Exception):
    """Base class for all Anthropic provider errors."""


class AnthropicBatchJobError(AnthropicError):
    """Raised when an Anthropic Message Batch fails or finishes with failed requests."""


class AnthropicBatchTimeout(AnthropicError):
    """Raised when an Anthropic Message Batch does not reach a terminal status in time."""


class AnthropicTriggerEventError(AnthropicError):
    """Raised when a deferred task resumes with a missing or malformed trigger event."""


class AnthropicAgentSessionError(AnthropicError):
    """Raised when a Managed Agents session terminates or fails."""


class AnthropicAgentSessionTimeout(AnthropicError):
    """Raised when a Managed Agents session does not reach a terminal status in time."""
