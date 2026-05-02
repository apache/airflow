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
"""Exceptions raised by common.ai privacy helpers."""

from __future__ import annotations

# Shared message used wherever binary multimodal attachments are rejected.
ATTACHMENT_REJECTION_MSG = (
    "input_guard cannot sanitize binary multimodal attachments. "
    "Disable multimodal inputs or set attachment_policy='allow_unmodified'."
)


class InputGuardError(ValueError):
    """Base error for invalid or unsupported input-guard behavior."""


class InputGuardAttachmentError(InputGuardError):
    """Raised when guarded multimodal attachments are not allowed."""
