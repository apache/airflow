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

import sys
import warnings
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.exceptions import RemovedFeatureError

if TYPE_CHECKING:
    from types import FrameType

UPGRADE_GUIDE_URL = "https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/upgrading.html"

NEXT_MAJOR_VERSION = "10.0.0"

# Off in every release before ``NEXT_MAJOR_VERSION``; the pull request that releases that version turns it
# on. It is not derived from the provider version, which can be bumped to a major for an unrelated
# breaking change. Tests patch it to cover both settings.
_NEXT_MAJOR_ENABLED = False


def is_next_major_enabled() -> bool:
    """
    Tell whether provider code behaves the way it does in ``NEXT_MAJOR_VERSION``.

    Code whose behaviour changes in that version carries both behaviours and asks this each time it
    runs. Reading it at import time would fix the answer before a test can change it.
    """
    return _NEXT_MAJOR_ENABLED


def _get_module_name(frame: FrameType) -> str:
    return frame.f_globals.get("__name__", "")


def _is_provider_code(frame: FrameType) -> bool:
    return _get_module_name(frame).startswith("airflow.providers.")


def _is_airflow_code(frame: FrameType) -> bool:
    name = _get_module_name(frame)
    return name == "airflow" or name.startswith("airflow.")


def _is_user_code(frame: FrameType) -> bool:
    if _is_airflow_code(frame) or _get_module_name(frame).partition(".")[0] in sys.stdlib_module_names:
        return False
    filename = frame.f_code.co_filename
    return "site-packages" not in filename and "dist-packages" not in filename


def _find_user_caller(provider_frame: FrameType) -> FrameType | None:
    """
    Return the frame of the user code that called the provider directly, if there is one.

    There is none when Airflow calls the provider code, as it does for ``execute``, ``poke`` and a
    trigger's ``run``, or when the call comes through the standard library or an installed library. The
    frame that would be found by walking further up in those cases is whatever started the task or the
    wrapper that made the call, not the code that has to change.
    """
    caller: FrameType | None = provider_frame
    while caller and _is_provider_code(caller):
        called, caller = caller, caller.f_back
        # ``BaseOperatorMeta`` wraps every operator ``__init__``, which puts one frame of Airflow's own
        # between a provider ``__init__`` and the code that instantiates the operator.
        if (
            caller
            and called.f_code.co_name == "__init__"
            and _is_airflow_code(caller)
            and not _is_provider_code(caller)
        ):
            caller = caller.f_back
    return caller if caller and _is_user_code(caller) else None


def warn_deprecated(message: str, *, removal_version: str) -> None:
    """
    Warn that the calling provider code ran a deprecated behaviour.

    Call this from the provider function that implements the deprecated behaviour, instead of calling
    ``warnings.warn`` there. Airflow shows a ``DeprecationWarning`` only when it is attributed to an
    ``airflow`` module, so one that ``stacklevel`` attributes to a Dag file or to other user code is
    dropped by Python's default filters. This warning always passes those filters. It points at the line
    of user code that called the provider when there is one, and at the calling provider code otherwise.

    Once ``is_next_major_enabled()`` is true, a deprecation that names ``NEXT_MAJOR_VERSION`` raises
    instead, so the deprecated code acts as removed until it is deleted.

    :param message: What is deprecated and what to use instead. The removal release and a link to the
        upgrade guide are appended.
    :param removal_version: The Amazon provider release that removes the deprecated behaviour. Pass
        ``NEXT_MAJOR_VERSION`` instead of a literal: searching for it lists everything that release
        removes, and if that release has to move, one line moves every removal with it.
    :raises RemovedFeatureError: When ``removal_version`` is ``NEXT_MAJOR_VERSION`` and
        ``is_next_major_enabled()`` is true.
    """
    if removal_version == NEXT_MAJOR_VERSION and is_next_major_enabled():
        raise RemovedFeatureError(
            f"{message} The deprecated behaviour was removed in version {removal_version} of the "
            f"Amazon provider. See {UPGRADE_GUIDE_URL}"
        )
    provider_frame = sys._getframe(1)
    frame = _find_user_caller(provider_frame) or provider_frame
    warnings.warn_explicit(
        f"{message} The deprecated behaviour will be removed in version {removal_version} of the "
        f"Amazon provider. See {UPGRADE_GUIDE_URL}",
        AirflowProviderDeprecationWarning,
        filename=frame.f_code.co_filename,
        lineno=frame.f_lineno,
        # The filters match on this name and not on the file, so the provider module gets the warning
        # shown even when the line it points at is in user code.
        module=_get_module_name(provider_frame),
        # Without a registry every call is shown, including each iteration of a loop.
        registry=frame.f_globals.setdefault("__warningregistry__", {}),
        module_globals=frame.f_globals,
    )
