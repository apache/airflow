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

import contextlib
import copy
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import attrs

from airflow.models.abstractoperator import AbstractOperator
from airflow.sdk.definitions.mappedoperator import MappedOperator as TaskSDKMappedOperator
from airflow.triggers.base import StartTriggerArgs
from airflow.utils.helpers import prevent_duplicates

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.sdk.definitions.context import Context


@attrs.define(
    kw_only=True,
    # Disable custom __getstate__ and __setstate__ generation since it interacts
    # badly with Airflow's DAG serialization and pickling. When a mapped task is
    # deserialized, subclasses are coerced into MappedOperator, but when it goes
    # through DAG pickling, all attributes defined in the subclasses are dropped
    # by attrs's custom state management. Since attrs does not do anything too
    # special here (the logic is only important for slots=True), we use Python's
    # built-in implementation, which works (as proven by good old BaseOperator).
    getstate_setstate=False,
    repr=False,
)
# TODO: Task-SDK: Multiple inheritance is a crime. There must be a better way
class MappedOperator(TaskSDKMappedOperator, AbstractOperator):  # type: ignore[misc] # It complains about weight_rule being different
    """Object representing a mapped operator in a DAG."""

    def _expand_mapped_kwargs(
        self, context: Mapping[str, Any], session: Session, *, include_xcom: bool
    ) -> tuple[Mapping[str, Any], set[int]]:
        """
        Get the kwargs to create the unmapped operator.

        This exists because taskflow operators expand against op_kwargs, not the
        entire operator kwargs dict.
        """
        return self._get_specified_expand_input().resolve(context, session, include_xcom=include_xcom)

    def _get_unmap_kwargs(self, mapped_kwargs: Mapping[str, Any], *, strict: bool) -> dict[str, Any]:
        """
        Get init kwargs to unmap the underlying operator class.

        :param mapped_kwargs: The dict returned by ``_expand_mapped_kwargs``.
        """
        if strict:
            prevent_duplicates(
                self.partial_kwargs,
                mapped_kwargs,
                fail_reason="unmappable or already specified",
            )

        # If params appears in the mapped kwargs, we need to merge it into the
        # partial params, overriding existing keys.
        params = copy.copy(self.params)
        with contextlib.suppress(KeyError):
            params.update(mapped_kwargs["params"])

        # Ordering is significant; mapped kwargs should override partial ones,
        # and the specially handled params should be respected.
        return {
            "task_id": self.task_id,
            "dag": self.dag,
            "task_group": self.task_group,
            "start_date": self.start_date,
            "end_date": self.end_date,
            **self.partial_kwargs,
            **mapped_kwargs,
            "params": params,
        }

    def expand_start_from_trigger(self, *, context: Context, session: Session) -> bool:
        """
        Get the start_from_trigger value of the current abstract operator.

        MappedOperator uses this to unmap start_from_trigger to decide whether to start the task
        execution directly from triggerer.

        :meta private:
        """
        # start_from_trigger only makes sense when start_trigger_args exists.
        if not self.start_trigger_args:
            return False

        mapped_kwargs, _ = self._expand_mapped_kwargs(context, session, include_xcom=False)
        if self._disallow_kwargs_override:
            prevent_duplicates(
                self.partial_kwargs,
                mapped_kwargs,
                fail_reason="unmappable or already specified",
            )

        # Ordering is significant; mapped kwargs should override partial ones.
        return mapped_kwargs.get(
            "start_from_trigger", self.partial_kwargs.get("start_from_trigger", self.start_from_trigger)
        )

    def expand_start_trigger_args(self, *, context: Context, session: Session) -> StartTriggerArgs | None:
        """
        Get the kwargs to create the unmapped start_trigger_args.

        This method is for allowing mapped operator to start execution from triggerer.
        """
        if not self.start_trigger_args:
            return None

        mapped_kwargs, _ = self._expand_mapped_kwargs(context, session, include_xcom=False)
        if self._disallow_kwargs_override:
            prevent_duplicates(
                self.partial_kwargs,
                mapped_kwargs,
                fail_reason="unmappable or already specified",
            )

        # Ordering is significant; mapped kwargs should override partial ones.
        trigger_kwargs = mapped_kwargs.get(
            "trigger_kwargs",
            self.partial_kwargs.get("trigger_kwargs", self.start_trigger_args.trigger_kwargs),
        )
        next_kwargs = mapped_kwargs.get(
            "next_kwargs",
            self.partial_kwargs.get("next_kwargs", self.start_trigger_args.next_kwargs),
        )
        timeout = mapped_kwargs.get(
            "trigger_timeout", self.partial_kwargs.get("trigger_timeout", self.start_trigger_args.timeout)
        )
        return StartTriggerArgs(
            trigger_cls=self.start_trigger_args.trigger_cls,
            trigger_kwargs=trigger_kwargs,
            next_method=self.start_trigger_args.next_method,
            next_kwargs=next_kwargs,
            timeout=timeout,
        )
