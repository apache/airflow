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
"""
In-process bidirectional migration for supervisor schema bodies.

:class:`SchemaVersionMigrator` walks a :class:`~cadwyn.VersionBundle`
itself rather than going through Cadwyn's HTTP runner so the supervisor
can downgrade outgoing bodies and upgrade incoming bodies without a
network round-trip. The downgrade path additionally re-validates against
the cadwyn-generated versioned class so declarative
``schema(X).field(Y).didnt_exist`` instructions actually drop fields on
the wire.
"""

from __future__ import annotations

import copy
import functools
from typing import TYPE_CHECKING, Any, cast

import attrs
from cadwyn import generate_versioned_models

if TYPE_CHECKING:
    from cadwyn import VersionBundle
    from cadwyn.schema_generation import SchemaGenerator
    from pydantic import BaseModel


@attrs.define
class _BodyInfo:
    """
    Duck-type stand-in for Cadwyn's ``RequestInfo`` / ``ResponseInfo``.

    ``cadwyn.structure.data._AlterDataInstruction.__call__`` only reads
    and writes ``info.body``; the by-schema transformers we drive never
    touch FastAPI's Request/Response. Passing this minimal object lets
    us run cadwyn's migrations from a pure in-process code path with no
    HTTP stack.
    """

    body: dict[str, Any]


def _validate_schema_version(instance: SchemaVersionMigrator, _, value: str) -> str:
    return instance.resolve_version(value)


def _calculate_version_values(migrator: SchemaVersionMigrator) -> frozenset[str]:
    return frozenset(v.value for v in migrator._bundle.versions)


@attrs.define(kw_only=True)
class SchemaVersionMigrator:
    """
    Bidirectional in-process migrator for supervisor schema bodies.

    Each foreign runtime is pinned to a specific dated lang-SDK supervisor
    schema version; this class walks Cadwyn's ``VersionChange`` chain in-process
    to bridge the two::

        head shape  --- downgrade(msg, lang_sdk) --->  lang-SDK wire
        head shape  <-- upgrade(msg, lang_sdk)   ---   lang-SDK wire

    *supervisor_version* is fixed at construction time.

    note::
        Use ``bundle.versions[0].value`` to get the latest dated entry. Cadwyn
        keeps versions in newest-to-oldest order.

    A message whose Pydantic type is not mentioned by any ``schema(...)``
    instruction in the bundle is passed through as-is: Cadwyn keys its
    instruction dicts by message type, so the lookup misses and no
    transformer runs.
    """

    _bundle: VersionBundle
    _supervisor_version: str = attrs.field(validator=_validate_schema_version)

    # Caches over the bundle (which is immutable for the migrator's lifetime).
    # ``generate_versioned_models`` walks the full version graph;
    # ``_version_values`` mirrors cadwyn's internal lookup set without reaching
    # into its private attribute.
    _versioned_models: dict[str, SchemaGenerator] = attrs.field(init=False, default=None)
    _version_values: frozenset[str] = attrs.field(
        init=False,
        default=attrs.Factory(_calculate_version_values, takes_self=True),
    )

    def _versioned_class(self, version: str, model: type[BaseModel]) -> type[BaseModel]:
        """Get the Cadwyn-generated class for *model* at *version*."""
        if self._versioned_models is None:
            self._versioned_models = generate_versioned_models(self._bundle)
        return self._versioned_models[version][model]

    def resolve_version(self, v: str) -> str:
        """Validate *v* is present in the bundle."""
        if v not in self._version_values:
            raise ValueError(f"Version {v!r} not found in supervisor schema bundle")
        return v

    def downgrade(
        self,
        msg: BaseModel,
        target_version: str,
        **dump_opts,
    ) -> BaseModel:
        """
        Downgrade *msg* from server to *target_version*.

        Used on the supervisor -> foreign-runtime path: *msg* is a head-shape
        Pydantic instance, and the returned dict matches the target.

        :param msg: A Pydantic instance shaped according to the head
            (latest) version of the bundle.
        :param target_version: Dated supervisor schema version string in
            ``YYYY-MM-DD`` format.
        :param dump_opts: Forwarded to ``model_dump`` when dumping *msg* for
            migration. The mode is already set to ``json`` so datetime/UUID/Path
            etc. serialize to primitives the versioned-model validators inside
            the chain accept.
        :returns: A Pydantic instance shaped to the target version. The type
            of this object is dynamically generated by Cadwyn.
        """
        if (target_version := self.resolve_version(target_version)) == self._supervisor_version:
            return msg
        model = type(msg)
        info = _BodyInfo(msg.model_dump(**cast("dict[str, Any]", {**dump_opts, "mode": "json"})))
        for version in self._bundle.versions:
            if version.value > self._supervisor_version:
                continue
            if version.value <= target_version:
                break
            for change in version.changes:
                for instr in change.alter_response_by_schema_instructions.get(model, ()):
                    # TODO: Cadwyn is tightly coupled to Startlette request and
                    # response objects. Our supervisor does not use an HTTP
                    # framework, so we need to mock out the object. Fix this
                    # when Cadwyn provides a framework-agnostic interface.
                    instr(info)  # type: ignore[arg-type]
        # Re-validate against the versioned class so schema(X).field(Y).didnt_exist
        # instructions take effect: those alter the class shape, not the dict, so
        # without this round-trip the dropped field would still appear on the wire.
        versioned_class = self._versioned_class(target_version, model)
        return versioned_class.model_validate(info.body)

    def upgrade(
        self,
        body: dict[str, Any],
        model: type[BaseModel],
        source_version: str,
    ) -> dict[str, Any]:
        """
        Upgrade *body* from *source_version* to the supervisor's shape.

        Used on the foreign-runtime -> supervisor path: *body* is the
        already-deserialized payload off the wire (still in the lang-SDK's
        schema), and the returned dict is shaped for ``model_validate``
        against the head Pydantic class.

        *model* must be supplied because a dict carries no Python type
        information; the caller resolves it from the discriminator
        (``body["type"]``) and the registered-models index.

        :param body: The wire payload as a dict.
        :param model: The server-side Pydantic class *body* should validate
            against after migration.
        :param source_version: Dated supervisor schema version *body* is
            in. This should be a string in ``YYYY-MM-DD`` format.
        :returns: An upgraded version of *body* in the current supervisor shape.
        """
        if (source_version := self.resolve_version(source_version)) == self._supervisor_version:
            return model.model_validate(body).model_dump()
        info = _BodyInfo(copy.deepcopy(body))
        for version in self._bundle.reversed_versions:
            if version.value <= source_version:
                continue
            if version.value > self._supervisor_version:
                continue
            for change in version.changes:
                for instr in change.alter_request_by_schema_instructions.get(model, ()):
                    instr(info)  # type: ignore[arg-type]
        versioned_class = self._versioned_class(self._supervisor_version, model)
        return versioned_class.model_validate(info.body).model_dump()


@functools.cache
def get_schema_version_migrator() -> SchemaVersionMigrator:
    """
    Return the process-wide :class:`SchemaVersionMigrator` bound to the supervisor bundle.

    Cached so the bundle is bound once per process. The migrator holds
    no per-call state, so concurrent callers can share a single
    instance safely.
    """
    from airflow.sdk.execution_time.schema.versions import bundle

    return SchemaVersionMigrator(bundle=bundle, supervisor_version=bundle.versions[0].value)


__all__ = ["SchemaVersionMigrator", "get_schema_version_migrator"]
