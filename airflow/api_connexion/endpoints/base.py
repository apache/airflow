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

from typing import Any, Container, Dict, Generic, Iterable, List, Optional, Tuple, Type, TypeVar

from flask import request
from marshmallow import Schema, ValidationError
from sqlalchemy.orm import Query
from sqlalchemy.sql import ColumnElement

from airflow import settings
from airflow._vendor.connexion import NoContent
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.compat.functools import cached_property
from airflow.models.base import Base

M = TypeVar("M", bound=Base)


class Endpoints(Generic[M]):
    """Base endpoint implementation generator class for an abstract resource.

    This class does not assume the data storage backend, and a resource must
    implement various methods to make things work. If the resource is a
    SQLAlchemy model, see ``ModelEndpoints``.
    """

    resource_name: str  # Name of the resource for display in error messages etc.
    instance_index: List[str]  # List keys to uniquely select an instance.

    # A list of two-tuples mapping a field's internal name to external (as seen by the user).
    internal_external_lookups: Dict[str, str]

    collection_schema: Schema
    instance_schema: Schema
    protected_fields: List[str]  # Fields that cannot be updated.

    @cached_property
    def external_internal_lookups(self) -> Dict[str, str]:
        """Map external to internal names."""
        return {e: i for i, e in self.internal_external_lookups.items()}

    def prepare_collection_for_dump(self, collection: Iterable[M]) -> Any:
        """Return something for collection_schema to serialize."""
        raise NotImplementedError()

    def get_collection(
        self,
        *,
        limit: int,
        offset: int,
        order_by: str,
    ) -> Iterable[M]:
        raise NotImplementedError()

    def get_instance(self, **kwargs: Any) -> M:
        raise NotImplementedError()

    def get_instance_or_none(self, **kwargs: Any) -> Optional[M]:
        raise NotImplementedError()

    def create_instance(self, data: Dict[str, Any]) -> M:
        raise NotImplementedError()

    def delete_instance(self, **kwargs: Any) -> None:
        raise NotImplementedError()

    def edit_instance(self, instance: M, data: Dict[str, Any]) -> None:
        raise NotImplementedError()

    @format_parameters({"limit": check_limit})
    def get_collection_view(
        self,
        *,
        limit: int,
        offset: int,
        order_by: str,
    ) -> Any:
        collection = self.get_collection(limit=limit, offset=offset, order_by=order_by)
        return self.collection_schema.dump(self.prepare_collection_for_dump(collection))

    def create_instance_view(self) -> Any:
        try:
            data = self.instance_schema.load(request.json)
        except ValidationError as e:
            raise BadRequest(detail=str(e.messages))
        select_args = {k: data[k] for k in self.instance_index}
        if self.get_instance_or_none(**select_args) is not None:
            criteria = ", ".join(
                f"{self.internal_external_lookups.get(k, k)} {v!r}" for k, v in select_args.items()
            )
            raise AlreadyExists(detail=f"{self.resource_name} already exists: {criteria}")
        instance = self.create_instance(data)
        return self.instance_schema.dump(instance)

    def get_instance_view(self, **kwargs: Any) -> Any:
        return self.instance_schema.dump(self.get_instance(**kwargs))

    def delete_instance_view(self, **kwargs: Any) -> Any:
        self.delete_instance(**kwargs)
        return NoContent, 204

    def _mask_data_for_update(
        self,
        data: Dict[str, Any],
        raw_fields: Container[str],
        masks: List[str],
    ) -> Dict[str, Any]:
        masks = [f.strip() for f in masks]
        # Compare with the raw data, not the cleaned data, so the masks' field
        # names are "external" and match the user's expectation.
        unknown_masks = [mask for mask in masks if mask not in raw_fields]
        if unknown_masks:
            display = ", ".join(unknown_masks)
            raise BadRequest(detail=f"Mask field not specified in payload: {display}")
        # Now we can convert the masks into internal field names.
        masks = (self.external_internal_lookups.get(mask, mask) for mask in masks)
        return {key: data[key] for key in masks}

    def edit_instance_view(self, __update_masks: Optional[List[str]], **kwargs: Any) -> Any:
        try:
            data = self.instance_schema.load(request.json, partial=True)
        except ValidationError as e:
            # If validation get to here, it is extra field validation.
            raise BadRequest(detail=str(e.messages))

        if __update_masks:
            data = self._mask_data_for_update(data, request.json, __update_masks)

        instance = self.get_instance(**kwargs)

        found_protected_field_reprs = [
            repr(self.internal_external_lookups.get(field, field))
            for field in self.protected_fields
            if field in data and data[field] != getattr(instance, field)
        ]
        if found_protected_field_reprs:
            display = ", ".join(found_protected_field_reprs)
            raise BadRequest(detail=f"Cannot update protected {self.resource_name} field: {display}")

        self.edit_instance(instance, data)
        return self.instance_schema.dump(instance)


class ModelEndpoints(Endpoints[M]):
    """Endpoint implementation generator class for SQLAlchemy model resource."""

    model: Type[M]  # SQLAlchemy model for this resource.

    orderings: Dict[str, ColumnElement]  # Mapping of keys and columns for ordering.
    primary_key: Dict[str, ColumnElement]  # Key to look up a single unique instance.

    def __init__(self, session: settings.SASession) -> None:
        super().__init__()
        self.session = session

    @property
    def resource_name(self) -> str:
        return self.model.__name__

    @property
    def instance_index(self) -> List[str]:
        return list(self.primary_key)

    def _apply_ordering(self, query: Query, order_by: str) -> Query:
        if order_by[:1] == "-":
            order_key = order_by[1:]
            desc = True
        else:
            order_key = order_by
            desc = False
        try:
            order_attr = self.orderings[order_key]
        except KeyError:
            raise BadRequest(detail=f"Ordering with {order_key!r} on {self.resource_name} is not allowed")
        if desc:
            order_attr = order_attr.desc()
        return query.order_by(order_attr)

    def _parse_instance_kwargs(self, kwargs: Dict[str, Any]) -> List[Tuple[ColumnElement, str, Any]]:
        return [(attr, key, kwargs[key]) for key, attr in self.primary_key.items()]

    def get_collection(
        self,
        limit: int,
        offset: int,
        order_by: str,
    ) -> Iterable[M]:
        query = self.session.query(self.model)
        if order_by:
            query = self._apply_ordering(query, order_by)
        if offset:
            query = query.offset(offset)
        query = query.limit(limit)
        return query

    def get_instance_or_none(self, **kwargs: Any) -> Optional[M]:
        filter_args = (attr == value for attr, _, value in self._parse_instance_kwargs(kwargs))
        return self.session.query(self.model).filter(*filter_args).one_or_none()

    def get_instance(self, **kwargs: Any) -> M:
        instance = self.get_instance_or_none(**kwargs)
        if instance is None:
            display = ", ".join(
                f"{self.internal_external_lookups.get(k, k)} {v!r}"
                for _, k, v in self._parse_instance_kwargs(kwargs)
            )
            detail = f"{self.resource_name} with {display} does not exist"
            raise NotFound(f"{self.resource_name} not found", detail=detail)
        return instance

    def create_instance(self, data: Dict[str, Any]) -> M:
        instance = self.model(**data)
        self.session.add(instance)
        self.session.flush()
        return instance

    def delete_instance(self, **kwargs: Any) -> None:
        instance = self.get_instance(**kwargs)
        self.session.delete(instance)
        self.session.flush()

    def edit_instance(self, instance: M, data: Dict[str, Any]) -> None:
        for key, value in data.items():
            setattr(instance, key, value)
        self.session.add(instance)
        self.session.flush()
