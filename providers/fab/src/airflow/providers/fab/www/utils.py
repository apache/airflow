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

from typing import TYPE_CHECKING, Any

from flask_appbuilder.models.filters import BaseFilter
from flask_appbuilder.models.sqla import filters as fab_sqlafilters
from flask_appbuilder.models.sqla.filters import get_field_setup_query, set_value_to_type
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_babel import lazy_gettext
from sqlalchemy import types
from sqlalchemy.ext.associationproxy import AssociationProxy

from airflow.utils import timezone

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


class UtcAwareFilterMixin:
    """Mixin for filter for UTC time."""

    def apply(self, query, value):
        """Apply the filter."""
        if isinstance(value, str) and not value.strip():
            value = None
        else:
            value = timezone.parse(value, timezone=timezone.utc)

        return super().apply(query, value)


class FilterIsNull(BaseFilter):
    """Is null filter."""

    name = lazy_gettext("Is Null")
    arg_name = "emp"

    def apply(self, query, value):
        query, field = get_field_setup_query(query, self.model, self.column_name)
        value = set_value_to_type(self.datamodel, self.column_name, None)
        return query.filter(field == value)


class FilterIsNotNull(BaseFilter):
    """Is not null filter."""

    name = lazy_gettext("Is not Null")
    arg_name = "nemp"

    def apply(self, query, value):
        query, field = get_field_setup_query(query, self.model, self.column_name)
        value = set_value_to_type(self.datamodel, self.column_name, None)
        return query.filter(field != value)


class FilterGreaterOrEqual(BaseFilter):
    """Greater than or Equal filter."""

    name = lazy_gettext("Greater than or Equal")
    arg_name = "gte"

    def apply(self, query, value):
        query, field = get_field_setup_query(query, self.model, self.column_name)
        value = set_value_to_type(self.datamodel, self.column_name, value)

        if value is None:
            return query

        return query.filter(field >= value)


class FilterSmallerOrEqual(BaseFilter):
    """Smaller than or Equal filter."""

    name = lazy_gettext("Smaller than or Equal")
    arg_name = "lte"

    def apply(self, query, value):
        query, field = get_field_setup_query(query, self.model, self.column_name)
        value = set_value_to_type(self.datamodel, self.column_name, value)

        if value is None:
            return query

        return query.filter(field <= value)


class UtcAwareFilterSmallerOrEqual(UtcAwareFilterMixin, FilterSmallerOrEqual):
    """Smaller than or Equal filter for UTC time."""


class UtcAwareFilterGreaterOrEqual(UtcAwareFilterMixin, FilterGreaterOrEqual):
    """Greater than or Equal filter for UTC time."""


class UtcAwareFilterEqual(UtcAwareFilterMixin, fab_sqlafilters.FilterEqual):
    """Equality filter for UTC time."""


class UtcAwareFilterGreater(UtcAwareFilterMixin, fab_sqlafilters.FilterGreater):
    """Greater Than filter for UTC time."""


class UtcAwareFilterSmaller(UtcAwareFilterMixin, fab_sqlafilters.FilterSmaller):
    """Smaller Than filter for UTC time."""


class UtcAwareFilterNotEqual(UtcAwareFilterMixin, fab_sqlafilters.FilterNotEqual):
    """Not Equal To filter for UTC time."""


class AirflowFilterConverter(fab_sqlafilters.SQLAFilterConverter):
    """Retrieve conversion tables for Airflow-specific filters."""

    conversion_table = (
        (
            "is_utcdatetime",
            [
                UtcAwareFilterEqual,
                UtcAwareFilterGreater,
                UtcAwareFilterSmaller,
                UtcAwareFilterNotEqual,
                UtcAwareFilterSmallerOrEqual,
                UtcAwareFilterGreaterOrEqual,
            ],
        ),
        # FAB will try to create filters for extendedjson fields even though we
        # exclude them from all UI, so we add this here to make it ignore them.
        ("is_extendedjson", []),
        ("is_json", []),
        *fab_sqlafilters.SQLAFilterConverter.conversion_table,
    )

    def __init__(self, datamodel):
        super().__init__(datamodel)

        for _, filters in self.conversion_table:
            if FilterIsNull not in filters:
                filters.append(FilterIsNull)
            if FilterIsNotNull not in filters:
                filters.append(FilterIsNotNull)


class CustomSQLAInterface(SQLAInterface):
    """
    FAB does not know how to handle columns with leading underscores because they are not supported by WTForm.

    This hack will remove the leading '_' from the key to lookup the column names.
    """

    def __init__(self, obj, session: Session | None = None):
        super().__init__(obj, session=session)

        def clean_column_names():
            if self.list_properties:
                self.list_properties = {k.lstrip("_"): v for k, v in self.list_properties.items()}
            if self.list_columns:
                self.list_columns = {k.lstrip("_"): v for k, v in self.list_columns.items()}

        clean_column_names()
        # Support for AssociationProxy in search and list columns
        for obj_attr, desc in self.obj.__mapper__.all_orm_descriptors.items():
            if isinstance(desc, AssociationProxy):
                proxy_instance = getattr(self.obj, obj_attr)
                if hasattr(proxy_instance.remote_attr.prop, "columns"):
                    self.list_columns[obj_attr] = proxy_instance.remote_attr.prop.columns[0]
                    self.list_properties[obj_attr] = proxy_instance.remote_attr.prop

    def is_utcdatetime(self, col_name):
        """Check if the datetime is a UTC one."""
        from airflow.utils.sqlalchemy import UtcDateTime

        if col_name in self.list_columns:
            obj = self.list_columns[col_name].type
            return (
                isinstance(obj, UtcDateTime)
                or isinstance(obj, types.TypeDecorator)
                and isinstance(obj.impl, UtcDateTime)
            )
        return False

    def is_extendedjson(self, col_name):
        """Check if it is a special extended JSON type."""
        from airflow.utils.sqlalchemy import ExtendedJSON

        if col_name in self.list_columns:
            obj = self.list_columns[col_name].type
            return (
                isinstance(obj, ExtendedJSON)
                or isinstance(obj, types.TypeDecorator)
                and isinstance(obj.impl, ExtendedJSON)
            )
        return False

    def is_json(self, col_name):
        """Check if it is a JSON type."""
        from sqlalchemy import JSON

        if col_name in self.list_columns:
            obj = self.list_columns[col_name].type
            return (
                isinstance(obj, JSON) or isinstance(obj, types.TypeDecorator) and isinstance(obj.impl, JSON)
            )
        return False

    def get_col_default(self, col_name: str) -> Any:
        if col_name not in self.list_columns:
            # Handle AssociationProxy etc, or anything that isn't a "real" column
            return None
        return super().get_col_default(col_name)

    filter_converter_class = AirflowFilterConverter
