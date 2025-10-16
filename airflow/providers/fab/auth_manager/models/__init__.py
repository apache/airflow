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

import datetime

# This product contains a modified portion of 'Flask App Builder' developed by Daniel Vaz Gaspar.
# (https://github.com/dpgaspar/Flask-AppBuilder).
# Copyright 2013, Daniel Vaz Gaspar
from typing import TYPE_CHECKING

import packaging.version
from flask import current_app, g
from flask_appbuilder.models.sqla import Model
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    event,
    func,
    select,
)
from sqlalchemy.orm import backref, declared_attr, registry, relationship

from airflow import __version__ as airflow_version
from airflow.auth.managers.models.base_user import BaseUser
from airflow.models.base import _get_schema, naming_convention

if TYPE_CHECKING:
    try:
        from sqlalchemy import Identity
    except Exception:
        Identity = None

"""
Compatibility note: The models in this file are duplicated from Flask AppBuilder.
"""

metadata = MetaData(schema=_get_schema(), naming_convention=naming_convention)
mapper_registry = registry(metadata=metadata)

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) >= packaging.version.parse(
    "3.0.0"
):
    Model.metadata = metadata
else:
    from airflow.models.base import Base

    Model.metadata = Base.metadata


class Action(Model):
    """Represents permission actions such as `can_read`."""

    __tablename__ = "ab_permission"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)

    def __repr__(self):
        return self.name


class Resource(Model):
    """Represents permission object such as `User` or `Dag`."""

    __tablename__ = "ab_view_menu"
    id = Column(Integer, primary_key=True)
    name = Column(String(250), unique=True, nullable=False)

    def __eq__(self, other):
        return (isinstance(other, self.__class__)) and (self.name == other.name)

    def __neq__(self, other):
        return self.name != other.name

    def __repr__(self):
        return self.name


assoc_permission_role = Table(
    "ab_permission_view_role",
    Model.metadata,
    Column("id", Integer, primary_key=True),
    Column("permission_view_id", Integer, ForeignKey("ab_permission_view.id")),
    Column("role_id", Integer, ForeignKey("ab_role.id")),
    UniqueConstraint("permission_view_id", "role_id"),
)


class Role(Model):
    """Represents a user role to which permissions can be assigned."""

    __tablename__ = "ab_role"

    id = Column(Integer, primary_key=True)
    name = Column(String(64), unique=True, nullable=False)
    permissions = relationship("Permission", secondary=assoc_permission_role, backref="role", lazy="joined")

    def __repr__(self):
        return self.name


class Permission(Model):
    """Permission pair comprised of an Action + Resource combo."""

    __tablename__ = "ab_permission_view"
    __table_args__ = (UniqueConstraint("permission_id", "view_menu_id"),)
    id = Column(Integer, primary_key=True)
    action_id = Column("permission_id", Integer, ForeignKey("ab_permission.id"))
    action = relationship(
        "Action",
        uselist=False,
        lazy="joined",
    )
    resource_id = Column("view_menu_id", Integer, ForeignKey("ab_view_menu.id"))
    resource = relationship(
        "Resource",
        uselist=False,
        lazy="joined",
    )

    def __repr__(self):
        return str(self.action).replace("_", " ") + " on " + str(self.resource)


assoc_user_role = Table(
    "ab_user_role",
    Model.metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", Integer, ForeignKey("ab_user.id")),
    Column("role_id", Integer, ForeignKey("ab_role.id")),
    UniqueConstraint("user_id", "role_id"),
)


class User(Model, BaseUser):
    """Represents an Airflow user which has roles assigned to it."""

    __tablename__ = "ab_user"
    id = Column(Integer, primary_key=True)
    first_name = Column(String(256), nullable=False)
    last_name = Column(String(256), nullable=False)
    username = Column(
        String(512).with_variant(String(512, collation="NOCASE"), "sqlite"), unique=True, nullable=False
    )
    password = Column(String(256))
    active = Column(Boolean, default=True)
    email = Column(String(512), unique=True, nullable=False)
    last_login = Column(DateTime)
    login_count = Column(Integer)
    fail_login_count = Column(Integer)
    roles = relationship("Role", secondary=assoc_user_role, backref="user", lazy="selectin")
    created_on = Column(DateTime, default=datetime.datetime.now, nullable=True)
    changed_on = Column(DateTime, default=datetime.datetime.now, nullable=True)

    @declared_attr
    def created_by_fk(self):
        return Column(Integer, ForeignKey("ab_user.id"), default=self.get_user_id, nullable=True)

    @declared_attr
    def changed_by_fk(self):
        return Column(Integer, ForeignKey("ab_user.id"), default=self.get_user_id, nullable=True)

    created_by = relationship(
        "User",
        backref=backref("created", uselist=True),
        remote_side=[id],
        primaryjoin="User.created_by_fk == User.id",
        uselist=False,
    )
    changed_by = relationship(
        "User",
        backref=backref("changed", uselist=True),
        remote_side=[id],
        primaryjoin="User.changed_by_fk == User.id",
        uselist=False,
    )

    @classmethod
    def get_user_id(cls):
        try:
            return g.user.get_id()
        except Exception:
            return None

    @property
    def is_authenticated(self):
        return True

    @property
    def is_active(self):
        return self.active

    @property
    def is_anonymous(self):
        return False

    @property
    def perms(self):
        if not self._perms:
            # Using the ORM here is _slow_ (Creating lots of objects to then throw them away) since this is in
            # the path for every request. Avoid it if we can!
            if current_app:
                sm = current_app.appbuilder.sm
                self._perms: set[tuple[str, str]] = set(
                    sm.get_session.execute(
                        select(sm.action_model.name, sm.resource_model.name)
                        .join(sm.permission_model.action)
                        .join(sm.permission_model.resource)
                        .join(sm.permission_model.role)
                        .where(sm.role_model.user.contains(self))
                    )
                )
            else:
                self._perms = {
                    (perm.action.name, perm.resource.name) for role in self.roles for perm in role.permissions
                }
        return self._perms

    def get_id(self):
        return self.id

    def get_name(self) -> str:
        return self.username or self.email or self.user_id

    def get_full_name(self):
        return f"{self.first_name} {self.last_name}"

    def __repr__(self):
        return self.get_full_name()

    _perms = None


class RegisterUser(Model):
    """Represents a user registration."""

    __tablename__ = "ab_register_user"
    id = Column(Integer, primary_key=True)
    first_name = Column(String(256), nullable=False)
    last_name = Column(String(256), nullable=False)
    username = Column(
        String(512).with_variant(String(512, collation="NOCASE"), "sqlite"), unique=True, nullable=False
    )
    password = Column(String(256))
    email = Column(String(512), nullable=False)
    registration_date = Column(DateTime, default=datetime.datetime.now, nullable=True)
    registration_hash = Column(String(256))


@event.listens_for(User.__table__, "before_create")
def add_index_on_ab_user_username_postgres(table, conn, **kw):
    if conn.dialect.name != "postgresql":
        return
    index_name = "idx_ab_user_username"
    if not any(table_index.name == index_name for table_index in table.indexes):
        table.indexes.add(Index(index_name, func.lower(table.c.username), unique=True))


@event.listens_for(RegisterUser.__table__, "before_create")
def add_index_on_ab_register_user_username_postgres(table, conn, **kw):
    if conn.dialect.name != "postgresql":
        return
    index_name = "idx_ab_register_user_username"
    if not any(table_index.name == index_name for table_index in table.indexes):
        table.indexes.add(Index(index_name, func.lower(table.c.username), unique=True))
