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
from flask import current_app, g
from flask_appbuilder import Model
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Sequence,
    String,
    Table,
    UniqueConstraint,
    event,
    func,
    select,
)
from sqlalchemy.orm import Mapped, backref, declared_attr, relationship

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.providers.common.compat.sqlalchemy.orm import mapped_column

"""
Compatibility note: The models in this file are duplicated from Flask AppBuilder.
"""

assoc_group_role = Table(
    "ab_group_role",
    Model.metadata,
    Column(
        "id",
        Integer,
        Sequence("ab_group_role_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    ),
    Column("group_id", Integer, ForeignKey("ab_group.id", ondelete="CASCADE")),
    Column("role_id", Integer, ForeignKey("ab_role.id", ondelete="CASCADE")),
    UniqueConstraint("group_id", "role_id"),
    Index("idx_group_id", "group_id"),
    Index("idx_group_role_id", "role_id"),
)

assoc_permission_role = Table(
    "ab_permission_view_role",
    Model.metadata,
    Column(
        "id",
        Integer,
        Sequence(
            "ab_permission_view_role_id_seq",
            start=1,
            increment=1,
            minvalue=1,
            cycle=False,
        ),
        primary_key=True,
    ),
    Column(
        "permission_view_id",
        Integer,
        ForeignKey("ab_permission_view.id", ondelete="CASCADE"),
    ),
    Column("role_id", Integer, ForeignKey("ab_role.id", ondelete="CASCADE")),
    UniqueConstraint("permission_view_id", "role_id"),
    Index("idx_permission_view_id", "permission_view_id"),
    Index("idx_role_id", "role_id"),
)

assoc_user_role = Table(
    "ab_user_role",
    Model.metadata,
    Column(
        "id",
        Integer,
        Sequence("ab_user_role_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    ),
    Column("user_id", Integer, ForeignKey("ab_user.id", ondelete="CASCADE")),
    Column("role_id", Integer, ForeignKey("ab_role.id", ondelete="CASCADE")),
    UniqueConstraint("user_id", "role_id"),
)

assoc_user_group = Table(
    "ab_user_group",
    Model.metadata,
    Column(
        "id",
        Integer,
        Sequence("ab_user_group_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    ),
    Column("user_id", Integer, ForeignKey("ab_user.id", ondelete="CASCADE")),
    Column("group_id", Integer, ForeignKey("ab_group.id", ondelete="CASCADE")),
    UniqueConstraint("user_id", "group_id"),
)


class Action(Model):
    """Represents permission actions such as `can_read`."""

    __tablename__ = "ab_permission"

    id: Mapped[int] = mapped_column(
        Integer,
        Sequence("ab_permission_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    def __repr__(self):
        return self.name


class Resource(Model):
    """Represents permission object such as `User` or `Dag`."""

    __tablename__ = "ab_view_menu"

    id: Mapped[int] = mapped_column(
        Integer,
        Sequence("ab_view_menu_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String(250), unique=True, nullable=False)

    def __eq__(self, other):
        return (isinstance(other, self.__class__)) and (self.name == other.name)

    def __hash__(self):
        return hash((self.id, self.name))

    def __neq__(self, other):
        return self.name != other.name

    def __repr__(self):
        return self.name


class Role(Model):
    """Represents a user role to which permissions can be assigned."""

    __tablename__ = "ab_role"

    id: Mapped[int] = mapped_column(
        Integer,
        Sequence("ab_role_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    permissions: Mapped[list[Permission]] = relationship(
        "Permission",
        secondary=assoc_permission_role,
        backref="role",
        lazy="joined",
        passive_deletes=True,
    )

    def __repr__(self):
        return self.name


class Permission(Model):
    """Permission pair comprised of an Action + Resource combo."""

    __tablename__ = "ab_permission_view"
    __table_args__ = (UniqueConstraint("permission_id", "view_menu_id"),)
    id: Mapped[int] = mapped_column(
        Integer,
        Sequence("ab_permission_view_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    )
    action_id: Mapped[int] = mapped_column("permission_id", Integer, ForeignKey("ab_permission.id"))
    action: Mapped[Action] = relationship("Action", lazy="joined", uselist=False)
    resource_id: Mapped[int] = mapped_column("view_menu_id", Integer, ForeignKey("ab_view_menu.id"))
    resource: Mapped[Resource] = relationship("Resource", lazy="joined", uselist=False)

    def __repr__(self):
        return str(self.action).replace("_", " ") + f" on {str(self.resource)}"


class Group(Model):
    """Represents an Airflow user group."""

    __tablename__ = "ab_group"

    id: Mapped[int] = mapped_column(
        Integer,
        Sequence("ab_group_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    label: Mapped[str | None] = mapped_column(String(150))
    description: Mapped[str | None] = mapped_column(String(512))
    users: Mapped[list[User]] = relationship(
        "User", secondary=assoc_user_group, backref="groups", passive_deletes=True
    )
    roles: Mapped[list[Role]] = relationship(
        "Role", secondary=assoc_group_role, backref="groups", passive_deletes=True
    )

    def __repr__(self):
        return self.name


class User(Model, BaseUser):
    """Represents an Airflow user which has roles assigned to it."""

    __tablename__ = "ab_user"

    id: Mapped[int] = mapped_column(
        Integer,
        Sequence("ab_user_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    )
    first_name: Mapped[str] = mapped_column(String(64), nullable=False)
    last_name: Mapped[str] = mapped_column(String(64), nullable=False)
    username: Mapped[str] = mapped_column(
        String(512).with_variant(String(512, collation="NOCASE"), "sqlite"), unique=True, nullable=False
    )
    password: Mapped[str | None] = mapped_column(String(256))
    active: Mapped[bool | None] = mapped_column(Boolean, default=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    last_login: Mapped[datetime.datetime | None] = mapped_column(DateTime, nullable=True)
    login_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fail_login_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    roles: Mapped[list[Role]] = relationship(
        "Role",
        secondary=assoc_user_role,
        backref="user",
        lazy="selectin",
        passive_deletes=True,
    )
    created_on: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, default=lambda: datetime.datetime.now(), nullable=True
    )
    changed_on: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, default=lambda: datetime.datetime.now(), nullable=True
    )

    @declared_attr
    def created_by_fk(self) -> Column:
        return Column(Integer, ForeignKey("ab_user.id"), default=self.get_user_id, nullable=True)

    @declared_attr
    def changed_by_fk(self) -> Column:
        return Column(Integer, ForeignKey("ab_user.id"), default=self.get_user_id, nullable=True)

    created_by: Mapped[User] = relationship(
        "User",
        backref=backref("created", uselist=True),
        remote_side=[id],
        primaryjoin="User.created_by_fk == User.id",
        uselist=False,
    )
    changed_by: Mapped[User] = relationship(
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
                    sm.session.execute(
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

    def get_id(self) -> str:
        return str(self.id)

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

    id = mapped_column(
        Integer,
        Sequence("ab_register_user_id_seq", start=1, increment=1, minvalue=1, cycle=False),
        primary_key=True,
    )
    first_name: Mapped[str] = mapped_column(String(64), nullable=False)
    last_name: Mapped[str] = mapped_column(String(64), nullable=False)
    username: Mapped[str] = mapped_column(
        String(512).with_variant(String(512, collation="NOCASE"), "sqlite"), unique=True, nullable=False
    )
    password: Mapped[str | None] = mapped_column(String(256))
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    registration_date: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, default=lambda: datetime.datetime.now(), nullable=True
    )
    registration_hash: Mapped[str | None] = mapped_column(String(256))


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
