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

# This product contains a modified portion of 'Flask App Builder' developed by Daniel Vaz Gaspar.
# (https://github.com/dpgaspar/Flask-AppBuilder).
# Copyright 2013, Daniel Vaz Gaspar

import datetime
import logging

from flask import current_app, g
from flask_appbuilder import const as c
from flask_appbuilder._compat import as_unicode
from flask_appbuilder.models.sqla import Model
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Sequence,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql.functions import current_time

from airflow.utils.session import provide_session

_dont_audit = False

log = logging.getLogger(__name__)


def get_session():
    return current_app.appbuilder.get_session


class Action(Model):
    """Represents permission actions such as `can_read`."""

    __tablename__ = "ab_permission"
    id = Column(Integer, Sequence("ab_permission_id_seq"), primary_key=True)
    name = Column(String(100), unique=True, nullable=False)

    def __repr__(self):
        return self.name

    @classmethod
    def get(cls, name: str, session=None):
        """
        Gets an existing action record.

        :param name: name
        :type name: str
        :return: Action record, if it exists
        :rtype: Action
        """
        session = session or get_session()
        return session.query(cls).filter_by(name=name).one_or_none()

    @classmethod
    def create(cls, name: str, session=None):
        """
        Adds an action to the backend, model action

        :param name:
            name of the action: 'can_add','can_edit' etc...
        """
        session = session or get_session()
        action = cls.get(name, session)
        if action is None:
            try:
                action = Action(name=name)
                session.add(action)
                session.commit()
                return action
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_PERMISSION.format(str(e)))
                session.rollback()
        return action

    @classmethod
    def delete(cls, name: str, session=None):
        session = session or get_session()
        action = Action.get(name, session)
        if not action:
            log.warning(c.LOGMSG_WAR_SEC_DEL_PERMISSION.format(name))
            return False
        try:
            perms = session.query(Permission).filter(Permission.action == action).all()
            if perms:
                log.warning(c.LOGMSG_WAR_SEC_DEL_PERM_PVM.format(action, perms))
                return False
            session.delete(action)
            session.commit()
            return True
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_PERMISSION.format(str(e)))
            session.rollback()
            return False


class Resource(Model):
    """Represents permission object such as `User` or `Dag`."""

    __tablename__ = "ab_view_menu"
    id = Column(Integer, Sequence("ab_view_menu_id_seq"), primary_key=True)
    name = Column(String(250), unique=True, nullable=False)

    def __eq__(self, other):
        return (isinstance(other, self.__class__)) and (self.name == other.name)

    def __neq__(self, other):
        return self.name != other.name

    def __repr__(self):
        return self.name

    @classmethod
    def get(cls, name, session=None):
        session = session or get_session()
        return session.query(Resource).filter_by(name=name).one_or_none()

    @classmethod
    def get_all(cls, session=None):
        """
        Gets all existing resource records.

        :return: List of all resources
        :rtype: List[Resource]
        """
        session = session or get_session()
        return session.query(Resource).all()

    @classmethod
    def create(cls, name, session=None):
        session = session or get_session()
        resource = cls.get(name, session)
        if resource is None:
            try:
                resource = Resource(name=name)
                session.add(resource)
                session.commit()
                return resource
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_VIEWMENU.format(str(e)))
                session.rollback()
        return resource

    @classmethod
    def delete(cls, name, session=None):
        session = session or get_session()
        resource = cls.get(name, session)
        if not resource:
            log.warning(c.LOGMSG_WAR_SEC_DEL_VIEWMENU.format(name))
            return False
        try:
            perms = session.query(Permission).filter(Permission.resource == resource).all()
            if perms:
                log.warning(c.LOGMSG_WAR_SEC_DEL_VIEWMENU_PVM.format(resource, perms))
                return False
            session.delete(resource)
            session.commit()
            return True
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_PERMISSION.format(str(e)))
            session.rollback()
            return False


assoc_permission_role = Table(
    "ab_permission_view_role",
    Model.metadata,
    Column("id", Integer, Sequence("ab_permission_view_role_id_seq"), primary_key=True),
    Column("permission_view_id", Integer, ForeignKey("ab_permission_view.id")),
    Column("role_id", Integer, ForeignKey("ab_role.id")),
    UniqueConstraint("permission_view_id", "role_id"),
)


class Role(Model):
    """Represents a user role to which permissions can be assigned."""

    __tablename__ = "ab_role"

    id = Column(Integer, Sequence("ab_role_id_seq"), primary_key=True)
    name = Column(String(64), unique=True, nullable=False)
    permissions = relationship("Permission", secondary=assoc_permission_role, backref="role")

    def __repr__(self):
        return self.name


class Permission(Model):
    """Permission pair comprised of an Action + Resource combo."""

    __tablename__ = "ab_permission_view"
    __table_args__ = (UniqueConstraint("permission_id", "view_menu_id"),)
    id = Column(Integer, Sequence("ab_permission_view_id_seq"), primary_key=True)
    action_id = Column("permission_id", Integer, ForeignKey("ab_permission.id"))
    action = relationship("Action")
    resource_id = Column("view_menu_id", Integer, ForeignKey("ab_view_menu.id"))
    resource = relationship("Resource")

    def __repr__(self):
        return str(self.action).replace("_", " ") + " on " + str(self.resource)


assoc_user_role = Table(
    "ab_user_role",
    Model.metadata,
    Column("id", Integer, Sequence("ab_user_role_id_seq"), primary_key=True),
    Column("user_id", Integer, ForeignKey("ab_user.id")),
    Column("role_id", Integer, ForeignKey("ab_role.id")),
    UniqueConstraint("user_id", "role_id"),
)


class User(Model):
    """Represents an Airflow user which has roles assigned to it."""

    __tablename__ = "ab_user"
    id = Column(Integer, Sequence("ab_user_id_seq"), primary_key=True)
    first_name = Column(String(64), nullable=False)
    last_name = Column(String(64), nullable=False)
    username = Column(String(64), unique=True, nullable=False)
    password = Column(String(256))
    active = Column(Boolean)
    email = Column(String(64), unique=True, nullable=False)
    last_login = Column(DateTime)
    login_count = Column(Integer)
    fail_login_count = Column(Integer)
    roles = relationship("Role", secondary=assoc_user_role, backref="user")
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
            return g.user.id
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

    def get_id(self):
        return as_unicode(self.id)

    def get_full_name(self):
        return f"{self.first_name} {self.last_name}"

    def __repr__(self):
        return self.get_full_name()


class RegisterUser(Model):
    """Represents a user registration."""

    __tablename__ = "ab_register_user"
    id = Column(Integer, Sequence("ab_register_user_id_seq"), primary_key=True)
    first_name = Column(String(64), nullable=False)
    last_name = Column(String(64), nullable=False)
    username = Column(String(64), unique=True, nullable=False)
    password = Column(String(256))
    email = Column(String(64), nullable=False)
    registration_date = Column(DateTime, default=datetime.datetime.now, nullable=True)
    registration_hash = Column(String(256))
