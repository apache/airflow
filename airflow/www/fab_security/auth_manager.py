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

import logging
import uuid
from typing import List, Optional, Set, Tuple

from flask_appbuilder import const as c
from sqlalchemy import and_, func, literal
from sqlalchemy.orm.exc import MultipleResultsFound
from werkzeug.security import generate_password_hash

from airflow.security import permissions
from airflow.www.fab_security.sqla.models import (
    Action,
    Permission,
    RegisterUser,
    Resource,
    Role,
    User,
    assoc_permission_role,
)

log = logging.getLogger(__name__)


class AuthorizationManager:
    def __init__(self, get_session):
        self._get_session = get_session

    @property
    def get_session(self):
        return self._get_session

    def create_action(self, name):
        """
        Adds an action to the backend, model action

        :param name:
            name of the action: 'can_add','can_edit' etc...
        """
        action = self.get_action(name)
        if action is None:
            try:
                action = Action(name=name)
                self.get_session.add(action)
                self.get_session.commit()
                return action
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_PERMISSION.format(str(e)))
                self.get_session.rollback()
        return action

    def get_action(self, name: str) -> Action:
        """
        Gets an existing action record.

        :param name: name
        :type name: str
        :return: Action record, if it exists
        :rtype: Action
        """
        return self.get_session.query(Action).filter_by(name=name).one_or_none()

    def delete_action(self, name: str) -> bool:
        """
        Deletes a permission action.

        :param name: Name of action to delete (e.g. can_read).
        :type name: str
        :return: Whether or not delete was successful.
        :rtype: bool
        """
        action = self.get_action(name)
        if not action:
            log.warning(c.LOGMSG_WAR_SEC_DEL_PERMISSION.format(name))
            return False
        try:
            perms = self.get_session.query(Permission).filter(Permission.action == action).all()
            if perms:
                log.warning(c.LOGMSG_WAR_SEC_DEL_PERM_PVM.format(action, perms))
                return False
            self.get_session.delete(action)
            self.get_session.commit()
            return True
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_PERMISSION.format(str(e)))
            self.get_session.rollback()
            return False

    def create_resource(self, name) -> Resource:
        """
        Create a resource with the given name.

        :param name: The name of the resource to create created.
        :type name: str
        :return: The FAB resource created.
        :rtype: Resource
        """
        resource = self.get_resource(name)
        if resource is None:
            try:
                resource = Resource(name=name)
                self.get_session.add(resource)
                self.get_session.commit()
                return resource
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_VIEWMENU.format(str(e)))
                self.get_session.rollback()
        return resource

    def get_resource(self, name: str) -> Resource:
        """
        Returns a resource record by name, if it exists.

        :param name: Name of resource
        :type name: str
        :return: Resource record
        :rtype: Resource
        """
        return self.get_session.query(Resource).filter_by(name=name).one_or_none()

    def delete_resource(self, name: str) -> bool:
        """
        Deletes a Resource from the backend

        :param name:
            name of the resource
        """
        resource = self.get_resource(name)
        if not resource:
            log.warning(c.LOGMSG_WAR_SEC_DEL_VIEWMENU.format(name))
            return False
        try:
            perms = self.get_session.query(Permission).filter(Permission.resource == resource).all()
            if perms:
                log.warning(c.LOGMSG_WAR_SEC_DEL_VIEWMENU_PVM.format(resource, perms))
                return False
            self.get_session.delete(resource)
            self.get_session.commit()
            return True
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_PERMISSION.format(str(e)))
            self.get_session.rollback()
            return False

    def get_all_resources(self) -> List[Resource]:
        """
        Gets all existing resource records.

        :return: List of all resources
        :rtype: List[Resource]
        """
        return self.get_session.query(Resource).all()

    def create_permission(self, action_name, resource_name):
        """
        Adds a permission on a resource to the backend

        :param action_name:
            name of the action to add: 'can_add','can_edit' etc...
        :param resource_name:
            name of the resource to add
        """
        if not (action_name and resource_name):
            return None
        perm = self.get_permission(action_name, resource_name)
        if perm:
            return perm
        resource = self.create_resource(resource_name)
        action = self.create_action(action_name)
        perm = Permission(resource_id=resource.id, action_id=action.id)
        try:
            self.get_session.add(perm)
            self.get_session.commit()
            log.info(c.LOGMSG_INF_SEC_ADD_PERMVIEW.format(str(perm)))
            return perm
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_ADD_PERMVIEW.format(str(e)))
            self.get_session.rollback()

    def get_permission(self, action_name: str, resource_name: str) -> Permission:
        """
        Gets a permission made with the given action->resource pair, if the permission already exists.

        :param action_name: Name of action
        :type action_name: str
        :param resource_name: Name of resource
        :type resource_name: str
        :return: The existing permission
        :rtype: Permission
        """
        action = self.get_action(action_name)
        resource = self.get_resource(resource_name)
        if action and resource:
            return (
                self.get_session.query(Permission).filter_by(action=action, resource=resource).one_or_none()
            )

    def get_all_permissions(self) -> Set[Tuple[str, str]]:
        """Returns all permissions as a set of tuples with the action and resource names"""
        return set(
            self.get_session.query(Permission)
            .join(Action)
            .join(Resource)
            .with_entities(Action.name, Resource.name)
            .all()
        )

    def delete_permission(self, action_name: str, resource_name: str) -> None:
        """
        Deletes the permission linking an action->resource pair. Doesn't delete the
        underlying action or resource.

        :param action_name: Name of existing action
        :type action_name: str
        :param resource_name: Name of existing resource
        :type resource_name: str
        :return: None
        :rtype: None
        """
        if not (action_name and resource_name):
            return
        perm = self.get_permission(action_name, resource_name)
        if not perm:
            return
        roles = self.get_session.query(Role).filter(Role.permissions.contains(perm)).first()
        if roles:
            log.warning(c.LOGMSG_WAR_SEC_DEL_PERMVIEW.format(resource_name, action_name, roles))
            return
        try:
            # delete permission on resource
            self.get_session.delete(perm)
            self.get_session.commit()
            # if no more permission on permission view, delete permission
            if not self.get_session.query(Permission).filter_by(action=perm.action).all():
                self.delete_action(perm.action.name)
            log.info(c.LOGMSG_INF_SEC_DEL_PERMVIEW.format(action_name, resource_name))
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_PERMVIEW.format(str(e)))
            self.get_session.rollback()

    def add_role(self, name: str) -> Optional[Role]:
        role = self.get_role(name)
        if role is None:
            try:
                role = Role(name=name)
                self.get_session.add(role)
                self.get_session.commit()
                log.info(c.LOGMSG_INF_SEC_ADD_ROLE.format(name))

                # All roles must have access to the homepage.
                # website_perm = self.create_permission(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)
                # self.add_permission_to_role(role, website_perm)

            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_ROLE.format(str(e)))
                self.get_session.rollback()
        return role

    def get_role(self, name):
        return self.get_session.query(Role).filter_by(name=name).one_or_none()

    def get_all_roles(self):
        return self.get_session.query(Role).all()

    def update_role(self, role_id, name: str) -> Optional[Role]:
        role = self.get_session.query(Role).get(role_id)
        if not role:
            return
        try:
            role.name = name
            self.get_session.merge(role)
            self.get_session.commit()
            log.info(c.LOGMSG_INF_SEC_UPD_ROLE.format(role))
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_UPD_ROLE.format(str(e)))
            self.get_session.rollback()
            return

    def delete_role(self, role_name):
        """
        Delete the given Role

        :param role_name: the name of a role in the ab_role table
        """
        session = self.get_session
        role = session.query(Role).filter(Role.name == role_name).first()
        if role:
            # self.log.info("Deleting role '%s'", role_name)
            session.delete(role)
            session.commit()
        else:
            log.error(f"Role named '{role_name}' does not exist")

    def add_permission_to_role(self, role: Role, permission: Permission) -> None:
        """
        Add an existing permission pair to a role.

        :param role: The role about to get a new permission.
        :type role: Role
        :param permission: The permission pair to add to a role.
        :type permission: Permission
        :return: None
        :rtype: None
        """
        if permission and permission not in role.permissions:
            try:
                role.permissions.append(permission)
                self.get_session.merge(role)
                self.get_session.commit()
                log.info(c.LOGMSG_INF_SEC_ADD_PERMROLE.format(str(permission), role.name))
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_PERMROLE.format(str(e)))
                self.get_session.rollback()

    def remove_permission_from_role(self, role: Role, permission: Permission) -> None:
        """
        Remove a permission pair from a role.

        :param role: User role containing permissions.
        :type role: Role
        :param permission: Object representing resource-> action pair
        :type permission: Permission
        """
        if permission in role.permissions:
            try:
                role.permissions.remove(permission)
                self.get_session.merge(role)
                self.get_session.commit()
                log.info(c.LOGMSG_INF_SEC_DEL_PERMROLE.format(str(permission), role.name))
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_DEL_PERMROLE.format(str(e)))
                self.get_session.rollback()

    def add_user(
        self,
        username,
        first_name,
        last_name,
        email,
        role,
        password="",
        hashed_password="",
    ):
        """Generic function to create user"""
        try:
            user = User(
                first_name=first_name,
                last_name=last_name,
                username=username,
                email=email,
                active=True,
                roles=role if isinstance(role, list) else [role],
                password=hashed_password or generate_password_hash(password),
            )
            self.get_session.add(user)
            self.get_session.commit()
            log.info(c.LOGMSG_INF_SEC_ADD_USER.format(username))
            return user
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_ADD_USER.format(str(e)))
            self.get_session.rollback()
            return False

    def get_user_by_id(self, id):
        return self.get_session.query(User).get(id)

    def get_user_by_username(self, username, case_sensitive=False):
        try:
            if case_sensitive:
                return (
                    self.get_session.query(User)
                    .filter(func.lower(User.username) == func.lower(username))
                    .one_or_none()
                )
            return self.get_session.query(User).filter(User.username == username).one_or_none()
        except MultipleResultsFound:
            log.error(f"Multiple results found for user {username}")
            return None

    def get_user_by_email(self, email):
        try:
            return self.get_session.query(User).filter_by(email=email).one_or_none()
        except MultipleResultsFound:
            log.error(f"Multiple results found for user with email {email}")
            return None

    def get_all_users(self):
        return self.get_session.query(User).all()

    def update_user(self, user):
        try:
            self.get_session.merge(user)
            self.get_session.commit()
            log.info(c.LOGMSG_INF_SEC_UPD_USER.format(user))
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_UPD_USER.format(str(e)))
            self.get_session.rollback()
            return False

    def delete_user(self, user):
        user.roles = []  # Clear foreign keys on this user first.
        try:
            self.get_session.delete(user)
            self.get_session.commit()
            log.info(f"Deleted user. {user.username}")
            return True
        except Exception as e:
            log.error(f"Error updating user to database. {user.username}")
            self.get_session.rollback()
            return False

    def count_users(self):
        return self.get_session.query(func.count(User.id)).scalar()

    def add_register_user(self, username, first_name, last_name, email, password="", hashed_password=""):
        """
        Add a registration request for the user.

        :rtype : RegisterUser
        """
        register_user = RegisterUser(
            username=username,
            email=email,
            first_name=first_name,
            last_name=last_name,
            password=hashed_password or generate_password_hash(password),
            registration_hash=str(uuid.uuid1()),
        )
        try:
            self.get_session.add(register_user)
            self.get_session.commit()
            return register_user
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_ADD_REGISTER_USER.format(str(e)))
            self.appbuilder.get_session.rollback()
            return None

    def find_register_user(self, registration_hash):
        return (
            self.get_session.query(RegisterUser)
            .filter(RegisterUser.registration_hash == registration_hash)
            .scalar()
        )

    def del_register_user(self, register_user):
        """
        Deletes registration object from database

        :param register_user: RegisterUser object to delete
        """
        try:
            self.get_session.delete(register_user)
            self.get_session.commit()
            return True
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_REGISTER_USER.format(str(e)))
            self.get_session.rollback()
            return False
