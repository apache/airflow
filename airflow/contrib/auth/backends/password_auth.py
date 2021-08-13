# -*- coding: utf-8 -*-
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
"""Password authentication backend"""
from __future__ import unicode_literals

import base64
import logging
from functools import wraps
from sys import version_info

from flask import flash, Response
from flask import url_for, redirect, make_response
from flask_bcrypt import generate_password_hash, check_password_hash

import flask_login
# noinspection PyUnresolvedReferences
# pylint: disable=unused-import
from flask_login import login_required, current_user, logout_user  # noqa: F401
# pylint: enable=unused-import

from wtforms import Form, PasswordField, StringField
from wtforms.validators import InputRequired

from sqlalchemy import Column, String
from sqlalchemy.ext.hybrid import hybrid_property

from airflow import models
from airflow.utils.db import provide_session, create_session

LOGIN_MANAGER = flask_login.LoginManager()
LOGIN_MANAGER.login_view = 'airflow.login'  # Calls login() below
LOGIN_MANAGER.login_message = None

PY3 = version_info[0] == 3
log = logging.getLogger(__name__)


CLIENT_AUTH = None


class AuthenticationError(Exception):
    """Error returned on authentication problems"""


# pylint: disable=no-member
# noinspection PyUnresolvedReferences
class PasswordUser(models.User):
    """Stores user with password"""
    _password = Column('password', String(255))

    def __init__(self, user):
        self.user = user

    @hybrid_property
    def password(self):
        """Returns password for the user"""
        return self._password

    @password.setter
    def password(self, plaintext):
        """Sets password for the user"""
        self._password = generate_password_hash(plaintext, 12)
        if PY3:
            self._password = str(self._password, 'utf-8')

    def authenticate(self, plaintext):
        """Authenticates user"""
        return check_password_hash(self._password, plaintext)

    @property
    def is_active(self):
        """Required by flask_login"""
        return True

    @property
    def is_authenticated(self):
        """Required by flask_login"""
        return True

    @property
    def is_anonymous(self):
        """Required by flask_login"""
        return False

    def get_id(self):
        """Returns the current user id as required by flask_login"""
        return str(self.id)

    # pylint: disable=no-self-use
    # noinspection PyMethodMayBeStatic
    def data_profiling(self):
        """Provides access to data profiling tools"""
        return True
    # pylint: enable=no-self-use

    def is_superuser(self):
        """Returns True if user is superuser"""
        return hasattr(self, 'user') and self.user.is_superuser()


# noinspection PyUnresolvedReferences
@LOGIN_MANAGER.user_loader
@provide_session
def load_user(userid, session=None):
    """Loads user from the database"""
    log.debug("Loading user %s", userid)
    if not userid or userid == 'None':
        return None

    user = session.query(models.User).filter(models.User.id == int(userid)).first()
    return PasswordUser(user)


def authenticate(session, username, password):
    """
    Authenticate a PasswordUser with the specified
    username/password.

    :param session: An active SQLAlchemy session
    :param username: The username
    :param password: The password

    :raise AuthenticationError: if an error occurred
    :return: a PasswordUser
    """
    if not username or not password:
        raise AuthenticationError()

    user = session.query(PasswordUser).filter(
        PasswordUser.username == username).first()

    if not user:
        raise AuthenticationError()

    if not user.authenticate(password):
        raise AuthenticationError()

    log.info("User %s successfully authenticated", username)
    return user


@provide_session
def login(self, request, session=None):
    """Logs the user in"""
    if current_user.is_authenticated:
        flash("You are already logged in")
        return redirect(url_for('admin.index'))

    username = None
    password = None

    form = LoginForm(request.form)

    if request.method == 'POST' and form.validate():
        username = request.form.get("username")
        password = request.form.get("password")

    try:
        user = authenticate(session, username, password)
        flask_login.login_user(user)

        return redirect(request.args.get("next") or url_for("admin.index"))
    except AuthenticationError:
        flash("Incorrect login details")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)


# pylint: disable=too-few-public-methods
class LoginForm(Form):
    """Form for the user"""
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
# pylint: enable=too-few-public-methods


def _unauthorized():
    """
    Indicate that authorization is required
    :return:
    """
    return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})


def _forbidden():
    return Response("Forbidden", 403)


def init_app(_):
    """Initializes backend"""


def requires_authentication(function):
    """Decorator for functions that require authentication"""
    @wraps(function)
    def decorated(*args, **kwargs):
        from flask import request

        header = request.headers.get("Authorization")
        if header:
            userpass = ''.join(header.split()[1:])
            username, password = base64.b64decode(userpass).decode("utf-8").split(":", 1)

            with create_session() as session:
                try:
                    authenticate(session, username, password)

                    response = function(*args, **kwargs)
                    response = make_response(response)
                    return response

                except AuthenticationError:
                    return _forbidden()

        return _unauthorized()
    return decorated
