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

from __future__ import unicode_literals

from sys import version_info

import base64
import flask_login
from flask_login import login_required, current_user, logout_user  # noqa: F401
from flask import flash, Response
from wtforms import Form, PasswordField, StringField
from wtforms.validators import InputRequired
from functools import wraps

from flask import url_for, redirect, make_response
from flask_bcrypt import generate_password_hash, check_password_hash

from sqlalchemy import Column, String
from sqlalchemy.ext.hybrid import hybrid_property

from airflow import settings
from airflow import models
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() below
login_manager.login_message = None

log = LoggingMixin().log
PY3 = version_info[0] == 3


client_auth = None


class AuthenticationError(Exception):
    pass


class PasswordUser(models.User):
    _password = Column('password', String(255))

    def __init__(self, user):
        self.user = user

    @hybrid_property
    def password(self):
        return self._password

    @password.setter
    def password(self, plaintext):
        self._password = generate_password_hash(plaintext, 12)
        if PY3:
            self._password = str(self._password, 'utf-8')

    def authenticate(self, plaintext):
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

    def data_profiling(self):
        """Provides access to data profiling tools"""
        return True

    def is_superuser(self):
        return hasattr(self, 'user') and self.user.is_superuser()


@login_manager.user_loader
@provide_session
def load_user(userid, session=None):
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
    finally:
        session.commit()
        session.close()


class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])


def _unauthorized():
    """
    Indicate that authorization is required
    :return:
    """
    return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})


def _forbidden():
    return Response("Forbidden", 403)


def init_app(app):
    pass


def requires_authentication(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        from flask import request

        header = request.headers.get("Authorization")
        if header:
            userpass = ''.join(header.split()[1:])
            username, password = base64.b64decode(userpass).decode("utf-8").split(":", 1)

            session = settings.Session()
            try:
                authenticate(session, username, password)

                response = function(*args, **kwargs)
                response = make_response(response)
                return response

            except AuthenticationError:
                return _forbidden()

            finally:
                session.commit()
                session.close()
        return _unauthorized()
    return decorated
