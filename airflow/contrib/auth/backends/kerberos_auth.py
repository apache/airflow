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
"""Kerberos authentication module"""
import logging

import flask_login
# pykerberos should be used as it verifies the KDC, the "kerberos" module does not do so
# and make it possible to spoof the KDC
import kerberos
from flask import flash, redirect, url_for
from flask_login import current_user
from wtforms import Form, PasswordField, StringField
from wtforms.validators import InputRequired

from airflow import models
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.security import utils
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session

# pylint: disable=c-extension-no-member
LOGIN_MANAGER = flask_login.LoginManager()
LOGIN_MANAGER.login_view = 'airflow.login'  # Calls login() below
LOGIN_MANAGER.login_message = None


class AuthenticationError(Exception):
    """Error raised when authentication error occurs"""


class KerberosUser(models.User, LoggingMixin):
    """User authenticated with Kerberos"""
    def __init__(self, user):
        self.user = user

    @staticmethod
    def authenticate(username, password):
        service_principal = "%s/%s" % (
            conf.get('kerberos', 'principal'),
            utils.get_fqdn()
        )
        realm = conf.get("kerberos", "default_realm")

        try:
            user_realm = conf.get("security", "default_realm")
        except AirflowConfigException:
            user_realm = realm

        user_principal = utils.principal_from_username(username, user_realm)

        try:
            # this is pykerberos specific, verify = True is needed to prevent KDC spoofing
            if not kerberos.checkPassword(user_principal,
                                          password,
                                          service_principal, realm, True):
                raise AuthenticationError()
        except kerberos.KrbError as e:
            logging.error(
                'Password validation for user '
                '%s in realm %s failed %s', user_principal, realm, e)
            raise AuthenticationError(e)

        return

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
        return self.user.get_id()

    def data_profiling(self):
        """Provides access to data profiling tools"""
        return True

    def is_superuser(self):
        """Access all the things"""
        return True


@LOGIN_MANAGER.user_loader
@provide_session
def load_user(userid, session=None):
    if not userid or userid == 'None':
        return None

    user = session.query(models.User).filter(models.User.id == int(userid)).first()
    return KerberosUser(user)


@provide_session
def login(self, request, session=None):
    if current_user.is_authenticated:
        flash("You are already logged in")
        return redirect(url_for('index'))

    username = None
    password = None

    form = LoginForm(request.form)

    if request.method == 'POST' and form.validate():
        username = request.form.get("username")
        password = request.form.get("password")

    if not username or not password:
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)

    try:
        KerberosUser.authenticate(username, password)

        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                is_superuser=False)

        session.merge(user)
        session.commit()
        flask_login.login_user(KerberosUser(user))
        session.commit()

        return redirect(request.args.get("next") or url_for("admin.index"))
    except AuthenticationError:
        flash("Incorrect login details")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)


class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
