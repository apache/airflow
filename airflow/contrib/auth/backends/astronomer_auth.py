# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import unicode_literals

from sys import version_info

import flask_login
from flask_login import login_required, current_user, logout_user
from flask import flash
from wtforms import (
    Form, PasswordField, StringField)
from wtforms.validators import InputRequired

from flask import url_for, redirect
from flask_bcrypt import generate_password_hash, check_password_hash

from sqlalchemy import (
    Column, String, DateTime)
from sqlalchemy.ext.hybrid import hybrid_property

from airflow import settings
from airflow import models
from airflow import configuration

import os
import logging
import requests

import json

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() below
login_manager.login_message = None

LOG = logging.getLogger(__name__)
PY3 = version_info[0] == 3


class AuthenticationError(Exception):
    pass


'''
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(ID_LEN), unique=True)
    email = Column(String(500))
    superuser = False

    def __repr__(self):
        return self.username

    def get_id(self):
        return str(self.id)

    def is_superuser(self):
        return self.superuser
'''

class AstronomerUser(models.User):
    def __init__(self, user):
        self.user = user

    @staticmethod
    def authenticate(username, password, org_id):
        hostname = configuration.get("astronomer-api", "hostname")
        port = configuration.get("astronomer-api", "port")
        protocol = configuration.get("astronomer-api", "protocol")

        base = '{}://{}:{}'.format(protocol, hostname, port)
        endpoint = requests.compat.urljoin(base, 'v1')


        try:
            request_data = {
                'query': '''
                mutation Login($username: String!, $password: String!, $orgId: String) {
                  createToken(username: $username, password: $password, orgId: $orgId) {
                    success,
                    message,
                    token
                  }
                }
                ''',
                'variables': {'username': username, 'password': password, 'orgId': org_id}
            }

            headers = {'Accept': 'application/json',
                       'Content-Type': 'application/json'}

            post_data = json.dumps(request_data).encode("UTF-8")

            response = requests.post(endpoint, data=post_data, headers=headers)

            data = response.json()

            data = data['data']['createToken']

            if data and 'success' in data and not data['success']:
                raise Exception(data['message'])

            if not data or 'success' not in data:
                return False

            return True
        except requests.exceptions.RequestException as e:
            LOG.info("Problem communicating with API: %s", str(e))
            return False

    def is_active(self):
        '''Required by flask_login'''
        return True

    def is_authenticated(self):
        '''Required by flask_login'''
        return True

    def is_anonymous(self):
        '''Required by flask_login'''
        return False

    def get_id(self):
        '''Returns the current user id as required by flask_login'''
        return self.user.get_id()

    def data_profiling(self):
        '''Provides access to data profiling tools'''
        return True

    def is_superuser(self):
        '''Access all the things'''
        return True


@login_manager.user_loader
def load_user(userid):
    LOG.debug("Loading user %s", userid)
    if not userid or userid == 'None':
        return None

    session = settings.Session()
    user = session.query(models.User).filter(models.User.id == int(userid)).first()
    session.expunge_all()
    session.commit()
    session.close()
    return AstronomerUser(user)


def login(self, request):
    if current_user.is_authenticated():
        flash("You are already logged in")
        return redirect(url_for('admin.index'))

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

        session = settings.Session()

        if 'ASTRONOMER_ORG_ID' not in os.environ:
            raise AuthenticationError("Unknown organization, server not configured correctly")
        astro_org_id = os.environ['ASTRONOMER_ORG_ID']

        if not AstronomerUser.authenticate(username, password, astro_org_id):
            session.close()
            raise AuthenticationError("Incorrect login details")

        LOG.info("User %s successfully authenticated", username)

        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                is_superuser=False)

            # Add to database immediately, then query for the full object
            session.merge(user)
            session.commit()
            user = session.query(models.User).filter(
                models.User.username == username).first()

        session.merge(user)
        session.commit()
        flask_login.login_user(AstronomerUser(user))
        session.commit()
        session.close()

        return redirect(request.args.get("next") or url_for("admin.index"))
    except AuthenticationError as e:
        flash(str(e))
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)
    except Exception as e:
        flash("Authentication Error: %s" % str(e))
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)

class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
