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

import okta
import flask_login
from flask_login import login_required, current_user, logout_user  # noqa: F401
from flask import flash
from wtforms import (Form, PasswordField, StringField)
from wtforms.validators import InputRequired

from flask import url_for, redirect, session

from airflow import models
from airflow import configuration

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() below
login_manager.login_message = 'Login using your Okta creds'


class OktaUser(object):
    def __init__(self, user):
        self.user = models.User(id=user['id'], username=user['username'], email=user['email'], superuser=True)

    @property
    def is_active(self):
        '''Required by flask_login'''
        return True

    @property
    def is_authenticated(self):
        '''Required by flask_login'''
        return True

    @property
    def is_anonymous(self):
        '''Required by flask_login'''
        return False

    def data_profiling(self):
        '''Provides access to data profiling tools'''
        return True

    def is_superuser(self):
        '''Access all the things'''
        return True

    def get_id(self):
        '''Returns the current user id as required by flask_login'''
        return self.user.get_id()

    @property
    def username(self):
        return self.user.username


@login_manager.user_loader
def load_user(userid):
    return OktaUser(session[userid])


def login(self, request):
    if current_user.is_authenticated:
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
        url = configuration.get("okta", "url")
        key = configuration.get("okta", "api_key")

        # Authenticate user
        usersClient = okta.AuthClient(url, key)
        res = usersClient.authenticate(username, password)

        # User authenticated, store in session and proceed to log them in
        user = dict()
        user['id'] = res.sessionToken
        user['email'] = username
        user['username'] = username

        flask_login.login_user(OktaUser(user))
        session[user['id']] = user

        return redirect(request.args.get("next") or url_for("admin.index"))

    except Exception as e:
        flash("Incorrect username or password")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)


class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
