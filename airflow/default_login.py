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
#
'''
Override this file to handle your authenticating / login.

Copy and alter this file and put in your PYTHONPATH as airflow_login.py,
the new module will override this one.
'''

import flask_login
from flask_login import login_required, current_user, logout_user

from flask import url_for, redirect

from airflow import settings
from airflow import models

DEFAULT_USERNAME = 'airflow'

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() below
login_manager.login_message = None


class DefaultUser(object):
    def __init__(self, user):
        self.user = user

    def is_active(self):
        '''Required by flask_login'''
        return True

    def is_authenticated(self):
        '''Required by flask_login'''
        return True

    def is_anonymous(self):
        '''Required by flask_login'''
        return False

    def data_profiling(self):
        '''Provides access to data profiling tools'''
        return True

    def is_superuser(self):
        '''Access all the things'''
        return True

#models.User = User  # hack!
#del User


@login_manager.user_loader
def load_user(userid):
    session = settings.Session()
    user = session.query(models.User).filter(models.User.id == userid).first()
    session.expunge_all()
    session.commit()
    session.close()
    return DefaultUser(user)


def login(self, request):
    session = settings.Session()
    user = session.query(models.User).filter(
        models.User.username == DEFAULT_USERNAME).first()
    if not user:
        user = models.User(
            username=DEFAULT_USERNAME,
            is_superuser=True)
    session.merge(user)
    session.commit()
    flask_login.login_user(DefaultUser(user))
    session.commit()
    session.close()
    return redirect(request.args.get("next") or url_for("index"))
