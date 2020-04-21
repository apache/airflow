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
import flask_login
from flask import redirect, request, url_for
# Need to expose these downstream
# flake8: noqa: F401
from flask_login import current_user, login_required, login_user, logout_user
from flask_oauthlib.client import OAuth

from airflow import models
from airflow.configuration import conf
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def get_config_param(param):
    return str(conf.get('google', param))


class GoogleUser(models.User):

    def __init__(self, user):
        self.user = user

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


class AuthenticationError(Exception):
    pass


class GoogleAuthBackend:

    def __init__(self):
        # self.google_host = get_config_param('host')
        self.login_manager = flask_login.LoginManager()
        self.login_manager.login_view = 'airflow.login'
        self.flask_app = None
        self.google_oauth = None
        self.api_rev = None

    def init_app(self, flask_app):
        self.flask_app = flask_app

        self.login_manager.init_app(self.flask_app)

        self.google_oauth = OAuth(self.flask_app).remote_app(
            'google',
            consumer_key=get_config_param('client_id'),
            consumer_secret=get_config_param('client_secret'),
            request_token_params={'scope': [
                'https://www.googleapis.com/auth/userinfo.profile',
                'https://www.googleapis.com/auth/userinfo.email']},
            base_url='https://www.google.com/accounts/',
            request_token_url=None,
            access_token_method='POST',
            access_token_url='https://accounts.google.com/o/oauth2/token',
            authorize_url='https://accounts.google.com/o/oauth2/auth')

        self.login_manager.user_loader(self.load_user)

        self.flask_app.add_url_rule(get_config_param('oauth_callback_route'),
                                    'google_oauth_callback',
                                    self.oauth_callback)

    def login(self, request):
        log.debug('Redirecting user to Google login')
        return self.google_oauth.authorize(callback=url_for(
            'google_oauth_callback',
            _external=True),
            state=request.args.get('next') or request.referrer or None)

    def get_google_user_profile_info(self, google_token):
        resp = self.google_oauth.get(
            'https://www.googleapis.com/oauth2/v1/userinfo',
            token=(google_token, ''))

        if not resp or resp.status != 200:
            raise AuthenticationError(
                'Failed to fetch user profile, status ({0})'.format(
                    resp.status if resp else 'None'))

        return resp.data['name'], resp.data['email']

    def domain_check(self, email):
        domain = email.split('@')[1]
        domains = get_config_param('domain').split(',')
        if domain in domains:
            return True
        return False

    @provide_session
    def load_user(self, userid, session=None):
        if not userid or userid == 'None':
            return None

        user = session.query(models.User).filter(
            models.User.id == int(userid)).first()
        return GoogleUser(user)

    @provide_session
    def oauth_callback(self, session=None):
        log.debug('Google OAuth callback called')

        next_url = request.args.get('state') or url_for('admin.index')

        resp = self.google_oauth.authorized_response()

        try:
            if resp is None:
                raise AuthenticationError(
                    'Null response from Google, denying access.'
                )

            google_token = resp['access_token']

            username, email = self.get_google_user_profile_info(google_token)

            if not self.domain_check(email):
                return redirect(url_for('airflow.noaccess'))

        except AuthenticationError:
            return redirect(url_for('airflow.noaccess'))

        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                email=email,
                is_superuser=False)

        session.merge(user)
        session.commit()
        login_user(GoogleUser(user))
        session.commit()

        return redirect(next_url)


login_manager = GoogleAuthBackend()


def login(self, request):
    return login_manager.login(request)
