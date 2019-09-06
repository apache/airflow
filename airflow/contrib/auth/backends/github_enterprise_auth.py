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

# Need to expose these downstream
# flake8: noqa: F401
from flask_login import current_user, logout_user, login_required, login_user

from flask import url_for, redirect, request

from flask_oauthlib.client import OAuth

from airflow import models
from airflow.configuration import AirflowConfigException, conf
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def get_config_param(param):
    return str(conf.get('github_enterprise', param))


class GHEUser(models.User):

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


class GHEAuthBackend:

    def __init__(self):
        self.ghe_host = get_config_param('host')
        self.login_manager = flask_login.LoginManager()
        self.login_manager.login_view = 'airflow.login'
        self.flask_app = None
        self.ghe_oauth = None
        self.api_url = None

    def ghe_api_route(self, leaf):
        if not self.api_url:
            self.api_url = (
                'https://api.github.com' if self.ghe_host == 'github.com'
                else '/'.join(['https:/',
                               self.ghe_host,
                               'api',
                               get_config_param('api_rev')])
            )
        return self.api_url + leaf

    def init_app(self, flask_app):
        self.flask_app = flask_app

        self.login_manager.init_app(self.flask_app)

        self.ghe_oauth = OAuth(self.flask_app).remote_app(
            'ghe',
            consumer_key=get_config_param('client_id'),
            consumer_secret=get_config_param('client_secret'),
            # need read:org to get team member list
            request_token_params={'scope': 'user:email,read:org'},
            base_url=self.ghe_host,
            request_token_url=None,
            access_token_method='POST',
            access_token_url=''.join(['https://',
                                      self.ghe_host,
                                      '/login/oauth/access_token']),
            authorize_url=''.join(['https://',
                                   self.ghe_host,
                                   '/login/oauth/authorize']))

        self.login_manager.user_loader(self.load_user)

        self.flask_app.add_url_rule(get_config_param('oauth_callback_route'),
                                    'ghe_oauth_callback',
                                    self.oauth_callback)

    def login(self, request):
        log.debug('Redirecting user to GHE login')
        return self.ghe_oauth.authorize(callback=url_for(
            'ghe_oauth_callback',
            _external=True),
            state=request.args.get('next') or request.referrer or None)

    def get_ghe_user_profile_info(self, ghe_token):
        resp = self.ghe_oauth.get(self.ghe_api_route('/user'),
                                  token=(ghe_token, ''))

        if not resp or resp.status != 200:
            raise AuthenticationError(
                'Failed to fetch user profile, status ({0})'.format(
                    resp.status if resp else 'None'))

        return resp.data['login'], resp.data['email']

    def ghe_team_check(self, username, ghe_token):
        try:
            # the response from ghe returns the id of the team as an integer
            try:
                allowed_teams = [int(team.strip())
                                 for team in
                                 get_config_param('allowed_teams').split(',')]
            except ValueError:
                # this is to deprecate using the string name for a team
                raise ValueError(
                    'it appears that you are using the string name for a team, '
                    'please use the id number instead')

        except AirflowConfigException:
            # No allowed teams defined, let anyone in GHE in.
            return True

        # https://developer.github.com/v3/orgs/teams/#list-user-teams
        resp = self.ghe_oauth.get(self.ghe_api_route('/user/teams'),
                                  token=(ghe_token, ''))

        if not resp or resp.status != 200:
            raise AuthenticationError(
                'Bad response from GHE ({0})'.format(
                    resp.status if resp else 'None'))

        for team in resp.data:
            # mylons: previously this line used to be if team['slug'] in teams
            # however, teams are part of organizations. organizations are unique,
            # but teams are not therefore 'slug' for a team is not necessarily unique.
            # use id instead
            if team['id'] in allowed_teams:
                return True

        log.debug('Denying access for user "%s", not a member of "%s"',
                  username,
                  str(allowed_teams))

        return False

    @provide_session
    def load_user(self, userid, session=None):
        if not userid or userid == 'None':
            return None

        user = session.query(models.User).filter(
            models.User.id == int(userid)).first()
        return GHEUser(user)

    @provide_session
    def oauth_callback(self, session=None):
        log.debug('GHE OAuth callback called')

        next_url = request.args.get('state') or url_for('admin.index')

        resp = self.ghe_oauth.authorized_response()

        try:
            if resp is None:
                raise AuthenticationError(
                    'Null response from GHE, denying access.'
                )

            ghe_token = resp['access_token']

            username, email = self.get_ghe_user_profile_info(ghe_token)

            if not self.ghe_team_check(username, ghe_token):
                return redirect(url_for('airflow.noaccess'))

        except AuthenticationError:
            log.exception('')
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
        login_user(GHEUser(user))
        session.commit()

        return redirect(next_url)


login_manager = GHEAuthBackend()


def login(self, request):
    return login_manager.login(request)
