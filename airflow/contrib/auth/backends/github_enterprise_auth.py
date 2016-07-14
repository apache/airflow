# Copyright 2015 Matthew Pelland (matt@pelland.io)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging

import flask_login

# Need to expose these downstream
# pylint: disable=unused-import
from flask_login import (current_user,
                         logout_user,
                         login_required)
# pylint: enable=unused-import

from flask import url_for, redirect, request

from flask_oauthlib.client import OAuth

from airflow import models, configuration, settings
from airflow.configuration import AirflowConfigException

_log = logging.getLogger(__name__)


def get_config_param(param):
    return str(configuration.get('github_enterprise', param))


class GHEUser(models.User):

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

    def get_id(self):
        '''Returns the current user id as required by flask_login'''
        return self.user.get_id()

    def data_profiling(self):
        '''Provides access to data profiling tools'''
        return True

    def is_superuser(self):
        '''Access all the things'''
        return True


class AuthenticationError(Exception):
    pass


class GHEAuthBackend(object):

    def __init__(self):
        self.ghe_host = get_config_param('host')
        self.login_manager = flask_login.LoginManager()
        self.login_manager.login_view = 'airflow.login'
        self.flask_app = None
        self.ghe_oauth = None
        self.api_rev = None

    def ghe_api_route(self, leaf):
        if not self.api_rev:
            self.api_rev = get_config_param('api_rev')

        return '/'.join(['https:/',
                         self.ghe_host,
                         'api',
                         self.api_rev,
                         leaf.strip('/')])

    def init_app(self, flask_app):
        self.flask_app = flask_app

        self.login_manager.init_app(self.flask_app)

        self.ghe_oauth = OAuth(self.flask_app).remote_app(
            'ghe',
            consumer_key=get_config_param('client_id'),
            consumer_secret=get_config_param('client_secret'),
            # need read:org to get team member list
            request_token_params={'scope': 'user,read:org'},
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
        _log.debug('Redirecting user to GHE login')
        return self.ghe_oauth.authorize(callback=url_for(
            'ghe_oauth_callback',
            _external=True,
            next=request.args.get('next') or request.referrer or None))

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
            teams = [team.strip()
                     for team in
                     get_config_param('allowed_teams').split(',')]
        except AirflowConfigException:
            # No allowed teams defined, let anyone in GHE in.
            return True

        resp = self.ghe_oauth.get(self.ghe_api_route('/user/teams'),
                                  token=(ghe_token, ''))

        if not resp or resp.status != 200:
            raise AuthenticationError(
                'Bad response from GHE ({0})'.format(
                    resp.status if resp else 'None'))

        for team in resp.data:
            # team json object has a slug cased team name field aptly named
            # 'slug'
            if team['slug'] in teams:
                return True

        _log.debug('Denying access for user "%s", not a member of "%s"',
                   username,
                   str(teams))

        return False

    def load_user(self, userid):
        if not userid or userid == 'None':
            return None

        session = settings.Session()
        user = session.query(models.User).filter(
            models.User.id == int(userid)).first()
        session.expunge_all()
        session.commit()
        session.close()
        return GHEUser(user)

    def oauth_callback(self):
        _log.debug('GHE OAuth callback called')

        next_url = request.args.get('next') or url_for('admin.index')

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
            _log.exception('')
            return redirect(url_for('airflow.noaccess'))

        session = settings.Session()

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
        session.close()

        return redirect(next_url)

login_manager = GHEAuthBackend()


def login(self, request):
    return login_manager.login(request)
