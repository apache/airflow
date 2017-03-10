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
from future.utils import native

import flask_login
from flask_login import login_required, current_user, logout_user
from flask import flash
from wtforms import (
    Form, PasswordField, StringField)
from wtforms.validators import InputRequired

from ldap3 import Server, Connection, Tls, LEVEL, SUBTREE, BASE
import ssl

from flask import url_for, redirect

from airflow import settings
from airflow import models
from airflow import configuration
from airflow.configuration import AirflowConfigException

import logging

import traceback
import re

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() below
login_manager.login_message = None

LOG = logging.getLogger(__name__)


class AuthenticationError(Exception):
    pass


class LdapException(Exception):
    pass


def get_ldap_connection(dn=None, password=None):
    tls_configuration = None
    use_ssl = False
    try:
        cacert = configuration.get("ldap", "cacert")
        tls_configuration = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=cacert)
        use_ssl = True
    except:
        pass

    server = Server(configuration.get("ldap", "uri"), use_ssl, tls_configuration)
    conn = Connection(server, native(dn), native(password))

    if not conn.bind():
        LOG.error("Cannot bind to ldap server: %s ", conn.last_error)
        raise AuthenticationError("Cannot bind to ldap server")

    return conn


def group_contains_user(conn, search_base, group_filter, user_name_attr, username):
    search_filter = '(&({0}))'.format(group_filter)
    if not conn.search(native(search_base), native(search_filter),
                       attributes=[native(user_name_attr)]):
        LOG.warning("Unable to find group for %s %s", search_base, search_filter)
    else:
        for resp in conn.response:
            if (
                        'attributes' in resp and (
                            resp['attributes'].get(user_name_attr)[0] == username or
                            resp['attributes'].get(user_name_attr) == username
                )
            ):
                return True
    return False


def groups_user(conn, search_base, user_filter, user_name_att, username):
    search_filter = "(&({0})({1}={2}))".format(user_filter, user_name_att, username)
    res = conn.search(native(search_base), native(search_filter), attributes=[native("memberOf")])
    if not res:
        LOG.info("Cannot find user %s", username)
        raise AuthenticationError("Invalid username or password")

    if conn.response and "memberOf" not in conn.response[0]["attributes"]:
        LOG.warn("""Missing attribute "memberOf" when looked-up in Ldap database.
        The user does not seem to be a member of a group and therefore won't see any dag
        if the option filter_by_owner=True and owner_mode=ldapgroup are set""")
        return []

    user_groups = conn.response[0]["attributes"]["memberOf"]

    regex = re.compile("cn=([^,]*).*", re.IGNORECASE)
    groups_list = []
    try:
        groups_list = [regex.search(i).group(1) for i in user_groups]
    except IndexError:
        LOG.warning("Parsing error when retrieving the user's group(s)."
                    " Check if the user belongs to at least one group"
                    " or if the user's groups name do not contain special characters")

    return groups_list


class LdapUser(models.User):
    def __init__(self, user):
        self.user = user
        self.ldap_groups = []

        # Load and cache superuser and data_profiler settings.
        conn = get_ldap_connection(configuration.get("ldap", "bind_user"),
                                   configuration.get("ldap", "bind_password"))
        try:
            self.superuser = group_contains_user(conn,
                                                 configuration.get("ldap", "basedn"),
                                                 configuration.get("ldap", "superuser_filter"),
                                                 configuration.get("ldap", "user_name_attr"),
                                                 user.username)
        except AirflowConfigException:
            self.superuser = True
            LOG.debug("Missing configuration for superuser settings.  Skipping.")

        try:
            self.data_profiler = group_contains_user(conn,
                                                     configuration.get("ldap", "basedn"),
                                                     configuration.get("ldap", "data_profiler_filter"),
                                                     configuration.get("ldap", "user_name_attr"),
                                                     user.username)
        except AirflowConfigException:
            self.data_profiler = True
            LOG.debug("Missing configuration for dataprofiler settings. Skipping")

        # Load the ldap group(s) a user belongs to
        try:
            self.ldap_groups = groups_user(conn,
                                           configuration.get("ldap", "basedn"),
                                           configuration.get("ldap", "user_filter"),
                                           configuration.get("ldap", "user_name_attr"),
                                           user.username)
        except AirflowConfigException:
            LOG.debug("Missing configuration for ldap settings. Skipping")

    @staticmethod
    def try_login(username, password):
        conn = get_ldap_connection(configuration.get("ldap", "bind_user"),
                                   configuration.get("ldap", "bind_password"))

        search_filter = "(&({0})({1}={2}))".format(
            configuration.get("ldap", "user_filter"),
            configuration.get("ldap", "user_name_attr"),
            username
        )

        search_scopes = {
            "LEVEL": LEVEL,
            "SUBTREE": SUBTREE,
            "BASE": BASE
        }

        search_scope = LEVEL
        if configuration.has_option("ldap", "search_scope"):
            search_scope = SUBTREE if configuration.get("ldap", "search_scope") == "SUBTREE" else LEVEL

        # todo: BASE or ONELEVEL?

        res = conn.search(native(configuration.get("ldap", "basedn")),
                          native(search_filter),
                          search_scope=native(search_scope))

        # todo: use list or result?
        if not res:
            LOG.info("Cannot find user %s", username)
            raise AuthenticationError("Invalid username or password")

        entry = conn.response[0]

        conn.unbind()

        if 'dn' not in entry:
            # The search filter for the user did not return any values, so an
            # invalid user was used for credentials.
            raise AuthenticationError("Invalid username or password")

        try:
            conn = get_ldap_connection(entry['dn'], password)
        except KeyError as e:
            LOG.error("""
            Unable to parse LDAP structure. If you're using Active Directory and not specifying an OU, you must set search_scope=SUBTREE in airflow.cfg.
            %s
            """ % traceback.format_exc())
            raise LdapException("Could not parse LDAP structure. Try setting search_scope in airflow.cfg, or check logs")

        if not conn:
            LOG.info("Password incorrect for user %s", username)
            raise AuthenticationError("Invalid username or password")

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
        return self.data_profiler

    def is_superuser(self):
        '''Access all the things'''
        return self.superuser


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
    return LdapUser(user)


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
        LdapUser.try_login(username, password)
        LOG.info("User %s successfully authenticated", username)

        session = settings.Session()
        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                is_superuser=False)

        session.merge(user)
        session.commit()
        flask_login.login_user(LdapUser(user))
        session.commit()
        session.close()

        return redirect(request.args.get("next") or url_for("admin.index"))
    except (LdapException, AuthenticationError) as e:
        if type(e) == LdapException:
            flash(e, "error")
        else:
            flash("Incorrect login details")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)


class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
