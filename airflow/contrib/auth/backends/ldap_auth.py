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
from future.utils import native

import flask_login
from flask_login import login_required, current_user, logout_user  # noqa: F401
from flask import flash
from wtforms import Form, PasswordField, StringField
from wtforms.validators import InputRequired

from ldap3 import Server, Connection, Tls, LEVEL, SUBTREE
import ssl

from flask import url_for, redirect

from airflow import models
from airflow import configuration
from airflow.configuration import AirflowConfigException
from airflow.utils.db import provide_session

import traceback
import re

from airflow.utils.log.logging_mixin import LoggingMixin

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() below
login_manager.login_message = None

log = LoggingMixin().log


class AuthenticationError(Exception):
    pass


class LdapException(Exception):
    pass


def get_ldap_connection(dn=None, password=None):
    tls_configuration = None
    use_ssl = False
    try:
        cacert = configuration.conf.get("ldap", "cacert")
        tls_configuration = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=cacert)
        use_ssl = True
    except Exception:
        pass

    server = Server(configuration.conf.get("ldap", "uri"), use_ssl, tls_configuration)
    conn = Connection(server, native(dn), native(password))

    if not conn.bind():
        log.error("Cannot bind to ldap server: %s ", conn.last_error)
        raise AuthenticationError("Cannot bind to ldap server")

    return conn


def group_contains_user(conn, search_base, group_filter, user_name_attr, username):
    search_filter = '(&({0}))'.format(group_filter)

    if not conn.search(native(search_base), native(search_filter),
                       attributes=[native(user_name_attr)]):
        log.warning("Unable to find group for %s %s", search_base, search_filter)
    else:
        for entry in conn.entries:
            if username.lower() in map(lambda attr: attr.lower(),
                                       getattr(entry, user_name_attr).values):
                return True

    return False


def groups_user(conn, search_base, user_filter, user_name_att, username):
    search_filter = "(&({0})({1}={2}))".format(user_filter, user_name_att, username)
    try:
        memberof_attr = configuration.conf.get("ldap", "group_member_attr")
    except Exception:
        memberof_attr = "memberOf"
    res = conn.search(native(search_base), native(search_filter),
                      attributes=[native(memberof_attr)])
    if not res:
        log.info("Cannot find user %s", username)
        raise AuthenticationError("Invalid username or password")

    if conn.response and memberof_attr not in conn.response[0]["attributes"]:
        log.warning("""Missing attribute "%s" when looked-up in Ldap database.
        The user does not seem to be a member of a group and therefore won't see any dag
        if the option filter_by_owner=True and owner_mode=ldapgroup are set""",
                    memberof_attr)
        return []

    user_groups = conn.response[0]["attributes"][memberof_attr]

    regex = re.compile("cn=([^,]*).*", re.IGNORECASE)
    groups_list = []
    try:
        groups_list = [regex.search(i).group(1) for i in user_groups]
    except IndexError:
        log.warning("Parsing error when retrieving the user's group(s)."
                    " Check if the user belongs to at least one group"
                    " or if the user's groups name do not contain special characters")

    return groups_list


class LdapUser(models.User):
    def __init__(self, user):
        self.user = user
        self.ldap_groups = []

        # Load and cache superuser and data_profiler settings.
        conn = get_ldap_connection(configuration.conf.get("ldap", "bind_user"),
                                   configuration.conf.get("ldap", "bind_password"))

        superuser_filter = None
        data_profiler_filter = None
        try:
            superuser_filter = configuration.conf.get("ldap", "superuser_filter")
        except AirflowConfigException:
            pass

        if not superuser_filter:
            self.superuser = True
            log.debug("Missing configuration for superuser settings or empty. Skipping.")
        else:
            self.superuser = group_contains_user(conn,
                                                 configuration.conf.get("ldap", "basedn"),
                                                 superuser_filter,
                                                 configuration.conf.get("ldap",
                                                                        "user_name_attr"),
                                                 user.username)

        try:
            data_profiler_filter = configuration.conf.get("ldap", "data_profiler_filter")
        except AirflowConfigException:
            pass

        if not data_profiler_filter:
            self.data_profiler = True
            log.debug("Missing configuration for data profiler settings or empty. "
                      "Skipping.")
        else:
            self.data_profiler = group_contains_user(
                conn,
                configuration.conf.get("ldap", "basedn"),
                data_profiler_filter,
                configuration.conf.get("ldap",
                                       "user_name_attr"),
                user.username
            )

        # Load the ldap group(s) a user belongs to
        try:
            self.ldap_groups = groups_user(
                conn,
                configuration.conf.get("ldap", "basedn"),
                configuration.conf.get("ldap", "user_filter"),
                configuration.conf.get("ldap", "user_name_attr"),
                user.username
            )
        except AirflowConfigException:
            log.debug("Missing configuration for ldap settings. Skipping")

    @staticmethod
    def try_login(username, password):
        conn = get_ldap_connection(configuration.conf.get("ldap", "bind_user"),
                                   configuration.conf.get("ldap", "bind_password"))

        search_filter = "(&({0})({1}={2}))".format(
            configuration.conf.get("ldap", "user_filter"),
            configuration.conf.get("ldap", "user_name_attr"),
            username
        )

        search_scope = LEVEL
        if configuration.conf.has_option("ldap", "search_scope"):
            if configuration.conf.get("ldap", "search_scope") == "SUBTREE":
                search_scope = SUBTREE
            else:
                search_scope = LEVEL

        # todo: BASE or ONELEVEL?

        res = conn.search(native(configuration.conf.get("ldap", "basedn")),
                          native(search_filter),
                          search_scope=native(search_scope))

        # todo: use list or result?
        if not res:
            log.info("Cannot find user %s", username)
            raise AuthenticationError("Invalid username or password")

        entry = conn.response[0]

        conn.unbind()

        if 'dn' not in entry:
            # The search filter for the user did not return any values, so an
            # invalid user was used for credentials.
            raise AuthenticationError("Invalid username or password")

        try:
            conn = get_ldap_connection(entry['dn'], password)
        except KeyError:
            log.error("""
            Unable to parse LDAP structure. If you're using Active Directory
            and not specifying an OU, you must set search_scope=SUBTREE in airflow.cfg.
            %s
            """ % traceback.format_exc())
            raise LdapException(
                "Could not parse LDAP structure. "
                "Try setting search_scope in airflow.cfg, or check logs"
            )

        if not conn:
            log.info("Password incorrect for user %s", username)
            raise AuthenticationError("Invalid username or password")

    def is_active(self):
        """Required by flask_login"""
        return True

    def is_authenticated(self):
        """Required by flask_login"""
        return True

    def is_anonymous(self):
        """Required by flask_login"""
        return False

    def get_id(self):
        """Returns the current user id as required by flask_login"""
        return self.user.get_id()

    def data_profiling(self):
        """Provides access to data profiling tools"""
        return self.data_profiler

    def is_superuser(self):
        """Access all the things"""
        return self.superuser


@login_manager.user_loader
@provide_session
def load_user(userid, session=None):
    log.debug("Loading user %s", userid)
    if not userid or userid == 'None':
        return None

    user = session.query(models.User).filter(models.User.id == int(userid)).first()
    return LdapUser(user)


@provide_session
def login(self, request, session=None):
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
        log.info("User %s successfully authenticated", username)

        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                is_superuser=False)
            session.add(user)

        session.commit()
        session.merge(user)
        flask_login.login_user(LdapUser(user))
        session.commit()

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
