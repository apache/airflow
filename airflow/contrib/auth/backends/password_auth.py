from __future__ import unicode_literals

from sys import version_info

import flask_login
from flask_login import login_required, current_user, logout_user
from flask import flash
from wtforms import (
    Form, PasswordField, StringField)
from wtforms.validators import InputRequired

from flask import url_for, redirect
from flask.ext.bcrypt import generate_password_hash, check_password_hash

from sqlalchemy import (
    Column, String, DateTime)
from sqlalchemy.ext.hybrid import hybrid_property

from airflow import settings
from airflow import models
from airflow import configuration

import logging

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() bellow
login_manager.login_message = None

LOG = logging.getLogger(__name__)
PY3 = version_info[0] == 3

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
    def _set_password(self, plaintext):
        self._password = generate_password_hash(plaintext, 12)
        if PY3:
            self._password = str(self._password, 'utf-8')

    def authenticate(self, plaintext):
        return check_password_hash(self._password, plaintext)

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
        return str(self.id)

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
    return PasswordUser(user)


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
        user = session.query(PasswordUser).filter(
            PasswordUser.username == username).first()

        if not user:
            session.close()
            raise AuthenticationError()

        if not user.authenticate(password):
            session.close()
            raise AuthenticationError()
        LOG.info("User %s successfully authenticated", username)

        flask_login.login_user(user)
        session.commit()
        session.close()

        return redirect(request.args.get("next") or url_for("admin.index"))
    except AuthenticationError:
        flash("Incorrect login details")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)


class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
