import flask_login
from flask_login import login_required, current_user, logout_user
from flask import flash
from wtforms import (
    Form, PasswordField, StringField)
from wtforms.validators import InputRequired

# pykerberos should be used as it verifies the KDC, the "kerberos" module does not do so
# and make it possible to spoof the KDC
import kerberos
import airflow.security.utils as utils

from flask import url_for, redirect

from airflow import settings
from airflow import models
from airflow.configuration import conf

import logging

DEFAULT_USERNAME = 'airflow'

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() bellow
login_manager.login_message = None

class AuthenticationError(Exception):
    pass


class User(models.BaseUser):
    @staticmethod
    def authenticate(username, password):
        service_principal = "%s/%s" % (conf.get('kerberos', 'principal'), utils.get_fqdn())
        realm = conf.get("kerberos", "default_realm")
        user_principal = utils.principal_from_username(username)

        try:
            # this is pykerberos specific, verify = True is needed to prevent KDC spoofing
            if not kerberos.checkPassword(user_principal, password, service_principal, realm, True):
                raise AuthenticationError()
        except kerberos.KrbError, e:
            logging.error('Password validation for principal %s failed %s', user_principal, e)
            raise AuthenticationError(e)

        return

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

models.User = User  # hack!
del User


@login_manager.user_loader
def load_user(userid):
    session = settings.Session()
    user = session.query(models.User).filter(models.User.id == userid).first()
    session.expunge_all()
    session.commit()
    session.close()
    return user


def login(self, request):
    if current_user.is_authenticated():
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
        models.User.authenticate(username, password)

        session = settings.Session()
        user = session.query(models.User).filter(
            models.User.username == DEFAULT_USERNAME).first()

        if not user:
            user = models.User(
                username=DEFAULT_USERNAME,
                is_superuser=True)

        session.merge(user)
        session.commit()
        flask_login.login_user(user)
        session.commit()
        session.close()

        return redirect(request.args.get("next") or url_for("index"))
    except AuthenticationError:
        flash("Incorrect login details")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)

class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
