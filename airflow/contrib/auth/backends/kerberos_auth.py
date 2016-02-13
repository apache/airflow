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
from airflow import configuration

import logging

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() bellow
login_manager.login_message = None


class AuthenticationError(Exception):
    pass


class KerberosUser(models.User):
    def __init__(self, user):
        self.user = user

    @staticmethod
    def authenticate(username, password):
        service_principal = "%s/%s" % (configuration.get('kerberos', 'principal'), utils.get_fqdn())
        realm = configuration.get("kerberos", "default_realm")
        user_principal = utils.principal_from_username(username)

        try:
            # this is pykerberos specific, verify = True is needed to prevent KDC spoofing
            if not kerberos.checkPassword(user_principal, password, service_principal, realm, True):
                raise AuthenticationError()
        except kerberos.KrbError as e:
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
    if not userid or userid == 'None':
        return None

    session = settings.Session()
    user = session.query(models.User).filter(models.User.id == int(userid)).first()
    session.expunge_all()
    session.commit()
    session.close()
    return KerberosUser(user)


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
        KerberosUser.authenticate(username, password)

        session = settings.Session()
        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                is_superuser=False)

        session.merge(user)
        session.commit()
        flask_login.login_user(KerberosUser(user))
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
