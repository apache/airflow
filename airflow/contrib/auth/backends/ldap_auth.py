import flask_login
from flask_login import login_required, current_user, logout_user
from flask import flash
from wtforms import (
    Form, PasswordField, StringField)
from wtforms.validators import InputRequired

import ldap

try:
    import ldap.sasl
except ImportError:
    pass

from flask import url_for, redirect

from airflow import settings
from airflow import models
from airflow.configuration import conf

DEFAULT_USERNAME = 'airflow'

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() bellow
login_manager.login_message = None

def get_ldap_connection():
    try:
        cacert = conf.get("ldap", "cacert")
        ldap.set_option(ldap.OPT_X_TLS_CACERTFILE, cacert)
        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_HARD)
    except:
        pass

    conn = ldap.initialize(conf.get("ldap", "uri"))
    return conn

class User(models.BaseUser):
    @staticmethod
    def try_login(username, password):
        conn = get_ldap_connection()
        if conf.get('core', 'security') == 'kerberos':
            sasl = ldap.sasl.gssapi()
            conn.sasl_interactive_bind_s("", sasl)
        else:
            conn.simple_bind_s(conf.get("ldap", "bind_user"), conf.get("ldap", "bind_password"))

        search_filter = "(&({0})({1}={2}))".format(
            conf.get("ldap", "user_filter"),
            conf.get("ldap", "user_name_attr"),
            username
        )

        # todo: BASE or ONELEVEL?
        res = conn.search_s(conf.get("ldap", "basedn"), ldap.SCOPE_ONELEVEL, search_filter)

        # todo: use list or result?
        if len(res) == 0:
            raise ldap.INVALID_CREDENTIALS

        dn, entry = res[0]
        dn = str(dn)

        conn.unbind()
        conn = get_ldap_connection()
        conn.simple_bind_s(dn, password)

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
        models.User.try_login(username, password)

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
    except ldap.INVALID_CREDENTIALS:
        flash("Incorrect login details")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)

class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
