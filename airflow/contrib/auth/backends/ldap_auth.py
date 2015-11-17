import flask_login
from flask_login import login_required, current_user, logout_user
from flask import flash
from wtforms import (
    Form, PasswordField, StringField)
from wtforms.validators import InputRequired

from ldap3 import Server, Connection, Tls, LEVEL
import ssl

from flask import url_for, redirect

from airflow import settings
from airflow import models
from airflow import configuration

DEFAULT_USERNAME = 'airflow'

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() bellow
login_manager.login_message = None


class AuthenticationError(Exception):
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
    conn = Connection(server, dn, password)

    if not conn.bind():
        raise AuthenticationError("Username or password incorrect")

    return conn


class LdapUser(models.User):
    def __init__(self, user):
        self.user = user

    @staticmethod
    def try_login(username, password):
        conn = get_ldap_connection(configuration.get("ldap", "bind_user"), configuration.get("ldap", "bind_password"))

        search_filter = "(&({0})({1}={2}))".format(
            configuration.get("ldap", "user_filter"),
            configuration.get("ldap", "user_name_attr"),
            username
        )

        # todo: BASE or ONELEVEL?

        res = conn.search(configuration.get("ldap", "basedn"), search_filter, search_scope=LEVEL)

        # todo: use list or result?
        if not res:
            raise AuthenticationError("Invalid username or password")

        entry = conn.response[0]

        conn.unbind()
        conn = get_ldap_connection(entry['dn'], password)

        if not conn:
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

    def data_profiling(self):
        '''Provides access to data profiling tools'''
        return True

    def is_superuser(self):
        '''Access all the things'''
        return True


@login_manager.user_loader
def load_user(userid):
    session = settings.Session()
    user = session.query(models.User).filter(models.User.id == userid).first()
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

        session = settings.Session()
        user = session.query(models.User).filter(
            models.User.username == DEFAULT_USERNAME).first()

        if not user:
            user = models.User(
                username=DEFAULT_USERNAME,
                is_superuser=True)

        session.merge(user)
        session.commit()
        flask_login.login_user(LdapUser(user))
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