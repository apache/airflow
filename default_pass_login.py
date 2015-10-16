'''
Override this file to handle your authenticating / login.

Copy and alter this file and put in your PYTHONPATH as airflow_login.py,
the new module will override this one.
'''

import flask_login

from flask_login import login_required, current_user, logout_user

from flask import url_for, redirect,Flask
from flask.ext.bcrypt import Bcrypt
from airflow import settings
from airflow import models

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() bellow
login_manager.login_message = None


class User(models.BaseUser):

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
    bcrypt = Bcrypt(None)
    session = settings.Session()
    user = session.query(models.User).filter(
        models.User.username == request.form.get('username')).first()
    if not user:
        return False
    session.merge(user)
    session.commit()
    if bcrypt.check_password_hash(user.password, request.form.get('password')):
        flask_login.login_user(user)
        session.commit()
        session.close()
        return True
