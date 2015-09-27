'''
Override this file to handle your authenticating / login.

Copy and alter this file and put in your PYTHONPATH as airflow_login.py,
the new module will override this one.
'''

import flask_login
from flask_login import login_required, current_user, logout_user

from flask import url_for, redirect

from airflow import settings
from airflow import models

DEFAULT_USERNAME = 'airflow'

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
