from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy import and_, func, literal
from sqlalchemy.orm.exc import MultipleResultsFound
import logging

from airflow.www.fab_security.sqla.models import (
    Action,
    Permission,
    RegisterUser,
    Resource,
    Role,
    User,
    assoc_permission_role,
)

from flask_appbuilder.const import (
    LOGMSG_WAR_SEC_LOGIN_FAILED,
)

log = logging.getLogger(__name__)


class DatabaseBackend:

    user_model = User

    def __init__(self, appbuilder):
        self.appbuilder = appbuilder
        # app = self.appbuilder.get_app


    @property
    def get_session(self):
        return self.appbuilder.get_session

    @property
    def auth_username_ci(self):
        """Gets the auth username for CI"""
        return self.appbuilder.get_app.config.get("AUTH_USERNAME_CI", True)

    def find_user(self, username=None, email=None):
        """Finds user by username or email"""
        if username:
            try:
                if self.auth_username_ci:
                    return (
                        self.get_session.query(self.user_model)
                        .filter(func.lower(self.user_model.username) == func.lower(username))
                        .one_or_none()
                    )
                else:
                    return (
                        self.get_session.query(self.user_model)
                        .filter(self.user_model.username == username)
                        .one_or_none()
                    )
            except MultipleResultsFound:
                log.error(f"Multiple results found for user {username}")
                return None
        elif email:
            try:
                return self.get_session.query(self.user_model).filter_by(email=email).one_or_none()
            except MultipleResultsFound:
                log.error(f"Multiple results found for user with email {email}")
                return None

    def auth_user_db(self, username, password):
        """
        Method for authenticating user, auth db style

        :param username:
            The username or registered email address
        :param password:
            The password, will be tested against hashed password on db
        """
        if username is None or username == "":
            return None
        user = self.find_user(username=username)
        if user is None:
            user = self.find_user(email=username)
        if user is None or (not user.is_active):
            # Balance failure and success
            check_password_hash(
                "pbkdf2:sha256:150000$Z3t6fmj2$22da622d94a1f8118"
                "c0976a03d2f18f680bfff877c9a965db9eedc51bc0be87c",
                "password",
            )
            log.info(LOGMSG_WAR_SEC_LOGIN_FAILED.format(username))
            return None
        elif check_password_hash(user.password, password):
            self.update_user_auth_stat(user, True)
            return user
        else:
            self.update_user_auth_stat(user, False)
            log.info(LOGMSG_WAR_SEC_LOGIN_FAILED.format(username))
            return None