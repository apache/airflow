from flask import current_app
from flask_login import login_user
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

class TheOverrideFabAuthManager(FabAuthManager):
    def create_token(self, headers: dict, body: dict):
        userinfo = body.get("userinfo")
        if not userinfo:
            raise ValueError("userinfo must be provided")

        app = current_app._get_current_object()
        with app.app_context():
            with app.test_request_context('/'):
                # Skip session rotation safely
                user = current_app.appbuilder.sm.auth_user_oauth(userinfo, rotate_session_id=False)
                login_user(user, remember=False)
                return user
