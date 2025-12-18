import pytest
from fab_auth_manager_override import TheOverrideFabAuthManager

def test_create_token_outside_request_context(monkeypatch):
    # Mock userinfo and user object
    userinfo = {"username": "test_user"}

    class DummyUser:
        pass

    # Patch auth_user_oauth to return dummy user
    monkeypatch.setattr(
        "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager.auth_user_oauth",
        lambda self, u, rotate_session_id=True: DummyUser(),
    )

    sm = TheOverrideFabAuthManager()
    user = sm.create_token({}, {"userinfo": userinfo})
    assert user is not None
