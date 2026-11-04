# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import asyncio
import os
import re
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowNotFoundException
from airflow.providers.modal.exceptions import ModalConnectionError
from airflow.providers.modal.hooks.modal import ModalHook

modal = pytest.importorskip("modal")

TOKEN_ID = "ak-test-id"
TOKEN_SECRET = "as-test-secret"


def _missing_connection():
    """Stand-in for ``BaseHook.get_connection`` when the connection id does not exist anywhere."""
    return mock.Mock(spec=ModalHook.get_connection, side_effect=AirflowNotFoundException("nope"))


@pytest.fixture
def modal_client_cls():
    """Patch ``modal.Client`` so no network call is made; yields the patched class."""
    with mock.patch("airflow.providers.modal.hooks.modal.modal.Client", autospec=True) as client_cls:
        yield client_cls


class TestCredentialResolution:
    def test_connection_with_both_tokens_builds_client_from_credentials(
        self, create_connection_without_db, modal_client_cls
    ):
        create_connection_without_db(
            Connection(conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET)
        )

        client = ModalHook(modal_conn_id="modal_full").get_conn()

        modal_client_cls.from_credentials.assert_called_once_with(TOKEN_ID, TOKEN_SECRET)
        modal_client_cls.from_env.assert_not_called()
        assert client is modal_client_cls.from_credentials.return_value

    def test_connection_with_no_tokens_defers_to_sdk_resolution(
        self, create_connection_without_db, modal_client_cls
    ):
        create_connection_without_db(Connection(conn_id="modal_ambient", conn_type="modal"))

        hook = ModalHook(modal_conn_id="modal_ambient")

        assert hook.credentials is None
        hook.get_conn()
        modal_client_cls.from_env.assert_called_once_with()
        modal_client_cls.from_credentials.assert_not_called()

    @pytest.mark.parametrize(
        ("login", "password", "missing"),
        [
            pytest.param(TOKEN_ID, None, "Token Secret (password)", id="secret-missing"),
            pytest.param(None, TOKEN_SECRET, "Token ID (login)", id="id-missing"),
            pytest.param(TOKEN_ID, "", "Token Secret (password)", id="secret-empty-string"),
        ],
    )
    def test_half_filled_connection_raises_instead_of_falling_back(
        self, create_connection_without_db, modal_client_cls, login, password, missing
    ):
        create_connection_without_db(
            Connection(conn_id="modal_half", conn_type="modal", login=login, password=password)
        )

        with pytest.raises(ModalConnectionError, match=re.escape(missing)):
            ModalHook(modal_conn_id="modal_half").get_conn()

        modal_client_cls.from_env.assert_not_called()
        modal_client_cls.from_credentials.assert_not_called()

    def test_missing_default_connection_falls_back_to_ambient(self, modal_client_cls):
        hook = ModalHook()

        with mock.patch.object(ModalHook, "get_connection", new=_missing_connection()):
            hook.get_conn()

        modal_client_cls.from_env.assert_called_once_with()

    def test_missing_named_connection_raises(self, modal_client_cls):
        hook = ModalHook(modal_conn_id="modal_typo")

        with (
            mock.patch.object(ModalHook, "get_connection", new=_missing_connection()),
            pytest.raises(AirflowNotFoundException),
        ):
            hook.get_conn()

        modal_client_cls.from_env.assert_not_called()

    def test_none_conn_id_skips_lookup_entirely(self, modal_client_cls):
        hook = ModalHook(modal_conn_id=None)

        get_connection = mock.Mock(spec=ModalHook.get_connection)
        with mock.patch.object(ModalHook, "get_connection", new=get_connection):
            hook.get_conn()

        get_connection.assert_not_called()
        modal_client_cls.from_env.assert_called_once_with()

    def test_client_is_built_once(self, create_connection_without_db, modal_client_cls):
        create_connection_without_db(
            Connection(conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET)
        )
        hook = ModalHook(modal_conn_id="modal_full")

        first, second = hook.client, hook.client

        assert first is second
        modal_client_cls.from_credentials.assert_called_once()

    def test_client_is_rebuilt_after_fork(self, create_connection_without_db, modal_client_cls):
        create_connection_without_db(
            Connection(conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET)
        )
        modal_client_cls.from_credentials.side_effect = [
            mock.sentinel.parent_client,
            mock.sentinel.child_client,
        ]
        hook = ModalHook(modal_conn_id="modal_full")
        parent_pid = os.getpid()

        with mock.patch(
            "airflow.providers.modal.hooks.modal.os.getpid", autospec=True, return_value=parent_pid
        ):
            in_parent = hook.client
        with mock.patch(
            "airflow.providers.modal.hooks.modal.os.getpid", autospec=True, return_value=parent_pid + 1
        ):
            in_child = hook.client
            in_child_again = hook.client

        assert in_parent is mock.sentinel.parent_client
        assert in_child is mock.sentinel.child_client
        assert in_child_again is mock.sentinel.child_client
        assert modal_client_cls.from_credentials.call_count == 2


class TestEnvironment:
    def test_environment_from_extra(self, create_connection_without_db, modal_client_cls):
        create_connection_without_db(
            Connection(
                conn_id="modal_env",
                conn_type="modal",
                login=TOKEN_ID,
                password=TOKEN_SECRET,
                extra={"environment": "staging"},
            )
        )

        hook = ModalHook(modal_conn_id="modal_env")

        assert hook.environment_name == "staging"
        assert hook.client_kwargs == {"client": modal_client_cls.from_credentials.return_value}

    @pytest.mark.parametrize("extra", [None, {}, {"environment": ""}, {"environment": None}])
    def test_unset_environment_is_none(self, create_connection_without_db, modal_client_cls, extra):
        create_connection_without_db(
            Connection(
                conn_id="modal_env", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET, extra=extra
            )
        )

        assert ModalHook(modal_conn_id="modal_env").environment_name is None

    def test_no_connection_means_no_environment(self, modal_client_cls):
        assert ModalHook(modal_conn_id=None).environment_name is None


class TestLookupApp:
    def test_threads_client_and_environment(self, create_connection_without_db, modal_client_cls):
        create_connection_without_db(
            Connection(
                conn_id="modal_env",
                conn_type="modal",
                login=TOKEN_ID,
                password=TOKEN_SECRET,
                extra={"environment": "staging"},
            )
        )
        hook = ModalHook(modal_conn_id="modal_env")

        with mock.patch("airflow.providers.modal.hooks.modal.modal.App", autospec=True) as app_cls:
            app = hook.lookup_app("my-app", create_if_missing=True)

        app_cls.lookup.assert_called_once_with(
            "my-app",
            client=modal_client_cls.from_credentials.return_value,
            environment_name="staging",
            create_if_missing=True,
        )
        assert app is app_cls.lookup.return_value


class TestTestConnection:
    def test_explicit_credentials_use_throwaway_verify_client(
        self, create_connection_without_db, modal_client_cls
    ):
        create_connection_without_db(
            Connection(conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET)
        )

        ok, message = ModalHook(modal_conn_id="modal_full").test_connection()

        assert ok is True
        assert message == "Connection established!"
        modal_client_cls.verify.assert_called_once_with(
            modal.config.config.get("server_url"), (TOKEN_ID, TOKEN_SECRET)
        )
        # No long-lived client is built just to test the form.
        modal_client_cls.from_credentials.assert_not_called()
        modal_client_cls.from_env.assert_not_called()

    def test_ambient_credentials_check_the_shared_client(
        self, create_connection_without_db, modal_client_cls
    ):
        create_connection_without_db(Connection(conn_id="modal_ambient", conn_type="modal"))

        ok, _ = ModalHook(modal_conn_id="modal_ambient").test_connection()

        assert ok is True
        modal_client_cls.from_env.return_value.hello.assert_called_once_with()
        modal_client_cls.verify.assert_not_called()

    def test_failure_reports_error_text(self, create_connection_without_db, modal_client_cls):
        create_connection_without_db(
            Connection(conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET)
        )
        modal_client_cls.verify.side_effect = modal.exception.AuthError("bad token")

        ok, message = ModalHook(modal_conn_id="modal_full").test_connection()

        assert ok is False
        assert "bad token" in message

    def test_half_filled_connection_is_reported_not_raised(
        self, create_connection_without_db, modal_client_cls
    ):
        create_connection_without_db(Connection(conn_id="modal_half", conn_type="modal", login=TOKEN_ID))

        ok, message = ModalHook(modal_conn_id="modal_half").test_connection()

        assert ok is False
        assert "Token Secret (password)" in message


@pytest.fixture
def env_hook(create_connection_without_db, modal_client_cls):
    """A hook bound to a connection with explicit credentials and environment ``staging``."""
    create_connection_without_db(
        Connection(
            conn_id="modal_env",
            conn_type="modal",
            login=TOKEN_ID,
            password=TOKEN_SECRET,
            extra={"environment": "staging"},
        )
    )
    return ModalHook(modal_conn_id="modal_env")


class TestResourceLookups:
    """Every lookup must carry both the connection's client and its environment."""

    def test_get_conn_returns_the_cached_client(self, env_hook, modal_client_cls):
        assert env_hook.get_conn() is env_hook.client
        modal_client_cls.from_credentials.assert_called_once()

    def test_get_function(self, env_hook, modal_client_cls):
        with mock.patch("airflow.providers.modal.hooks.modal.modal.Function", autospec=True) as function_cls:
            handle = env_hook.get_function("training", "train", version=3)

        function_cls.from_name.assert_called_once_with(
            "training",
            "train",
            version=3,
            environment_name="staging",
            client=modal_client_cls.from_credentials.return_value,
        )
        assert handle is function_cls.from_name.return_value

    def test_get_cls(self, env_hook, modal_client_cls):
        with mock.patch("airflow.providers.modal.hooks.modal.modal.Cls", autospec=True) as cls_cls:
            env_hook.get_cls("training", "Trainer")

        cls_cls.from_name.assert_called_once_with(
            "training",
            "Trainer",
            version=None,
            environment_name="staging",
            client=modal_client_cls.from_credentials.return_value,
        )

    def test_get_secret(self, env_hook, modal_client_cls):
        with mock.patch("airflow.providers.modal.hooks.modal.modal.Secret", autospec=True) as secret_cls:
            env_hook.get_secret("hf-token", required_keys=["HF_TOKEN"])

        secret_cls.from_name.assert_called_once_with(
            "hf-token",
            environment_name="staging",
            required_keys=["HF_TOKEN"],
            client=modal_client_cls.from_credentials.return_value,
        )

    def test_get_volume(self, env_hook, modal_client_cls):
        with mock.patch("airflow.providers.modal.hooks.modal.modal.Volume", autospec=True) as volume_cls:
            env_hook.get_volume("datasets", create_if_missing=True)

        volume_cls.from_name.assert_called_once_with(
            "datasets",
            environment_name="staging",
            create_if_missing=True,
            client=modal_client_cls.from_credentials.return_value,
        )

    def test_create_sandbox_resolves_app_in_environment(self, env_hook, modal_client_cls):
        with (
            mock.patch("airflow.providers.modal.hooks.modal.modal.App", autospec=True) as app_cls,
            mock.patch("airflow.providers.modal.hooks.modal.modal.Sandbox", autospec=True) as sandbox_cls,
        ):
            sandbox = env_hook.create_sandbox(
                "python", "-c", "print(1)", app_name="airflow-tasks", create_app_if_missing=True, gpu="A10G"
            )

        app_cls.lookup.assert_called_once_with(
            "airflow-tasks",
            client=modal_client_cls.from_credentials.return_value,
            environment_name="staging",
            create_if_missing=True,
        )
        sandbox_cls.create.assert_called_once_with(
            "python",
            "-c",
            "print(1)",
            app=app_cls.lookup.return_value,
            client=modal_client_cls.from_credentials.return_value,
            gpu="A10G",
        )
        # environment_name is deliberately absent: Sandbox.create(environment_name=) is deprecated.
        assert "environment_name" not in sandbox_cls.create.call_args.kwargs
        assert sandbox is sandbox_cls.create.return_value

    def test_get_sandbox(self, env_hook, modal_client_cls):
        with mock.patch("airflow.providers.modal.hooks.modal.modal.Sandbox", autospec=True) as sandbox_cls:
            env_hook.get_sandbox("sb-123")

        sandbox_cls.from_id.assert_called_once_with(
            "sb-123", client=modal_client_cls.from_credentials.return_value
        )


class _FakeClient:
    """A Modal client as the hook's async path sees it: only ``is_closed`` is read."""

    def __init__(self, *credentials: str) -> None:
        self.credentials = credentials
        self.closed = False

    def is_closed(self) -> bool:
        return self.closed


class _AioConstructor:
    """A ``modal.Client`` constructor as async code calls it, through ``.aio``; records the calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    async def aio(self, *credentials: str) -> _FakeClient:
        self.calls.append(credentials)
        return _FakeClient(*credentials)


class _FakeClientClass:
    def __init__(self) -> None:
        self.from_credentials = _AioConstructor()
        self.from_env = _AioConstructor()


@pytest.fixture
def async_client_cls():
    """Patch ``modal.Client`` with async constructors and start from an empty shared-client cache."""
    client_cls = _FakeClientClass()
    with (
        mock.patch("airflow.providers.modal.hooks.modal.modal.Client", new=client_cls),
        mock.patch.dict("airflow.providers.modal.hooks.modal._async_clients", clear=True),
    ):
        yield client_cls


def _async_lookup(*, returns=None, raises=None):
    """Stand-in for ``get_async_connection``, as the hook module imports it."""

    # The signature at the provider's floor, common-compat 1.12.0, which has no ``hook=``.
    async def get_async_connection(conn_id):
        if raises is not None:
            raise raises
        return returns

    return mock.patch("airflow.providers.modal.hooks.modal.get_async_connection", new=get_async_connection)


@pytest.mark.asyncio
class TestGetAsyncClient:
    async def test_connection_tokens_build_a_client_shared_across_hooks(self, async_client_cls):
        connection = Connection(
            conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET
        )

        with _async_lookup(returns=connection):
            first = await ModalHook(modal_conn_id="modal_full").get_async_client()
            second = await ModalHook(modal_conn_id="modal_full").get_async_client()

        assert first is second
        assert async_client_cls.from_credentials.calls == [(TOKEN_ID, TOKEN_SECRET)]
        assert async_client_cls.from_env.calls == []

    async def test_closed_shared_client_is_replaced(self, async_client_cls):
        connection = Connection(
            conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET
        )

        with _async_lookup(returns=connection):
            first = await ModalHook(modal_conn_id="modal_full").get_async_client()
            first.closed = True
            second = await ModalHook(modal_conn_id="modal_full").get_async_client()

        assert second is not first
        assert len(async_client_cls.from_credentials.calls) == 2

    async def test_connection_without_tokens_uses_ambient_client(self, async_client_cls):
        with _async_lookup(returns=Connection(conn_id="modal_ambient", conn_type="modal")):
            await ModalHook(modal_conn_id="modal_ambient").get_async_client()

        assert async_client_cls.from_env.calls == [()]
        assert async_client_cls.from_credentials.calls == []

    async def test_missing_default_connection_falls_back_to_ambient(self, async_client_cls):
        with _async_lookup(raises=AirflowNotFoundException("nope")):
            await ModalHook().get_async_client()

        assert async_client_cls.from_env.calls == [()]

    async def test_missing_named_connection_raises(self, async_client_cls):
        with _async_lookup(raises=AirflowNotFoundException("nope")), pytest.raises(AirflowNotFoundException):
            await ModalHook(modal_conn_id="modal_typo").get_async_client()

        assert async_client_cls.from_env.calls == []

    async def test_half_filled_connection_raises(self, async_client_cls):
        connection = Connection(conn_id="modal_half", conn_type="modal", login=TOKEN_ID)

        with _async_lookup(returns=connection), pytest.raises(ModalConnectionError):
            await ModalHook(modal_conn_id="modal_half").get_async_client()

    async def test_concurrent_callers_share_one_new_client(self, async_client_cls):
        connection = Connection(
            conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET
        )
        constructor = async_client_cls.from_credentials
        construct = constructor.aio

        async def slow_construct(*credentials):
            await asyncio.sleep(0.01)
            return await construct(*credentials)

        constructor.aio = slow_construct

        with _async_lookup(returns=connection):
            clients = await asyncio.gather(
                *(ModalHook(modal_conn_id="modal_full").get_async_client() for _ in range(5))
            )

        assert len({id(client) for client in clients}) == 1
        assert constructor.calls == [(TOKEN_ID, TOKEN_SECRET)]

    async def test_aget_sandbox_reattaches_through_the_async_client(self, async_client_cls):
        connection = Connection(
            conn_id="modal_full", conn_type="modal", login=TOKEN_ID, password=TOKEN_SECRET
        )
        reattached: list[tuple[str, object]] = []

        class _FromId:
            async def aio(self, sandbox_id, *, client):
                reattached.append((sandbox_id, client))
                return mock.sentinel.sandbox

        class _SandboxClass:
            from_id = _FromId()

        with (
            _async_lookup(returns=connection),
            mock.patch("airflow.providers.modal.hooks.modal.modal.Sandbox", new=_SandboxClass),
        ):
            hook = ModalHook(modal_conn_id="modal_full")
            sandbox = await hook.aget_sandbox("sb-1")
            client = await hook.get_async_client()

        assert sandbox is mock.sentinel.sandbox
        assert reattached == [("sb-1", client)]
