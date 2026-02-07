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
"""Tests for AirflowArbiter and AirflowGunicornApp."""

from __future__ import annotations

from unittest import mock

import pytest


class TestAirflowArbiter:
    """Tests for the AirflowArbiter class."""

    @pytest.fixture
    def mock_app(self):
        """Create a mock gunicorn application."""
        app = mock.MagicMock()
        app.cfg = mock.MagicMock()
        app.cfg.workers = 4
        app.cfg.settings = {}
        return app

    def test_init_with_refresh_enabled(self, mock_app):
        """Test AirflowArbiter initialization with worker refresh enabled."""

        def mock_arbiter_init(self, app):
            # Set up minimal state that Arbiter.__init__ would set
            self._num_workers = 4
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 2,
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                arbiter = AirflowArbiter(mock_app)

                assert arbiter.worker_refresh_interval == 1800
                assert arbiter.worker_refresh_batch_size == 2
                assert arbiter._refresh_in_progress is False
                assert arbiter._workers_to_replace == set()

    def test_init_batch_size_capped_to_workers(self, mock_app, caplog):
        """Test that batch size is reduced when greater than worker count."""

        def mock_arbiter_init(self, app):
            self._num_workers = 4
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 10,  # Greater than workers
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                arbiter = AirflowArbiter(mock_app)

                assert arbiter.worker_refresh_batch_size == 4  # Capped to num_workers
                assert "reducing batch size" in caplog.text

    def test_init_with_refresh_disabled(self, mock_app):
        """Test AirflowArbiter initialization with worker refresh disabled."""

        def mock_arbiter_init(self, app):
            self._num_workers = 4
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 0,  # Disabled
                ("api", "worker_refresh_batch_size"): 1,
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                arbiter = AirflowArbiter(mock_app)

                assert arbiter.worker_refresh_interval == 0

    def test_manage_workers_calls_parent(self, mock_app):
        """Test that manage_workers calls parent implementation."""

        def mock_arbiter_init(self, app):
            self._num_workers = 4
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 0,
                ("api", "worker_refresh_batch_size"): 1,
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                with mock.patch("gunicorn.arbiter.Arbiter.manage_workers") as mock_parent:
                    from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                    arbiter = AirflowArbiter(mock_app)
                    arbiter.manage_workers()

                    mock_parent.assert_called_once()

    def test_manage_workers_triggers_refresh_when_due(self, mock_app):
        """Test that manage_workers starts refresh when interval elapsed."""

        def mock_arbiter_init(self, app):
            self._num_workers = 4
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 1,
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                with mock.patch("gunicorn.arbiter.Arbiter.manage_workers"):
                    from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                    arbiter = AirflowArbiter(mock_app)
                    arbiter.WORKERS = {100: mock.MagicMock(), 101: mock.MagicMock()}
                    arbiter.spawn_worker = mock.MagicMock()  # Prevent deep call

                    # Simulate time elapsed past refresh interval
                    with mock.patch("time.monotonic", return_value=arbiter._last_refresh_time + 2000):
                        arbiter.manage_workers()

                    # Should have started a refresh cycle
                    assert arbiter._refresh_in_progress is True
                    assert arbiter._workers_to_replace == {100, 101}

    def test_start_refresh_cycle(self, mock_app):
        """Test starting a refresh cycle marks workers for replacement."""

        def mock_arbiter_init(self, app):
            self._num_workers = 4
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 1,
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                arbiter = AirflowArbiter(mock_app)
                arbiter.WORKERS = {
                    100: mock.MagicMock(age=0),
                    101: mock.MagicMock(age=1),
                    102: mock.MagicMock(age=2),
                }
                arbiter.spawn_worker = mock.MagicMock()  # Prevent deep call

                arbiter._start_refresh_cycle()

                assert arbiter._refresh_in_progress is True
                assert arbiter._workers_to_replace == {100, 101, 102}

    def test_continue_refresh_cycle_spawns_workers(self, mock_app):
        """Test that continue_refresh_cycle spawns new workers."""

        def mock_arbiter_init(self, app):
            self._num_workers = 2
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 1,
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                arbiter = AirflowArbiter(mock_app)
                arbiter.WORKERS = {100: mock.MagicMock(age=0), 101: mock.MagicMock(age=1)}
                arbiter._refresh_in_progress = True
                arbiter._workers_to_replace = {100, 101}
                arbiter.spawn_worker = mock.MagicMock()
                arbiter.kill_worker = mock.MagicMock()

                arbiter._continue_refresh_cycle()

                # Should spawn 1 worker (batch_size)
                arbiter.spawn_worker.assert_called_once()

    def test_continue_refresh_cycle_kills_old_workers(self, mock_app):
        """Test that continue_refresh_cycle kills old workers when over capacity."""

        def mock_arbiter_init(self, app):
            self._num_workers = 2
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 1,
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                arbiter = AirflowArbiter(mock_app)
                # 3 workers (1 over capacity)
                arbiter.WORKERS = {
                    100: mock.MagicMock(age=0),
                    101: mock.MagicMock(age=1),
                    102: mock.MagicMock(age=2),  # New worker
                }
                arbiter._refresh_in_progress = True
                arbiter._workers_to_replace = {100, 101}  # Old workers to replace
                arbiter.spawn_worker = mock.MagicMock()
                arbiter.kill_worker = mock.MagicMock()

                arbiter._continue_refresh_cycle()

                # Should kill oldest worker (age=0, pid=100)
                import signal

                arbiter.kill_worker.assert_called_once_with(100, signal.SIGTERM)
                # Worker 100 should be removed from tracking
                assert 100 not in arbiter._workers_to_replace

    def test_refresh_cycle_completes(self, mock_app):
        """Test that refresh cycle completes when all workers replaced."""

        def mock_arbiter_init(self, app):
            self._num_workers = 2
            self.cfg = app.cfg
            self.WORKERS = {}

        with mock.patch(
            "airflow.api_fastapi.gunicorn_app.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 1,
            }.get((section, key), fallback),
        ):
            with mock.patch("gunicorn.arbiter.Arbiter.__init__", mock_arbiter_init):
                from airflow.api_fastapi.gunicorn_app import AirflowArbiter

                arbiter = AirflowArbiter(mock_app)
                # All new workers (none to replace)
                arbiter.WORKERS = {102: mock.MagicMock(age=0), 103: mock.MagicMock(age=1)}
                arbiter._refresh_in_progress = True
                arbiter._workers_to_replace = {100, 101}  # These are gone now
                arbiter.spawn_worker = mock.MagicMock()
                arbiter.kill_worker = mock.MagicMock()

                with mock.patch("time.monotonic", return_value=12345):
                    arbiter._continue_refresh_cycle()

                # Refresh should be complete
                assert arbiter._refresh_in_progress is False
                assert arbiter._workers_to_replace == set()
                assert arbiter._last_refresh_time == 12345


class TestAirflowGunicornApp:
    """Tests for the AirflowGunicornApp class."""

    def test_load_config(self):
        """Test that options are loaded into gunicorn config."""
        from airflow.api_fastapi.gunicorn_app import AirflowGunicornApp

        def mock_init(self, options):
            pass  # Do nothing, we'll set up state manually

        with mock.patch.object(AirflowGunicornApp, "__init__", mock_init):
            app = AirflowGunicornApp.__new__(AirflowGunicornApp)
            app.options = {"workers": 4, "bind": "0.0.0.0:8080"}
            app.cfg = mock.MagicMock()
            app.cfg.settings = {"workers": mock.MagicMock(), "bind": mock.MagicMock()}

            app.load_config()

            assert app.cfg.set.call_count == 2
            app.cfg.set.assert_any_call("workers", 4)
            app.cfg.set.assert_any_call("bind", "0.0.0.0:8080")

    def test_load_returns_airflow_app(self):
        """Test that load() returns the Airflow FastAPI app."""
        from airflow.api_fastapi.gunicorn_app import AirflowGunicornApp

        def mock_init(self, options):
            pass  # Do nothing, we'll set up state manually

        with mock.patch.object(AirflowGunicornApp, "__init__", mock_init):
            app = AirflowGunicornApp.__new__(AirflowGunicornApp)
            app.application = None

            with mock.patch("airflow.api_fastapi.main.app", "mock_fastapi_app"):
                result = app.load()

            assert result == "mock_fastapi_app"
            assert app.application == "mock_fastapi_app"

    def test_run_uses_airflow_arbiter(self):
        """Test that run() uses AirflowArbiter."""
        from airflow.api_fastapi.gunicorn_app import AirflowGunicornApp

        def mock_init(self, options):
            pass  # Do nothing, we'll set up state manually

        with mock.patch.object(AirflowGunicornApp, "__init__", mock_init):
            app = AirflowGunicornApp.__new__(AirflowGunicornApp)

            with mock.patch("airflow.api_fastapi.gunicorn_app.AirflowArbiter") as mock_arbiter:
                mock_arbiter_instance = mock.MagicMock()
                mock_arbiter.return_value = mock_arbiter_instance

                app.run()

                mock_arbiter.assert_called_once_with(app)
                mock_arbiter_instance.run.assert_called_once()


class TestCreateGunicornApp:
    """Tests for the create_gunicorn_app factory function."""

    def test_create_basic_app(self):
        """Test creating an app with basic settings."""
        from airflow.api_fastapi.gunicorn_app import create_gunicorn_app

        with mock.patch("airflow.api_fastapi.gunicorn_app.AirflowGunicornApp") as mock_app_class:
            create_gunicorn_app(
                host="0.0.0.0",
                port=8080,
                num_workers=4,
                worker_timeout=120,
            )

            mock_app_class.assert_called_once()
            options = mock_app_class.call_args[0][0]

            assert options["bind"] == "0.0.0.0:8080"
            assert options["workers"] == 4
            assert options["timeout"] == 120
            assert options["worker_class"] == "uvicorn.workers.UvicornWorker"
            assert options["preload_app"] is True

    def test_create_app_with_ssl(self):
        """Test creating an app with SSL settings."""
        from airflow.api_fastapi.gunicorn_app import create_gunicorn_app

        with mock.patch("airflow.api_fastapi.gunicorn_app.AirflowGunicornApp") as mock_app_class:
            create_gunicorn_app(
                host="0.0.0.0",
                port=8443,
                num_workers=4,
                worker_timeout=120,
                ssl_cert="/path/to/cert.pem",
                ssl_key="/path/to/key.pem",
            )

            options = mock_app_class.call_args[0][0]

            assert options["certfile"] == "/path/to/cert.pem"
            assert options["keyfile"] == "/path/to/key.pem"

    def test_create_app_with_proxy_headers(self):
        """Test creating an app with proxy headers enabled."""
        from airflow.api_fastapi.gunicorn_app import create_gunicorn_app

        with mock.patch("airflow.api_fastapi.gunicorn_app.AirflowGunicornApp") as mock_app_class:
            create_gunicorn_app(
                host="0.0.0.0",
                port=8080,
                num_workers=4,
                worker_timeout=120,
                proxy_headers=True,
            )

            options = mock_app_class.call_args[0][0]

            assert options["forwarded_allow_ips"] == "*"

    def test_create_app_with_access_log(self):
        """Test creating an app with access logging enabled."""
        from airflow.api_fastapi.gunicorn_app import create_gunicorn_app

        with mock.patch("airflow.api_fastapi.gunicorn_app.AirflowGunicornApp") as mock_app_class:
            create_gunicorn_app(
                host="0.0.0.0",
                port=8080,
                num_workers=4,
                worker_timeout=120,
                access_log=True,
            )

            options = mock_app_class.call_args[0][0]

            assert options["accesslog"] == "-"

    def test_create_app_without_access_log(self):
        """Test creating an app with access logging disabled."""
        from airflow.api_fastapi.gunicorn_app import create_gunicorn_app

        with mock.patch("airflow.api_fastapi.gunicorn_app.AirflowGunicornApp") as mock_app_class:
            create_gunicorn_app(
                host="0.0.0.0",
                port=8080,
                num_workers=4,
                worker_timeout=120,
                access_log=False,
            )

            options = mock_app_class.call_args[0][0]

            assert "accesslog" not in options
