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

import hashlib
import io
import socket
from unittest import mock

import pytest
import run_pre_extras_install as m

ARCHIVE_CONTENT = b"pretend this is an SDK tarball"
ARCHIVE_SHA256 = hashlib.sha256(ARCHIVE_CONTENT).hexdigest()
URL = "https://example.com/sdk/9.4.0.0-Some-SDK-LinuxX64.tar.gz"


@pytest.fixture
def no_sleep(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(m.time, "sleep", sleeps.append)
    return sleeps


class TestAttemptDownload:
    @mock.patch("urllib.request.urlopen")
    def test_uses_a_short_socket_timeout(self, mock_urlopen, tmp_path):
        mock_urlopen.return_value = io.BytesIO(ARCHIVE_CONTENT)

        m._attempt_download(URL, ARCHIVE_SHA256, tmp_path / "archive.tar.gz")

        assert mock_urlopen.call_args.kwargs["timeout"] == m.DOWNLOAD_TIMEOUT_SECONDS
        assert (tmp_path / "archive.tar.gz").read_bytes() == ARCHIVE_CONTENT


class TestDownloadWithChecksum:
    """The routes tried are recorded as either the url (DNS resolution) or the fallback IP."""

    @pytest.fixture(autouse=True)
    def _record_routes(self, monkeypatch):
        self.routes: list[str] = []
        self.succeed_on: str | None = None

        def fake_attempt(url, expected_sha256, dest):
            route = socket.getaddrinfo(urlparse_host(url), 443)[0][4][0]
            self.routes.append(route)
            if route != self.succeed_on:
                raise TimeoutError("timed out")
            dest.write_bytes(ARCHIVE_CONTENT)

        def urlparse_host(url):
            return m.urlparse(url).hostname

        # Resolve the hostname to itself unless override_dns is in effect, so a route is
        # identified by the fallback IP that was patched in - or by the hostname otherwise.
        monkeypatch.setattr(
            m.socket,
            "getaddrinfo",
            lambda host, port, *args, **kwargs: [
                (m.socket.AF_INET, m.socket.SOCK_STREAM, m.socket.IPPROTO_TCP, "", (host, port))
            ],
        )
        monkeypatch.setattr(m, "_attempt_download", fake_attempt)

    def test_retries_in_rounds_and_gives_up(self, tmp_path, no_sleep):
        with pytest.raises(SystemExit):
            m.download_with_checksum(URL, ARCHIVE_SHA256, tmp_path / "archive.tar.gz")

        assert self.routes == ["example.com"] * m.DOWNLOAD_ROUNDS
        assert no_sleep == [m.SLEEP_BETWEEN_ROUNDS_SECONDS] * (m.DOWNLOAD_ROUNDS - 1)

    def test_succeeds_on_a_later_round(self, tmp_path, no_sleep):
        dest = tmp_path / "archive.tar.gz"
        attempts = 0

        def succeed_on_second_round(url, expected_sha256, dest):
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise TimeoutError("timed out")
            dest.write_bytes(ARCHIVE_CONTENT)

        with mock.patch.object(m, "_attempt_download", succeed_on_second_round):
            m.download_with_checksum(URL, ARCHIVE_SHA256, dest)

        assert attempts == 2
        assert dest.read_bytes() == ARCHIVE_CONTENT
        assert no_sleep == [m.SLEEP_BETWEEN_ROUNDS_SECONDS]

    def test_tries_every_fallback_ip_before_the_next_round(self, tmp_path, no_sleep):
        with pytest.raises(SystemExit):
            m.download_with_checksum(
                URL, ARCHIVE_SHA256, tmp_path / "archive.tar.gz", fallback_ips=["10.0.0.1", "10.0.0.2"]
            )

        assert self.routes == ["example.com", "10.0.0.1", "10.0.0.2"] * m.DOWNLOAD_ROUNDS

    def test_stops_at_the_fallback_ip_that_works(self, tmp_path, no_sleep):
        self.succeed_on = "10.0.0.2"
        dest = tmp_path / "archive.tar.gz"

        m.download_with_checksum(URL, ARCHIVE_SHA256, dest, fallback_ips=["10.0.0.1", "10.0.0.2"])

        assert self.routes == ["example.com", "10.0.0.1", "10.0.0.2"]
        assert dest.read_bytes() == ARCHIVE_CONTENT
        assert no_sleep == []

    def test_does_not_retry_a_checksum_mismatch(self, tmp_path, no_sleep):
        def wrong_checksum(url, expected_sha256, dest):
            self.routes.append(url)
            m.fail(f"sha256 mismatch for {url}")

        with mock.patch.object(m, "_attempt_download", wrong_checksum):
            with pytest.raises(SystemExit):
                m.download_with_checksum(
                    URL, ARCHIVE_SHA256, tmp_path / "archive.tar.gz", fallback_ips=["10.0.0.1"]
                )

        assert self.routes == [URL]
        assert no_sleep == []

    def test_rejects_a_url_without_a_hostname_when_fallbacks_are_configured(self, tmp_path, no_sleep):
        with pytest.raises(SystemExit):
            m.download_with_checksum(
                "https:///no-host.tar.gz", ARCHIVE_SHA256, tmp_path / "a.tar.gz", fallback_ips=["10.0.0.1"]
            )

        assert self.routes == []
