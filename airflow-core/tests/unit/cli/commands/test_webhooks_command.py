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

import base64
import hashlib
import hmac
import io
import json
import os
from argparse import ArgumentParser
from email.message import Message
from http.client import BadStatusLine, HTTPResponse, InvalidURL
from unittest import mock
from urllib.error import HTTPError, URLError
from urllib.request import BaseHandler, OpenerDirector, ProxyHandler, Request, build_opener
from urllib.response import addinfourl

import pytest
from sqlalchemy import delete

from airflow.cli.commands import webhooks_command
from airflow.models.connection import AirflowSecretsBackendAccessDenied, Connection
from airflow.models.team import Team
from airflow.providers_manager import ProviderInfo, ProvidersManager

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.webhooks import FakeHmacSource, build_webhook_request

pytestmark = pytest.mark.db_test

SECRET = "s3cret"
URL = "http://localhost:8080/webhooks/demo"
ANSWER = {"delivery_id": "d-1", "outcome": "no_subscribers"}
# The options with which FakeHmacSource verifies a delivery and reads its delivery id.
FAKE_SIGNING_EXTRA = {
    "source_type": "fake",
    "signature_header": "X-Fake-Signature",
    "signature_prefix": "sha256=",
    "delivery_id_header": "X-Fake-Delivery",
}


def build_connection_env(
    name: str, conn_type: str = "inbound_webhook", password: str | None = SECRET, **extra
) -> dict[str, str]:
    connection = {"conn_type": conn_type, "password": password, "extra": extra}
    return {f"AIRFLOW_CONN_WEBHOOK_{name.upper()}": json.dumps(connection)}


def build_response(status: int = 202, body: bytes = json.dumps(ANSWER).encode()) -> HTTPResponse:
    response = mock.create_autospec(HTTPResponse, instance=True)
    response.status = status
    response.read.return_value = body
    response.__enter__.return_value = response
    return response


def build_opener_mock(answer: HTTPResponse | Exception | None = None) -> mock.MagicMock:
    """Return an opener that answers with ``answer``, or raises it."""
    opener = mock.create_autospec(OpenerDirector, instance=True)
    if isinstance(answer, Exception):
        opener.open.side_effect = answer
    else:
        opener.open.return_value = answer or build_response()
    return opener


class RedirectAnswer(addinfourl):
    """An answer with the ``msg`` that urllib reads from the answers HTTPHandler returns."""

    msg = "Redirect"


class RedirectingHandler(BaseHandler):
    """Answers each http request with a redirect to another host, and keeps the requests it is sent."""

    # Ahead of HTTPHandler, so that no request reaches the network.
    handler_order = 0

    def __init__(self, status: int) -> None:
        self.status = status
        self.requests: list[Request] = []

    def http_open(self, req: Request) -> addinfourl:
        self.requests.append(req)
        headers = Message()
        headers["Location"] = "http://login.example.com/login"
        return RedirectAnswer(io.BytesIO(b"<html>sign in</html>"), headers, req.full_url, self.status)


@pytest.fixture(autouse=True)
def providers(cleanup_providers_manager):
    fake_source = {"source-type": "fake", "classpath": f"{FakeHmacSource.__module__}.FakeHmacSource"}
    dup_source = {**fake_source, "source-type": "dup"}
    ProvidersManager()._provider_dict = {
        "fake-webhooks": ProviderInfo(version="0.0.1", data={"webhook-sources": [fake_source]}),
        "fake-webhooks-first": ProviderInfo(version="0.0.1", data={"webhook-sources": [dup_source]}),
        "fake-webhooks-second": ProviderInfo(version="0.0.1", data={"webhook-sources": [dup_source]}),
    }


def run_check(parser: ArgumentParser, *names: str) -> None:
    webhooks_command.check(parser.parse_args(["webhooks", "check", *names]))


def run_send_test(parser: ArgumentParser, *argv: str) -> None:
    webhooks_command.send_test(parser.parse_args(["webhooks", "send-test", "demo", *argv]))


class TestCheck:
    @conf_vars({("webhooks", "names"): "github,demo,alpha", ("webhooks", "max_body_bytes"): "2048"})
    @mock.patch.dict(
        os.environ,
        {
            **build_connection_env("demo", source_type="fake"),
            **build_connection_env("alpha", source_type="fake", max_body_bytes=512),
        },
    )
    def test_check_without_names_checks_each_listed_webhook(self, parser, capsys):
        with pytest.raises(SystemExit) as exit_info:
            run_check(parser)

        out, err = capsys.readouterr()
        assert exit_info.value.code == 1
        assert out.splitlines() == [
            "Webhook alpha can be served at /webhooks/alpha by airflow api-server --apps webhooks, with "
            "source type fake and bodies of up to 512 bytes",
            "Webhook demo can be served at /webhooks/demo by airflow api-server --apps webhooks, with "
            "source type fake and bodies of up to 2048 bytes",
        ]
        assert err.splitlines() == ["Problem with webhook github: there is no connection webhook_github"]

    @conf_vars(
        {("webhooks", "names"): "demo", ("webhooks", "source_type_owners"): "dup=fake-webhooks-second"}
    )
    @mock.patch.dict(os.environ, build_connection_env("demo", source_type="dup"))
    def test_check_serves_a_conflicted_source_type_from_its_owner(self, parser, capsys):
        run_check(parser, "demo")

        assert "Webhook demo can be served at /webhooks/demo" in capsys.readouterr().out

    @pytest.mark.parametrize(
        ("name", "env", "expected_problem"),
        [
            pytest.param(
                "demo",
                {},
                "Problem with webhook demo: there is no connection webhook_demo",
                id="no-connection",
            ),
            pytest.param(
                "demo",
                build_connection_env("demo", conn_type="generic", source_type="fake"),
                "Problem with webhook demo: connection webhook_demo is misconfigured: its type is 'generic', "
                "not 'inbound_webhook'",
                id="type",
            ),
            pytest.param(
                "demo",
                build_connection_env("demo", source_type="bitbucket"),
                "Problem with webhook demo: connection webhook_demo is misconfigured: no installed provider "
                "serves source type 'bitbucket'",
                id="class",
            ),
            pytest.param(
                "demo",
                build_connection_env("demo", source_type="dup"),
                "Problem with webhook demo: connection webhook_demo is misconfigured: source type 'dup' is "
                "declared by fake-webhooks-first, fake-webhooks-second; set [webhooks] source_type_owners to "
                "the package that serves it",
                id="conflict",
            ),
            pytest.param(
                "github",
                build_connection_env("github", source_type="fake"),
                "Problem with webhook github: [webhooks] names does not list it, so its deliveries are "
                "answered with 404",
                id="not-listed",
            ),
            pytest.param(
                "analytics:demo",
                {},
                "Problem with webhook analytics:demo: it is qualified with a team, and this version of "
                "Airflow serves only webhooks without one",
                id="team-qualified-name",
            ),
            pytest.param(
                "Demo",
                {},
                "Problem with webhook Demo: it is not a webhook name: use 1 to 64 lowercase letters, digits, "
                "- and _, starting with a letter or a digit",
                id="not-a-name",
            ),
        ],
    )
    @conf_vars({("webhooks", "names"): "demo"})
    def test_check_fails_on_each_problem(self, parser, capsys, name, env, expected_problem):
        with mock.patch.dict(os.environ, env), pytest.raises(SystemExit) as exit_info:
            run_check(parser, name)

        assert exit_info.value.code == 1
        assert capsys.readouterr().err.splitlines() == [expected_problem]

    @pytest.fixture
    def team_connection(self, session):
        session.add(Team(name="analytics"))
        session.flush()
        session.add(
            Connection(
                conn_id="webhook_demo", conn_type="inbound_webhook", password=SECRET, team_name="analytics"
            )
        )
        session.commit()
        yield
        session.execute(delete(Connection).where(Connection.conn_id == "webhook_demo"))
        session.execute(delete(Team).where(Team.name == "analytics"))
        session.commit()

    @conf_vars({("webhooks", "names"): "demo"})
    def test_check_fails_on_a_connection_of_a_team(self, parser, team_connection, capsys):
        with pytest.raises(SystemExit) as exit_info:
            run_check(parser, "demo")

        assert exit_info.value.code == 1
        assert capsys.readouterr().err.splitlines() == [
            "Problem with webhook demo: connection webhook_demo belongs to team analytics, and this version "
            "of Airflow serves only webhooks without a team"
        ]

    @pytest.mark.parametrize(
        ("names", "expected_error"),
        [
            pytest.param("", r"^\[webhooks\] names lists no webhooks to check$", id="none-listed"),
            pytest.param("*", r"^\[webhooks\] names cannot be \*", id="invalid"),
        ],
    )
    def test_check_fails_on_the_webhooks_configuration(self, parser, names, expected_error):
        with conf_vars({("webhooks", "names"): names}), pytest.raises(SystemExit, match=expected_error):
            run_check(parser)


@mock.patch("airflow.cli.commands.webhooks_command.build_opener", autospec=True)
class TestSendTest:
    @mock.patch.dict(os.environ, build_connection_env("demo", **FAKE_SIGNING_EXTRA))
    def test_send_test_signs_as_the_connection_says(self, mock_build_opener, parser, capsys, tmp_path):
        opener = mock_build_opener.return_value = build_opener_mock()
        body_file = tmp_path / "body.json"
        body_file.write_bytes(b'{"path": "billing"}')

        run_send_test(
            parser,
            *("--url", URL, "--delivery-id", "d-1", "--header", "X-Fake-Event: deployed"),
            *("--data", f"@{body_file}"),
        )

        request = opener.open.call_args.args[0]
        assert (request.full_url, request.get_method(), request.data) == (URL, "POST", b'{"path": "billing"}')
        webhook_request = build_webhook_request(request.data, dict(request.header_items()))
        FakeHmacSource().verify(webhook_request, SECRET)
        delivery = FakeHmacSource().parse(webhook_request)
        assert (delivery.delivery_id, delivery.attributes) == ("d-1", {"event": "deployed"})
        assert json.loads(capsys.readouterr().out) == {"status": 202, "body": ANSWER}

    @pytest.mark.parametrize(
        "env",
        [
            pytest.param({}, id="no-connection"),
            pytest.param(build_connection_env("demo", signature_header="X-Other"), id="over-the-connection"),
            pytest.param(build_connection_env("demo", token_header="Authorization"), id="over-a-token"),
        ],
    )
    def test_send_test_signs_with_the_options_given(self, mock_build_opener, parser, env):
        opener = mock_build_opener.return_value = build_opener_mock()

        with mock.patch.dict(os.environ, env):
            run_send_test(
                parser,
                *("--url", URL, "--secret", "0ther", "--data", "{}"),
                *("--signature-header", "X-Fake-Signature", "--signature-prefix", "sha256="),
                *("--header", "X-Fake-Event: deployed"),
            )

        request = opener.open.call_args.args[0]
        FakeHmacSource().verify(build_webhook_request(request.data, dict(request.header_items())), "0ther")

    @pytest.mark.parametrize(
        ("env", "argv", "encode"),
        [
            pytest.param(
                build_connection_env("demo", signature_header="X-Signature"), (), bytes.hex, id="hex"
            ),
            pytest.param(
                build_connection_env("demo", signature_header="X-Signature", signature_encoding="base64"),
                (),
                lambda d: base64.b64encode(d).decode(),
                id="base64",
            ),
            pytest.param({}, ("--secret", SECRET), bytes.hex, id="no-connection"),
        ],
    )
    def test_send_test_writes_the_signature_as_the_connection_says(
        self, mock_build_opener, parser, env, argv, encode
    ):
        opener = mock_build_opener.return_value = build_opener_mock()

        with mock.patch.dict(os.environ, env):
            run_send_test(parser, "--url", URL, "--data", "{}", *argv)

        request = opener.open.call_args.args[0]
        assert request.get_header("X-signature") == encode(
            hmac.new(SECRET.encode(), b"{}", hashlib.sha256).digest()
        )

    @mock.patch.dict(
        os.environ, build_connection_env("demo", token_header="Authorization", token_prefix="Bearer ")
    )
    def test_send_test_sends_the_token_as_the_connection_says(self, mock_build_opener, parser):
        opener = mock_build_opener.return_value = build_opener_mock()

        run_send_test(parser, "--url", URL, "--data", "{}")

        request = opener.open.call_args.args[0]
        assert dict(request.header_items()) == {
            "Content-type": "application/json",
            "Authorization": f"Bearer {SECRET}",
        }

    @pytest.mark.parametrize(
        ("status", "answer", "expected_body"),
        [
            pytest.param(401, b'{"detail": "Unauthorized"}', {"detail": "Unauthorized"}, id="refused"),
            pytest.param(502, b"Bad Gateway", "Bad Gateway", id="not-json"),
        ],
    )
    @mock.patch.dict(os.environ, build_connection_env("demo", signature_header="X-Signature"))
    def test_send_test_prints_an_answer_that_is_not_a_2xx_and_fails(
        self, mock_build_opener, parser, capsys, status, answer, expected_body
    ):
        mock_build_opener.return_value = build_opener_mock(
            HTTPError(URL, status, "Refused", Message(), io.BytesIO(answer))
        )

        with pytest.raises(SystemExit) as exit_info:
            run_send_test(parser, "--url", URL, "--data", "{}")

        assert exit_info.value.code == 1
        assert json.loads(capsys.readouterr().out) == {"status": status, "body": expected_body}

    @pytest.mark.parametrize("status", [301, 302, 303])
    @mock.patch.dict(os.environ, build_connection_env("demo", **FAKE_SIGNING_EXTRA))
    def test_send_test_answers_with_a_redirect_instead_of_following_it(
        self, mock_build_opener, parser, capsys, status
    ):
        transport = RedirectingHandler(status)
        mock_build_opener.side_effect = lambda *handlers: build_opener(*handlers, ProxyHandler({}), transport)

        with pytest.raises(SystemExit) as exit_info:
            run_send_test(parser, "--url", URL, "--data", "{}")

        assert exit_info.value.code == 1
        assert json.loads(capsys.readouterr().out) == {"status": status, "body": "<html>sign in</html>"}
        assert [request.full_url for request in transport.requests] == [URL]

    @mock.patch.object(webhooks_command, "MAX_ANSWER_BYTES", 4)
    @mock.patch.dict(os.environ, build_connection_env("demo", **FAKE_SIGNING_EXTRA))
    def test_send_test_fails_on_an_answer_too_long_to_print(self, mock_build_opener, parser, capsys):
        answer = build_response(body=b"12345")
        mock_build_opener.return_value = build_opener_mock(answer)

        with pytest.raises(SystemExit, match=r"^The answer, with status 202, is longer than 4 bytes$"):
            run_send_test(parser, "--url", URL, "--data", "{}")

        answer.read.assert_called_once_with(5)
        assert capsys.readouterr().out == ""

    @pytest.mark.parametrize(
        ("error", "expected_reason"),
        [
            pytest.param(
                URLError(ConnectionRefusedError(111, "Connection refused")),
                ".*Connection refused",
                id="unreachable",
            ),
            pytest.param(
                InvalidURL("URL can't contain control characters"),
                "URL can't contain control characters",
                id="invalid-url",
            ),
            pytest.param(BadStatusLine("HTTP/9.9 OK"), "HTTP/9.9 OK", id="malformed-answer"),
            pytest.param(
                ValueError("Invalid header name b'X Fake'"), "Invalid header name", id="invalid-header-name"
            ),
        ],
    )
    @mock.patch.dict(os.environ, build_connection_env("demo", **FAKE_SIGNING_EXTRA))
    def test_send_test_fails_when_the_delivery_cannot_be_sent(
        self, mock_build_opener, parser, error, expected_reason
    ):
        mock_build_opener.return_value = build_opener_mock(error)

        with pytest.raises(SystemExit, match=f"^Could not send the delivery to {URL}: {expected_reason}"):
            run_send_test(parser, "--url", URL, "--data", "{}")

    @pytest.mark.parametrize(
        ("env", "argv", "expected_error"),
        [
            pytest.param(
                {},
                ("--url", URL),
                r"^There is no connection webhook_demo: pass --secret$",
                id="no-connection",
            ),
            pytest.param(
                build_connection_env("demo", password=None, **FAKE_SIGNING_EXTRA),
                ("--url", URL),
                r"^Connection webhook_demo has no password: pass --secret$",
                id="no-password",
            ),
            pytest.param(
                build_connection_env("demo", source_type="fake"),
                ("--url", URL),
                r"^Connection webhook_demo sets no signature_header to send the signature in: pass "
                r"--signature-header$",
                id="no-signature-header",
            ),
            pytest.param(
                build_connection_env("demo", token_header="Authorization"),
                ("--url", URL, "--signature-prefix", "sha256="),
                r"^Connection webhook_demo sets no signature_header",
                id="signature-prefix-over-a-token",
            ),
            pytest.param(
                build_connection_env("demo", signature_header="X-Signature"),
                ("--url", URL, "--delivery-id", "d-1"),
                r"^Connection webhook_demo sets no delivery_id_header",
                id="no-delivery-id-header",
            ),
            pytest.param(
                build_connection_env("demo", **FAKE_SIGNING_EXTRA),
                ("--url", URL, "--header", "X-Fake-Event"),
                r"^--header 'X-Fake-Event' is not of the form 'NAME: VALUE'$",
                id="malformed-header",
            ),
            pytest.param(
                build_connection_env("demo", **FAKE_SIGNING_EXTRA),
                ("--url", URL, "--header", "X-Fake-Event: deployed\r\nX-Injected: 1"),
                r"^Cannot send header 'X-Fake-Event': a header name must be ASCII and a value Latin-1, "
                r"without line breaks$",
                id="line-break",
            ),
            pytest.param(
                build_connection_env("demo", **FAKE_SIGNING_EXTRA),
                ("--url", URL, "--delivery-id", "d-€"),
                r"^Cannot send header 'X-Fake-Delivery'",
                id="value-not-latin-1",
            ),
            pytest.param(
                build_connection_env("demo", **FAKE_SIGNING_EXTRA),
                ("--url", URL, "--header", "X-Événement: deployed"),
                r"^Cannot send header 'X-Événement'",
                id="name-not-ascii",
            ),
            pytest.param(
                build_connection_env("demo", **FAKE_SIGNING_EXTRA),
                ("--url", "file:///etc/passwd"),
                r"^--url must be an http or https URL, not 'file:///etc/passwd'$",
                id="not-http",
            ),
        ],
    )
    def test_send_test_sends_nothing_it_cannot_send_as_asked(
        self, mock_build_opener, parser, env, argv, expected_error
    ):
        with mock.patch.dict(os.environ, env), pytest.raises(SystemExit, match=expected_error):
            run_send_test(parser, *argv, "--data", "{}")

        mock_build_opener.assert_not_called()

    @mock.patch.object(
        Connection,
        "get_connection_from_secrets",
        autospec=True,
        side_effect=AirflowSecretsBackendAccessDenied("access to webhook_demo is denied"),
    )
    def test_send_test_fails_when_a_secrets_backend_denies_the_connection(
        self, mock_get_connection, mock_build_opener, parser
    ):
        with pytest.raises(
            SystemExit, match=r"^Cannot read connection webhook_demo: access to webhook_demo is denied$"
        ):
            run_send_test(parser, "--url", URL, "--secret", SECRET, "--data", "{}")

        mock_build_opener.assert_not_called()

    @mock.patch.dict(os.environ, build_connection_env("demo", **FAKE_SIGNING_EXTRA))
    def test_send_test_fails_when_the_body_file_cannot_be_read(self, mock_build_opener, parser, tmp_path):
        with pytest.raises(SystemExit, match=r"^Cannot read the body to send: .*No such file"):
            run_send_test(parser, "--url", URL, "--data", f"@{tmp_path / 'missing.json'}")

        mock_build_opener.assert_not_called()
