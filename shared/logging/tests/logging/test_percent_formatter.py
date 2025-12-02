from __future__ import annotations

from unittest import mock

from airflow_shared.logging.percent_formatter import PercentFormatRender


class TestPercentFormatRender:
    def test_no_callsite(self):
        fmter = PercentFormatRender("%(filename)s:%(lineno)d %(message)s")

        formatted = fmter(mock.Mock(name="Logger"), "info", {"event": "our msg"})

        assert formatted == "(unknown file):0 our msg"
