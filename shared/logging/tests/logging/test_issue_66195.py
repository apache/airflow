from __future__ import annotations

from airflow_shared.logging.percent_formatter import PercentFormatRender


class TestIssue66195:
    def test_percent_render_handles_missing_int_parameters(self):
        """
        Verify that PercentFormatRender does not crash when numeric parameters like
        process, thread, or lineno are missing or set to '(unknown)'.
        """
        fmt = "%(process)d %(thread)d %(lineno)d %(message)s"
        renderer = PercentFormatRender(fmt)

        # Test case 1: Parameters are missing from event_dict
        event_dict = {"event": "test message"}
        result = renderer(None, "info", event_dict)
        assert result.startswith("0 0 0 test message")

        # Test case 2: Parameters are explicitly set to '(unknown)' string by structlog
        event_dict = {
            "event": "test message",
            "process": "(unknown)",
            "thread": "(unknown)",
            "lineno": "(unknown)",
        }
        result = renderer(None, "info", event_dict)
        assert result.startswith("0 0 0 test message")

    def test_percent_render_preserves_valid_int_parameters(self):
        """
        Verify that PercentFormatRender still correctly renders valid integer parameters.
        """
        fmt = "%(process)d %(thread)d %(lineno)d %(message)s"
        renderer = PercentFormatRender(fmt)

        event_dict = {"event": "test message", "process": 123, "thread": 456, "lineno": 789}
        result = renderer(None, "info", event_dict)
        assert result.startswith("123 456 789 test message")
