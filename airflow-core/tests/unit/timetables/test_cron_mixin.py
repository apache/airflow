import pytest
from croniter import CroniterBadCronError
from airflow.timetables._cron import CronMixin  

SAMPLE_TZ = "UTC"

def test_valid_cron_expression():
    cm = CronMixin("* * 1 * *", SAMPLE_TZ)  # every day at midnight
    assert isinstance(cm.description, str)
    assert "Every minute" in cm.description or "month" in cm.description


def test_invalid_cron_expression():
    cm = CronMixin("invalid cron", SAMPLE_TZ)
    assert cm.description == ""  


def test_dom_and_dow_conflict():
    cm = CronMixin("* * 1 * 1", SAMPLE_TZ)  # 1st of month or Monday
    desc = cm.description

    assert "(or)" in desc
    assert "Every minute, on day 1 of the month" in desc and "Every minute, only on Monday" in desc