from unittest import TestCase

from airflow.kubernetes.refresh_config import _parse_timestamp


class TestRefreshKubeConfigLoader(TestCase):

    def test_parse_timestamp_should_convert_Z_timezone_to_unix_timestamp(self):
        ts = _parse_timestamp("2020-01-13T13:42:20Z")
        self.assertEqual(1578922940, ts)

    def test_parse_timestamp_should_convert_regular_timezone_to_unix_timestamp(self):
        ts = _parse_timestamp("2020-01-13T13:42:20+0600")
        self.assertEqual(1578922940, ts)
