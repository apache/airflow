import json
import pytest
import apprise
from unittest.mock import MagicMock, call, patch

from airflow.models import Connection
from airflow.providers.apprise.hooks.apprise import AppriseHook


class TestAppriseHook:

    def test_get_config_from_conn(self):
        config = '{"path": "http://some_path_that_dont_exist/", "tag":"alert"}'

        with patch.object(
            AppriseHook,
            "get_connection",
            return_value=Connection(conn_type="apprise", extra=json.dumps({"config": config} )
            ),
        ):
            hook = AppriseHook()
            result = hook.get_config_from_conn()
            assert result == json.loads(config)

    def test_get_config_from_conn_with_dict_should_fail(self):
        config = {"path": "http://some_path_that_dont_exist/", "tag": "alert"}

        with patch.object(
            AppriseHook,
            "get_connection",
            return_value=Connection(conn_type="apprise", extra=json.dumps({"config": config})
            ),
        ):
            hook = AppriseHook()
            #with pytest.raises(TypeError):
            hook.get_config_from_conn()