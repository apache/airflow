#
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
import copy
import datetime
import io
import os
import re
import tempfile
import textwrap
import unittest
import warnings
from collections import OrderedDict
from unittest import mock

import pytest

from airflow import configuration
from airflow.configuration import (
    AirflowConfigException,
    AirflowConfigParser,
    _custom_secrets_backend,
    conf,
    expand_env_var,
    get_airflow_config,
    get_airflow_home,
    parameterized_config,
    run_command,
)
from tests.test_utils.config import conf_vars
from tests.test_utils.reset_warning_registry import reset_warning_registry
from tests.utils.test_config import (
    remove_all_configurations,
    set_deprecated_options,
    set_sensitive_config_values,
    use_config,
)

HOME_DIR = os.path.expanduser('~')


@unittest.mock.patch.dict(
    'os.environ',
    {
        'AIRFLOW__TESTSECTION__TESTKEY': 'testvalue',
        'AIRFLOW__TESTSECTION__TESTPERCENT': 'with%percent',
        'AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD': 'echo -n "OK"',
        'AIRFLOW__TESTCMDENV__NOTACOMMAND_CMD': 'echo -n "NOT OK"',
    },
)
class TestConf:
    def test_airflow_home_default(self):
        with unittest.mock.patch.dict('os.environ'):
            if 'AIRFLOW_HOME' in os.environ:
                del os.environ['AIRFLOW_HOME']
            assert get_airflow_home() == expand_env_var('~/airflow')

    def test_airflow_home_override(self):
        with unittest.mock.patch.dict('os.environ', AIRFLOW_HOME='/path/to/airflow'):
            assert get_airflow_home() == '/path/to/airflow'

    def test_airflow_config_default(self):
        with unittest.mock.patch.dict('os.environ'):
            if 'AIRFLOW_CONFIG' in os.environ:
                del os.environ['AIRFLOW_CONFIG']
            assert get_airflow_config('/home/airflow') == expand_env_var('/home/airflow/airflow.cfg')

    def test_airflow_config_override(self):
        with unittest.mock.patch.dict('os.environ', AIRFLOW_CONFIG='/path/to/airflow/airflow.cfg'):
            assert get_airflow_config('/home//airflow') == '/path/to/airflow/airflow.cfg'

    @conf_vars({("core", "percent"): "with%%inside"})
    def test_case_sensitivity(self):
        # section and key are case insensitive for get method
        # note: this is not the case for as_dict method
        assert conf.get("core", "percent") == "with%inside"
        assert conf.get("core", "PERCENT") == "with%inside"
        assert conf.get("CORE", "PERCENT") == "with%inside"

    def test_config_as_dict(self):
        """Test that getting config as dict works even if
        environment has non-legal env vars"""
        with unittest.mock.patch.dict('os.environ'):
            os.environ['AIRFLOW__VAR__broken'] = "not_ok"
            asdict = conf.as_dict(raw=True, display_sensitive=True)
        assert asdict.get('VAR') is None
        assert asdict['testsection']['testkey'] == 'testvalue'

    def test_env_var_config(self):
        opt = conf.get('testsection', 'testkey')
        assert opt == 'testvalue'

        opt = conf.get('testsection', 'testpercent')
        assert opt == 'with%percent'

        assert conf.has_option('testsection', 'testkey')

        with unittest.mock.patch.dict(
            'os.environ', AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AIRFLOW__TESTSECTION__TESTKEY='nested'
        ):
            opt = conf.get('kubernetes_environment_variables', 'AIRFLOW__TESTSECTION__TESTKEY')
            assert opt == 'nested'

    @mock.patch.dict(
        'os.environ', AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AIRFLOW__TESTSECTION__TESTKEY='nested'
    )
    @conf_vars({("core", "percent"): "with%%inside"})
    def test_conf_as_dict(self):
        cfg_dict = conf.as_dict()

        # test that configs are picked up
        assert cfg_dict['core']['unit_test_mode'] == 'True'

        assert cfg_dict['core']['percent'] == 'with%inside'

        # test env vars
        assert cfg_dict['testsection']['testkey'] == '< hidden >'
        assert cfg_dict['kubernetes_environment_variables']['AIRFLOW__TESTSECTION__TESTKEY'] == '< hidden >'

    def test_conf_as_dict_source(self):
        # test display_source
        cfg_dict = conf.as_dict(display_source=True)
        assert cfg_dict['core']['load_examples'][1] == 'airflow.cfg'
        assert cfg_dict['database']['load_default_connections'][1] == 'airflow.cfg'
        assert cfg_dict['testsection']['testkey'] == ('< hidden >', 'env var')

    def test_conf_as_dict_sensitive(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True)
        assert cfg_dict['testsection']['testkey'] == 'testvalue'
        assert cfg_dict['testsection']['testpercent'] == 'with%percent'

        # test display_source and display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True, display_source=True)
        assert cfg_dict['testsection']['testkey'] == ('testvalue', 'env var')

    @conf_vars({("core", "percent"): "with%%inside"})
    def test_conf_as_dict_raw(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(raw=True, display_sensitive=True)
        assert cfg_dict['testsection']['testkey'] == 'testvalue'

        # Values with '%' in them should be escaped
        assert cfg_dict['testsection']['testpercent'] == 'with%%percent'
        assert cfg_dict['core']['percent'] == 'with%%inside'

    def test_conf_as_dict_exclude_env(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(include_env=False, display_sensitive=True)

        # Since testsection is only created from env vars, it shouldn't be
        # present at all if we don't ask for env vars to be included.
        assert 'testsection' not in cfg_dict

    def test_command_precedence(self):
        test_config = '''[test]
key1 = hello
key2_cmd = printf cmd_result
key3 = airflow
key4_cmd = printf key4_result
'''
        test_config_default = '''[test]
key1 = awesome
key2 = airflow

[another]
key6 = value6
'''

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ('test', 'key2'),
            ('test', 'key4'),
        }
        assert 'hello' == test_conf.get('test', 'key1')
        assert 'cmd_result' == test_conf.get('test', 'key2')
        assert 'airflow' == test_conf.get('test', 'key3')
        assert 'key4_result' == test_conf.get('test', 'key4')
        assert 'value6' == test_conf.get('another', 'key6')

        assert 'hello' == test_conf.get('test', 'key1', fallback='fb')
        assert 'value6' == test_conf.get('another', 'key6', fallback='fb')
        assert 'fb' == test_conf.get('another', 'key7', fallback='fb')
        assert test_conf.getboolean('another', 'key8_boolean', fallback='True') is True
        assert 10 == test_conf.getint('another', 'key8_int', fallback='10')
        assert 1.0 == test_conf.getfloat('another', 'key8_float', fallback='1')

        assert test_conf.has_option('test', 'key1')
        assert test_conf.has_option('test', 'key2')
        assert test_conf.has_option('test', 'key3')
        assert test_conf.has_option('test', 'key4')
        assert not test_conf.has_option('test', 'key5')
        assert test_conf.has_option('another', 'key6')

        cfg_dict = test_conf.as_dict(display_sensitive=True)
        assert 'cmd_result' == cfg_dict['test']['key2']
        assert 'key2_cmd' not in cfg_dict['test']

        # If we exclude _cmds then we should still see the commands to run, not
        # their values
        cfg_dict = test_conf.as_dict(include_cmds=False, display_sensitive=True)
        assert 'key4' not in cfg_dict['test']
        assert 'printf key4_result' == cfg_dict['test']['key4_cmd']

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @conf_vars(
        {
            ("secrets", "backend"): "airflow.providers.hashicorp.secrets.vault.VaultBackend",
            ("secrets", "backend_kwargs"): '{"url": "http://127.0.0.1:8200", "token": "token"}',
        }
    )
    def test_config_from_secret_backend(self, mock_hvac):
        """Get Config Value from a Secret Backend"""
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '2d48a2ad-6bcb-e5b6-429d-da35fdf31f56',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'value': 'sqlite:////Users/airflow/airflow/airflow.db'},
                'metadata': {
                    'created_time': '2020-03-28T02:10:54.301784Z',
                    'deletion_time': '',
                    'destroyed': False,
                    'version': 1,
                },
            },
            'wrap_info': None,
            'warnings': None,
            'auth': None,
        }

        test_config = '''[test]
sql_alchemy_conn_secret = sql_alchemy_conn
'''
        test_config_default = '''[test]
sql_alchemy_conn = airflow
'''

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ('test', 'sql_alchemy_conn'),
        }

        assert 'sqlite:////Users/airflow/airflow/airflow.db' == test_conf.get('test', 'sql_alchemy_conn')

    def test_hidding_of_sensitive_config_values(self):
        test_config = '''[test]
                         sql_alchemy_conn_secret = sql_alchemy_conn
                      '''
        test_config_default = '''[test]
                                 sql_alchemy_conn = airflow
                              '''
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ('test', 'sql_alchemy_conn'),
        }

        assert 'airflow' == test_conf.get('test', 'sql_alchemy_conn')
        # Hide sensitive fields
        asdict = test_conf.as_dict(display_sensitive=False)
        assert '< hidden >' == asdict['test']['sql_alchemy_conn']
        # If display_sensitive is false, then include_cmd, include_env,include_secrets must all be True
        # This ensures that cmd and secrets env are hidden at the appropriate method and no surprises
        with pytest.raises(ValueError):
            test_conf.as_dict(display_sensitive=False, include_cmds=False)
        # Test that one of include_cmds, include_env, include_secret can be false when display_sensitive
        # is True
        assert test_conf.as_dict(display_sensitive=True, include_cmds=False)

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @conf_vars(
        {
            ("secrets", "backend"): "airflow.providers.hashicorp.secrets.vault.VaultBackend",
            ("secrets", "backend_kwargs"): '{"url": "http://127.0.0.1:8200", "token": "token"}',
        }
    )
    def test_config_raise_exception_from_secret_backend_connection_error(self, mock_hvac):
        """Get Config Value from a Secret Backend"""

        _custom_secrets_backend.cache_clear()
        mock_client = mock.MagicMock()
        # mock_client.side_effect = AirflowConfigException
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = Exception

        test_config = '''[test]
sql_alchemy_conn_secret = sql_alchemy_conn
'''
        test_config_default = '''[test]
sql_alchemy_conn = airflow
'''
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ('test', 'sql_alchemy_conn'),
        }

        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Cannot retrieve config from alternative secrets backend. '
                'Make sure it is configured properly and that the Backend '
                'is accessible.'
            ),
        ):
            test_conf.get('test', 'sql_alchemy_conn')
        _custom_secrets_backend.cache_clear()

    def test_getboolean(self):
        """Test AirflowConfigParser.getboolean"""
        test_config = """
[type_validation]
key1 = non_bool_value

[true]
key2 = t
key3 = true
key4 = 1

[false]
key5 = f
key6 = false
key7 = 0

[inline-comment]
key8 = true #123
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to bool. Please check "key1" key in "type_validation" section. '
                'Current value: "non_bool_value".'
            ),
        ):
            test_conf.getboolean('type_validation', 'key1')
        assert isinstance(test_conf.getboolean('true', 'key3'), bool)
        assert test_conf.getboolean('true', 'key2') is True
        assert test_conf.getboolean('true', 'key3') is True
        assert test_conf.getboolean('true', 'key4') is True
        assert test_conf.getboolean('false', 'key5') is False
        assert test_conf.getboolean('false', 'key6') is False
        assert test_conf.getboolean('false', 'key7') is False
        assert test_conf.getboolean('inline-comment', 'key8') is True

    def test_getint(self):
        """Test AirflowConfigParser.getint"""
        test_config = """
[invalid]
key1 = str

[valid]
key2 = 1
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to int. Please check "key1" key in "invalid" section. '
                'Current value: "str".'
            ),
        ):
            test_conf.getint('invalid', 'key1')
        assert isinstance(test_conf.getint('valid', 'key2'), int)
        assert 1 == test_conf.getint('valid', 'key2')

    def test_getfloat(self):
        """Test AirflowConfigParser.getfloat"""
        test_config = """
[invalid]
key1 = str

[valid]
key2 = 1.23
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to float. Please check "key1" key in "invalid" section. '
                'Current value: "str".'
            ),
        ):
            test_conf.getfloat('invalid', 'key1')
        assert isinstance(test_conf.getfloat('valid', 'key2'), float)
        assert 1.23 == test_conf.getfloat('valid', 'key2')

    @pytest.mark.parametrize(
        ("config_str", "expected"),
        [
            pytest.param('{"a": 123}', {'a': 123}, id='dict'),
            pytest.param('[1,2,3]', [1, 2, 3], id='list'),
            pytest.param('"abc"', 'abc', id='str'),
            pytest.param('2.1', 2.1, id='num'),
            pytest.param('', None, id='empty'),
        ],
    )
    def test_getjson(self, config_str, expected):
        config = textwrap.dedent(
            f"""
            [test]
            json = {config_str}
        """
        )
        test_conf = AirflowConfigParser()
        test_conf.read_string(config)

        assert test_conf.getjson('test', 'json') == expected

    def test_getjson_empty_with_fallback(self):
        config = textwrap.dedent(
            """
            [test]
            json =
            """
        )
        test_conf = AirflowConfigParser()
        test_conf.read_string(config)

        assert test_conf.getjson('test', 'json', fallback={}) == {}
        assert test_conf.getjson('test', 'json') is None

    @pytest.mark.parametrize(
        ("fallback"),
        [
            pytest.param({"a": "b"}, id='dict'),
            # fallback is _NOT_ json parsed, but used verbatim
            pytest.param('{"a": "b"}', id='str'),
            pytest.param(None, id='None'),
        ],
    )
    def test_getjson_fallback(self, fallback):
        test_conf = AirflowConfigParser()

        assert test_conf.getjson('test', 'json', fallback=fallback) == fallback

    def test_has_option(self):
        test_config = '''[test]
key1 = value1
'''
        test_conf = AirflowConfigParser()
        test_conf.read_string(test_config)
        assert test_conf.has_option('test', 'key1')
        assert not test_conf.has_option('test', 'key_not_exists')
        assert not test_conf.has_option('section_not_exists', 'key1')

    def test_remove_option(self):
        test_config = '''[test]
key1 = hello
key2 = airflow
'''
        test_config_default = '''[test]
key1 = awesome
key2 = airflow
'''

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert 'hello' == test_conf.get('test', 'key1')
        test_conf.remove_option('test', 'key1', remove_default=False)
        assert 'awesome' == test_conf.get('test', 'key1')

        test_conf.remove_option('test', 'key2')
        assert not test_conf.has_option('test', 'key2')

    def test_getsection(self):
        test_config = '''
[test]
key1 = hello
[new_section]
key = value
'''
        test_config_default = '''
[test]
key1 = awesome
key2 = airflow

[testsection]
key3 = value3
'''
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert OrderedDict([('key1', 'hello'), ('key2', 'airflow')]) == test_conf.getsection('test')
        assert OrderedDict(
            [('key3', 'value3'), ('testkey', 'testvalue'), ('testpercent', 'with%percent')]
        ) == test_conf.getsection('testsection')

        assert OrderedDict([('key', 'value')]) == test_conf.getsection('new_section')

        assert test_conf.getsection('non_existent_section') is None

    def test_get_section_should_respect_cmd_env_variable(self):
        with tempfile.NamedTemporaryFile(delete=False) as cmd_file:
            cmd_file.write(b"#!/usr/bin/env bash\n")
            cmd_file.write(b"echo -n difficult_unpredictable_cat_password\n")
            cmd_file.flush()
            os.chmod(cmd_file.name, 0o0555)
            cmd_file.close()

            with mock.patch.dict("os.environ", {"AIRFLOW__WEBSERVER__SECRET_KEY_CMD": cmd_file.name}):
                content = conf.getsection("webserver")
            os.unlink(cmd_file.name)
        assert content["secret_key"] == "difficult_unpredictable_cat_password"

    def test_kubernetes_environment_variables_section(self):
        test_config = '''
[kubernetes_environment_variables]
key1 = hello
AIRFLOW_HOME = /root/airflow
'''
        test_config_default = '''
[kubernetes_environment_variables]
'''
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert OrderedDict([('key1', 'hello'), ('AIRFLOW_HOME', '/root/airflow')]) == test_conf.getsection(
            'kubernetes_environment_variables'
        )

    def test_broker_transport_options(self):
        section_dict = conf.getsection("celery_broker_transport_options")
        assert isinstance(section_dict['visibility_timeout'], int)
        assert isinstance(section_dict['_test_only_bool'], bool)
        assert isinstance(section_dict['_test_only_float'], float)
        assert isinstance(section_dict['_test_only_string'], str)

    def test_auth_backends_adds_session(self):
        test_conf = AirflowConfigParser(default_config='')
        # Guarantee we have deprecated settings, so we test the deprecation
        # lookup even if we remove this explicit fallback
        test_conf.deprecated_values = {
            'api': {
                'auth_backends': (
                    re.compile(r'^airflow\.api\.auth\.backend\.deny_all$|^$'),
                    'airflow.api.auth.backend.session',
                    '3.0',
                ),
            },
        }
        test_conf.read_dict({'api': {'auth_backends': 'airflow.api.auth.backend.basic_auth'}})

        with pytest.warns(FutureWarning):
            test_conf.validate()
            assert (
                test_conf.get('api', 'auth_backends')
                == 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
            )

    def test_command_from_env(self):
        test_cmdenv_config = '''[testcmdenv]
itsacommand = NOT OK
notacommand = OK
'''
        test_cmdenv_conf = AirflowConfigParser()
        test_cmdenv_conf.read_string(test_cmdenv_config)
        test_cmdenv_conf.sensitive_config_values.add(('testcmdenv', 'itsacommand'))
        with unittest.mock.patch.dict('os.environ'):
            # AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD maps to ('testcmdenv', 'itsacommand') in
            # sensitive_config_values and therefore should return 'OK' from the environment variable's
            # echo command, and must not return 'NOT OK' from the configuration
            assert test_cmdenv_conf.get('testcmdenv', 'itsacommand') == 'OK'
            # AIRFLOW__TESTCMDENV__NOTACOMMAND_CMD maps to no entry in sensitive_config_values and therefore
            # the option should return 'OK' from the configuration, and must not return 'NOT OK' from
            # the environment variable's echo command
            assert test_cmdenv_conf.get('testcmdenv', 'notacommand') == 'OK'

    @pytest.mark.parametrize('display_sensitive, result', [(True, 'OK'), (False, '< hidden >')])
    def test_as_dict_display_sensitivewith_command_from_env(self, display_sensitive, result):

        test_cmdenv_conf = AirflowConfigParser()
        test_cmdenv_conf.sensitive_config_values.add(('testcmdenv', 'itsacommand'))
        with unittest.mock.patch.dict('os.environ'):
            asdict = test_cmdenv_conf.as_dict(True, display_sensitive)
            assert asdict['testcmdenv']['itsacommand'] == (result, 'cmd')

    def test_parameterized_config_gen(self):
        config = textwrap.dedent(
            """
            [core]
            dags_folder = {AIRFLOW_HOME}/dags
            sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/airflow.db
            parallelism = 32
            fernet_key = {FERNET_KEY}
        """
        )

        cfg = parameterized_config(config)

        # making sure some basic building blocks are present:
        assert "[core]" in cfg
        assert "dags_folder" in cfg
        assert "sql_alchemy_conn" in cfg
        assert "fernet_key" in cfg

        # making sure replacement actually happened
        assert "{AIRFLOW_HOME}" not in cfg
        assert "{FERNET_KEY}" not in cfg

    def test_config_use_original_when_original_and_fallback_are_present(self):
        assert conf.has_option("core", "FERNET_KEY")
        assert not conf.has_option("core", "FERNET_KEY_CMD")

        fernet_key = conf.get('core', 'FERNET_KEY')

        with conf_vars({('core', 'FERNET_KEY_CMD'): 'printf HELLO'}):
            fallback_fernet_key = conf.get("core", "FERNET_KEY")

        assert fernet_key == fallback_fernet_key

    def test_config_throw_error_when_original_and_fallback_is_absent(self):
        assert conf.has_option("core", "FERNET_KEY")
        assert not conf.has_option("core", "FERNET_KEY_CMD")

        with conf_vars({('core', 'fernet_key'): None}):
            with pytest.raises(AirflowConfigException) as ctx:
                conf.get("core", "FERNET_KEY")

        exception = str(ctx.value)
        message = "section/key [core/fernet_key] not found in config"
        assert message == exception

    def test_config_override_original_when_non_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = "some value"

        with mock.patch.dict('os.environ', {key: value}):
            fernet_key = conf.get('core', 'FERNET_KEY')

        assert value == fernet_key

    def test_config_override_original_when_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = "some value"

        with mock.patch.dict('os.environ', {key: value}):
            fernet_key = conf.get('core', 'FERNET_KEY')

        assert value == fernet_key

    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__DAGS_FOLDER": "/tmp/test_folder"})
    def test_write_should_respect_env_variable(self):
        with io.StringIO() as string_file:
            conf.write(string_file)
            content = string_file.getvalue()
        assert "dags_folder = /tmp/test_folder" in content

    def test_run_command(self):
        write = r'sys.stdout.buffer.write("\u1000foo".encode("utf8"))'

        cmd = f'import sys; {write}; sys.stdout.flush()'

        assert run_command(f"python -c '{cmd}'") == '\u1000foo'

        assert run_command('echo "foo bar"') == 'foo bar\n'
        with pytest.raises(AirflowConfigException):
            run_command('bash -c "exit 1"')

    def test_confirm_unittest_mod(self):
        assert conf.get('core', 'unit_test_mode')

    def test_enum_default_task_weight_rule_from_conf(self):
        test_conf = AirflowConfigParser(default_config='')
        test_conf.read_dict({'core': {'default_task_weight_rule': 'sidestream'}})
        with pytest.raises(AirflowConfigException) as ctx:
            test_conf.validate()
        exception = str(ctx.value)
        message = (
            "`[core] default_task_weight_rule` should not be 'sidestream'. Possible values: "
            "absolute, downstream, upstream."
        )
        assert message == exception

    def test_enum_logging_levels(self):
        test_conf = AirflowConfigParser(default_config='')
        test_conf.read_dict({'logging': {'logging_level': 'XXX'}})
        with pytest.raises(AirflowConfigException) as ctx:
            test_conf.validate()
        exception = str(ctx.value)
        message = (
            "`[logging] logging_level` should not be 'XXX'. Possible values: "
            "CRITICAL, FATAL, ERROR, WARN, WARNING, INFO, DEBUG."
        )
        assert message == exception

    def test_as_dict_works_without_sensitive_cmds(self):
        conf_materialize_cmds = conf.as_dict(display_sensitive=True, raw=True, include_cmds=True)
        conf_maintain_cmds = conf.as_dict(display_sensitive=True, raw=True, include_cmds=False)

        assert 'sql_alchemy_conn' in conf_materialize_cmds['core']
        assert 'sql_alchemy_conn_cmd' not in conf_materialize_cmds['core']

        assert 'sql_alchemy_conn' in conf_maintain_cmds['core']
        assert 'sql_alchemy_conn_cmd' not in conf_maintain_cmds['core']

        assert (
            conf_materialize_cmds['core']['sql_alchemy_conn']
            == conf_maintain_cmds['core']['sql_alchemy_conn']
        )

    def test_as_dict_respects_sensitive_cmds(self):
        conf_conn = conf['database']['sql_alchemy_conn']
        test_conf = copy.deepcopy(conf)
        test_conf.read_string(
            textwrap.dedent(
                """
                [database]
                sql_alchemy_conn_cmd = echo -n my-super-secret-conn
                """
            )
        )

        conf_materialize_cmds = test_conf.as_dict(display_sensitive=True, raw=True, include_cmds=True)
        conf_maintain_cmds = test_conf.as_dict(display_sensitive=True, raw=True, include_cmds=False)

        assert 'sql_alchemy_conn' in conf_materialize_cmds['database']
        assert 'sql_alchemy_conn_cmd' not in conf_materialize_cmds['database']

        if conf_conn == test_conf.airflow_defaults['database']['sql_alchemy_conn']:
            assert conf_materialize_cmds['database']['sql_alchemy_conn'] == 'my-super-secret-conn'

        assert 'sql_alchemy_conn_cmd' in conf_maintain_cmds['database']
        assert conf_maintain_cmds['database']['sql_alchemy_conn_cmd'] == 'echo -n my-super-secret-conn'

        if conf_conn == test_conf.airflow_defaults['database']['sql_alchemy_conn']:
            assert 'sql_alchemy_conn' not in conf_maintain_cmds['database']
        else:
            assert 'sql_alchemy_conn' in conf_maintain_cmds['database']
            assert conf_maintain_cmds['database']['sql_alchemy_conn'] == conf_conn

    @mock.patch.dict(
        'os.environ', {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD": "echo -n 'postgresql://'"}, clear=True
    )
    def test_as_dict_respects_sensitive_cmds_from_env(self):
        test_conf = copy.deepcopy(conf)
        test_conf.read_string("")

        conf_materialize_cmds = test_conf.as_dict(display_sensitive=True, raw=True, include_cmds=True)

        assert 'sql_alchemy_conn' in conf_materialize_cmds['database']
        assert 'sql_alchemy_conn_cmd' not in conf_materialize_cmds['database']

        assert conf_materialize_cmds['database']['sql_alchemy_conn'] == 'postgresql://'

    def test_gettimedelta(self):
        test_config = '''
[invalid]
# non-integer value
key1 = str

# fractional value
key2 = 300.99

# too large value for C int
key3 = 999999999999999

[valid]
# negative value
key4 = -1

# zero
key5 = 0

# positive value
key6 = 300

[default]
# Equals to None
key7 =
'''
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to int. Please check "key1" key in "invalid" section. '
                'Current value: "str".'
            ),
        ):
            test_conf.gettimedelta("invalid", "key1")

        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to int. Please check "key2" key in "invalid" section. '
                'Current value: "300.99".'
            ),
        ):
            test_conf.gettimedelta("invalid", "key2")

        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to timedelta in `seconds`. '
                'Python int too large to convert to C int. '
                'Please check "key3" key in "invalid" section. Current value: "999999999999999".'
            ),
        ):
            test_conf.gettimedelta("invalid", "key3")

        assert isinstance(test_conf.gettimedelta('valid', 'key4'), datetime.timedelta)
        assert test_conf.gettimedelta('valid', 'key4') == datetime.timedelta(seconds=-1)
        assert isinstance(test_conf.gettimedelta('valid', 'key5'), datetime.timedelta)
        assert test_conf.gettimedelta('valid', 'key5') == datetime.timedelta(seconds=0)
        assert isinstance(test_conf.gettimedelta('valid', 'key6'), datetime.timedelta)
        assert test_conf.gettimedelta('valid', 'key6') == datetime.timedelta(seconds=300)
        assert isinstance(test_conf.gettimedelta('default', 'key7'), type(None))
        assert test_conf.gettimedelta('default', 'key7') is None


class TestDeprecatedConf:
    @conf_vars(
        {
            ("celery", "worker_concurrency"): None,
            ("celery", "celeryd_concurrency"): None,
        }
    )
    def test_deprecated_options(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        with set_deprecated_options(
            deprecated_options={('celery', 'worker_concurrency'): ('celery', 'celeryd_concurrency', '2.0.0')}
        ):
            # Remove it so we are sure we use the right setting
            conf.remove_option('celery', 'worker_concurrency')

            with pytest.warns(DeprecationWarning):
                with mock.patch.dict('os.environ', AIRFLOW__CELERY__CELERYD_CONCURRENCY="99"):
                    assert conf.getint('celery', 'worker_concurrency') == 99

            with pytest.warns(DeprecationWarning), conf_vars({('celery', 'celeryd_concurrency'): '99'}):
                assert conf.getint('celery', 'worker_concurrency') == 99

    @conf_vars(
        {
            ('logging', 'logging_level'): None,
            ('core', 'logging_level'): None,
        }
    )
    def test_deprecated_options_with_new_section(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        with set_deprecated_options(
            deprecated_options={('logging', 'logging_level'): ('core', 'logging_level', '2.0.0')}
        ):
            # Remove it so we are sure we use the right setting
            conf.remove_option('core', 'logging_level')
            conf.remove_option('logging', 'logging_level')

            with pytest.warns(DeprecationWarning):
                with mock.patch.dict('os.environ', AIRFLOW__CORE__LOGGING_LEVEL="VALUE"):
                    assert conf.get('logging', 'logging_level') == "VALUE"

            with pytest.warns(DeprecationWarning), conf_vars({('core', 'logging_level'): 'VALUE'}):
                assert conf.get('logging', 'logging_level') == "VALUE"

    @conf_vars(
        {
            ("celery", "result_backend"): None,
            ("celery", "celery_result_backend"): None,
            ("celery", "celery_result_backend_cmd"): None,
        }
    )
    def test_deprecated_options_cmd(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        with set_deprecated_options(
            deprecated_options={('celery', "result_backend"): ('celery', 'celery_result_backend', '2.0.0')}
        ), set_sensitive_config_values(sensitive_config_values={('celery', 'celery_result_backend')}):
            conf.remove_option('celery', 'result_backend')
            with conf_vars({('celery', 'celery_result_backend_cmd'): '/bin/echo 99'}):
                with pytest.warns(DeprecationWarning):
                    tmp = None
                    if 'AIRFLOW__CELERY__RESULT_BACKEND' in os.environ:
                        tmp = os.environ.pop('AIRFLOW__CELERY__RESULT_BACKEND')
                    assert conf.getint('celery', 'result_backend') == 99
                    if tmp:
                        os.environ['AIRFLOW__CELERY__RESULT_BACKEND'] = tmp

    def test_deprecated_values_from_conf(self):
        test_conf = AirflowConfigParser(
            default_config="""
[core]
executor=SequentialExecutor
[database]
sql_alchemy_conn=sqlite://test
"""
        )
        # Guarantee we have deprecated settings, so we test the deprecation
        # lookup even if we remove this explicit fallback
        test_conf.deprecated_values = {
            'core': {'hostname_callable': (re.compile(r':'), r'.', '2.1')},
        }
        test_conf.read_dict({'core': {'hostname_callable': 'airflow.utils.net:getfqdn'}})

        with pytest.warns(FutureWarning):
            test_conf.validate()
            assert test_conf.get('core', 'hostname_callable') == 'airflow.utils.net.getfqdn'

    @pytest.mark.parametrize(
        "old, new",
        [
            (
                ("api", "auth_backend", "airflow.api.auth.backend.basic_auth"),
                (
                    "api",
                    "auth_backends",
                    "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session",
                ),
            ),
            (
                ("core", "sql_alchemy_conn", "postgres+psycopg2://localhost/postgres"),
                ("database", "sql_alchemy_conn", "postgresql://localhost/postgres"),
            ),
        ],
    )
    def test_deprecated_env_vars_upgraded_and_removed(self, old, new):
        test_conf = AirflowConfigParser(
            default_config="""
[core]
executor=SequentialExecutor
[database]
sql_alchemy_conn=sqlite://test
"""
        )
        old_section, old_key, old_value = old
        new_section, new_key, new_value = new
        old_env_var = test_conf._env_var_name(old_section, old_key)
        new_env_var = test_conf._env_var_name(new_section, new_key)

        with pytest.warns(FutureWarning):
            with unittest.mock.patch.dict('os.environ', **{old_env_var: old_value}):
                # Can't start with the new env var existing...
                os.environ.pop(new_env_var, None)

                test_conf.validate()
                assert test_conf.get(new_section, new_key) == new_value
                # We also need to make sure the deprecated env var is removed
                # so that any subprocesses don't use it in place of our updated
                # value.
                assert old_env_var not in os.environ
                # and make sure we track the old value as well, under the new section/key
                assert test_conf.upgraded_values[(new_section, new_key)] == old_value

    @pytest.mark.parametrize(
        "conf_dict",
        [
            {},  # Even if the section is absent from config file, environ still needs replacing.
            {'core': {'hostname_callable': 'airflow.utils.net.getfqdn'}},
        ],
    )
    def test_deprecated_values_from_environ(self, conf_dict):
        def make_config():
            test_conf = AirflowConfigParser(
                default_config="""
[core]
executor=SequentialExecutor
[database]
sql_alchemy_conn=sqlite://test
"""
            )
            # Guarantee we have a deprecated setting, so we test the deprecation
            # lookup even if we remove this explicit fallback
            test_conf.deprecated_values = {
                'core': {'hostname_callable': (re.compile(r':'), r'.', '2.1')},
            }
            test_conf.read_dict(conf_dict)
            test_conf.validate()
            return test_conf

        with pytest.warns(FutureWarning):
            with unittest.mock.patch.dict(
                'os.environ', AIRFLOW__CORE__HOSTNAME_CALLABLE='airflow.utils.net:getfqdn'
            ):
                test_conf = make_config()
                assert test_conf.get('core', 'hostname_callable') == 'airflow.utils.net.getfqdn'

        with reset_warning_registry():
            with warnings.catch_warnings(record=True) as warning:
                with unittest.mock.patch.dict(
                    'os.environ',
                    AIRFLOW__CORE__HOSTNAME_CALLABLE='CarrierPigeon',
                ):
                    test_conf = make_config()
                    assert test_conf.get('core', 'hostname_callable') == 'CarrierPigeon'
                    assert [] == warning

    def test_deprecated_funcs(self):
        for func in [
            'load_test_config',
            'get',
            'getboolean',
            'getfloat',
            'getint',
            'has_option',
            'remove_option',
            'as_dict',
            'set',
        ]:
            with mock.patch(f'airflow.configuration.conf.{func}') as mock_method:
                with pytest.warns(DeprecationWarning):
                    getattr(configuration, func)()
                mock_method.assert_called_once()

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_config(self, display_source: bool):
        with use_config(config="deprecated.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=False,
                include_cmds=False,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') == (
                ('mysql://', "airflow.cfg") if display_source else 'mysql://'
            )
            # database should be None because the deprecated value is set in config
            assert cfg_dict['database'].get('sql_alchemy_conn') is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == 'mysql://'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {"AIRFLOW__CORE__SQL_ALCHEMY_CONN": "postgresql://"}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_both_env_and_config(self, display_source: bool):
        with use_config(config="deprecated.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=False,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') == (
                ('postgresql://', "env var") if display_source else 'postgresql://'
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict['database'].get('sql_alchemy_conn') is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == 'postgresql://'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {"AIRFLOW__CORE__SQL_ALCHEMY_CONN": "postgresql://"}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_both_env_and_config_exclude_env(
        self, display_source: bool
    ):
        with use_config(config="deprecated.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=False,
                include_cmds=False,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') == (
                ('mysql://', "airflow.cfg") if display_source else 'mysql://'
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict['database'].get('sql_alchemy_conn') is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == 'mysql://'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {"AIRFLOW__CORE__SQL_ALCHEMY_CONN": "postgresql://"}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_env(self, display_source: bool):
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source, raw=True, display_sensitive=True, include_env=True
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') == (
                ('postgresql://', "env var") if display_source else 'postgresql://'
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict['database'].get('sql_alchemy_conn') is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == 'postgresql://'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {}, clear=True)
    def test_conf_as_dict_when_both_conf_and_env_are_empty(self, display_source: bool):
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(display_source=display_source, raw=True, display_sensitive=True)
            assert cfg_dict['core'].get('sql_alchemy_conn') is None
            # database should be taken from default because the deprecated value is missing in config
            assert cfg_dict['database'].get('sql_alchemy_conn') == (
                (f'sqlite:///{HOME_DIR}/airflow/airflow.db', "default")
                if display_source
                else f'sqlite:///{HOME_DIR}/airflow/airflow.db'
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == f'sqlite:///{HOME_DIR}/airflow/airflow.db'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_cmd_config(self, display_source: bool):
        with use_config(config="deprecated_cmd.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=True,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') == (
                ('postgresql://', "cmd") if display_source else 'postgresql://'
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict['database'].get('sql_alchemy_conn') is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == 'postgresql://'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict(
        'os.environ', {"AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD": "echo -n 'postgresql://'"}, clear=True
    )
    def test_conf_as_dict_when_deprecated_value_in_cmd_env(self, display_source: bool):
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=True,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') == (
                ('postgresql://', "cmd") if display_source else 'postgresql://'
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict['database'].get('sql_alchemy_conn') is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == 'postgresql://'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict(
        'os.environ', {"AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD": "echo -n 'postgresql://'"}, clear=True
    )
    def test_conf_as_dict_when_deprecated_value_in_cmd_disabled_env(self, display_source: bool):
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=False,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') is None
            assert cfg_dict['database'].get('sql_alchemy_conn') == (
                (f'sqlite:///{HOME_DIR}/airflow/airflow.db', 'default')
                if display_source
                else f'sqlite:///{HOME_DIR}/airflow/airflow.db'
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == f'sqlite:///{HOME_DIR}/airflow/airflow.db'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_cmd_disabled_config(self, display_source: bool):
        with use_config(config="deprecated_cmd.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_cmds=False,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') is None
            assert cfg_dict['database'].get('sql_alchemy_conn') == (
                (f'sqlite:///{HOME_DIR}/airflow/airflow.db', 'default')
                if display_source
                else f'sqlite:///{HOME_DIR}/airflow/airflow.db'
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == f'sqlite:///{HOME_DIR}/airflow/airflow.db'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {"AIRFLOW__CORE__SQL_ALCHEMY_CONN_SECRET": "secret_path'"}, clear=True)
    @mock.patch("airflow.configuration.get_custom_secret_backend")
    def test_conf_as_dict_when_deprecated_value_in_secrets(
        self, get_custom_secret_backend, display_source: bool
    ):
        get_custom_secret_backend.return_value.get_config.return_value = "postgresql://"
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_secret=True,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') == (
                ('postgresql://', "secret") if display_source else 'postgresql://'
            )
            # database should be None because the deprecated value is set in env value
            assert cfg_dict['database'].get('sql_alchemy_conn') is None
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == 'postgresql://'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch.dict('os.environ', {"AIRFLOW__CORE__SQL_ALCHEMY_CONN_SECRET": "secret_path'"}, clear=True)
    @mock.patch("airflow.configuration.get_custom_secret_backend")
    def test_conf_as_dict_when_deprecated_value_in_secrets_disabled_env(
        self, get_custom_secret_backend, display_source: bool
    ):
        get_custom_secret_backend.return_value.get_config.return_value = "postgresql://"
        with use_config(config="empty.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_secret=False,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') is None
            assert cfg_dict['database'].get('sql_alchemy_conn') == (
                (f'sqlite:///{HOME_DIR}/airflow/airflow.db', 'default')
                if display_source
                else f'sqlite:///{HOME_DIR}/airflow/airflow.db'
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == f'sqlite:///{HOME_DIR}/airflow/airflow.db'

    @pytest.mark.parametrize("display_source", [True, False])
    @mock.patch("airflow.configuration.get_custom_secret_backend")
    @mock.patch.dict('os.environ', {}, clear=True)
    def test_conf_as_dict_when_deprecated_value_in_secrets_disabled_config(
        self, get_custom_secret_backend, display_source: bool
    ):
        get_custom_secret_backend.return_value.get_config.return_value = "postgresql://"
        with use_config(config="deprecated_secret.cfg"):
            cfg_dict = conf.as_dict(
                display_source=display_source,
                raw=True,
                display_sensitive=True,
                include_env=True,
                include_secret=False,
            )
            assert cfg_dict['core'].get('sql_alchemy_conn') is None
            assert cfg_dict['database'].get('sql_alchemy_conn') == (
                (f'sqlite:///{HOME_DIR}/airflow/airflow.db', 'default')
                if display_source
                else f'sqlite:///{HOME_DIR}/airflow/airflow.db'
            )
            if not display_source:
                remove_all_configurations()
                conf.read_dict(dictionary=cfg_dict)
                os.environ.clear()
                assert conf.get('database', 'sql_alchemy_conn') == f'sqlite:///{HOME_DIR}/airflow/airflow.db'

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @conf_vars(
        {
            ("secrets", "backend"): "airflow.providers.hashicorp.secrets.vault.VaultBackend",
            ("secrets", "backend_kwargs"): '{"url": "http://127.0.0.1:8200", "token": "token"}',
        }
    )
    def test_config_from_secret_backend_caches_instance(self, mock_hvac):
        """Get Config Value from a Secret Backend"""
        _custom_secrets_backend.cache_clear()

        test_config = '''[test]
sql_alchemy_conn_secret = sql_alchemy_conn
secret_key_secret = secret_key
'''
        test_config_default = '''[test]
sql_alchemy_conn = airflow
secret_key = airflow
'''

        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        def fake_read_secret(path, mount_point, version):
            if path.endswith('sql_alchemy_conn'):
                return {
                    'request_id': '2d48a2ad-6bcb-e5b6-429d-da35fdf31f56',
                    'lease_id': '',
                    'renewable': False,
                    'lease_duration': 0,
                    'data': {
                        'data': {'value': 'fake_conn'},
                        'metadata': {
                            'created_time': '2020-03-28T02:10:54.301784Z',
                            'deletion_time': '',
                            'destroyed': False,
                            'version': 1,
                        },
                    },
                    'wrap_info': None,
                    'warnings': None,
                    'auth': None,
                }
            if path.endswith('secret_key'):
                return {
                    'request_id': '2d48a2ad-6bcb-e5b6-429d-da35fdf31f56',
                    'lease_id': '',
                    'renewable': False,
                    'lease_duration': 0,
                    'data': {
                        'data': {'value': 'fake_key'},
                        'metadata': {
                            'created_time': '2020-03-28T02:10:54.301784Z',
                            'deletion_time': '',
                            'destroyed': False,
                            'version': 1,
                        },
                    },
                    'wrap_info': None,
                    'warnings': None,
                    'auth': None,
                }

        mock_client.secrets.kv.v2.read_secret_version.side_effect = fake_read_secret

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ('test', 'sql_alchemy_conn'),
            ('test', 'secret_key'),
        }

        assert 'fake_conn' == test_conf.get('test', 'sql_alchemy_conn')
        mock_hvac.Client.assert_called_once()
        assert 'fake_key' == test_conf.get('test', 'secret_key')
        mock_hvac.Client.assert_called_once()
        _custom_secrets_backend.cache_clear()
