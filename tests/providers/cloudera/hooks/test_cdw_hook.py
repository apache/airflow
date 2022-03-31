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

from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow.providers.cloudera.hooks.cdw_hook import CdwHook


def test_beeline_command_hive(mocker):
    """
    Tests whether the expected beeline command is generated from CDHHook's parameters.
    """
    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(
            conn_id='fake',
            conn_type='hive_cli',
            host='hs2-beeline.host',
            login='user',
            password='pass',
            schema='hello',
            port='10001',
            extra=None,
            uri=None,
        ),
    )
    hook = CdwHook(cli_conn_id='anything')
    beeline_command = hook.get_cli_cmd()
    assert (
        ' '.join(beeline_command) == 'beeline -u jdbc:hive2://hs2-beeline.host/hello;'
        'transportMode=http;httpPath=cliservice;ssl=true -n user -p pass '
        '--hiveconf hive.query.isolation.scan.size.threshold=0B '
        '--hiveconf hive.query.results.cache.enabled=false '
        '--hiveconf hive.auto.convert.join.noconditionaltask.size=2505397589'
    ), 'invalid beeline command'


def test_beeline_command_impala(mocker):
    """
    Tests whether the expected beeline command is generated from CDHHook's parameters.
    CdwHook will force the following by default in case of impala:
    port: 443 (regardless of setting)
    AuthMech: should be present, default 3
    """
    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(
            conn_id='fake',
            conn_type='hive_cli',
            host='impala-proxy-beeline.host',
            login='user',
            password='pass',
            schema='hello',
            port='7777',
            extra=None,
            uri=None,
        ),
    )
    hook = CdwHook(cli_conn_id='anything')
    beeline_command = hook.get_cli_cmd()
    assert (
        ' '.join(beeline_command) == 'beeline -d com.cloudera.impala.jdbc41.Driver '
        '-u jdbc:impala://impala-proxy-beeline.host:443/hello;AuthMech=3;'
        'transportMode=http;httpPath=cliservice;ssl=1 -n user -p pass'
    ), 'invalid beeline command'


def test_beeline_command_impala_custom_driver(mocker):
    """
    Tests whether the expected beeline command is generated from CDHHook's
    parameters with custom impala driver.
    """
    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(
            conn_id='fake',
            conn_type='hive_cli',
            host='impala-proxy-beeline.host',
            login='user',
            password='pass',
            schema='hello',
            port='7777',
            extra=None,
            uri=None,
        ),
    )
    custom_driver = 'com.impala.another.driver'
    hook = CdwHook(cli_conn_id='anything', jdbc_driver=custom_driver)
    beeline_command = hook.get_cli_cmd()
    assert (
        ' '.join(beeline_command) == 'beeline -d ' + custom_driver + ' '
        '-u jdbc:impala://impala-proxy-beeline.host:443/hello;AuthMech=3;'
        'transportMode=http;httpPath=cliservice;ssl=1 -n user -p pass'
    ), 'invalid beeline command'


def test_beeline_command_non_isolation(mocker):
    """
    Tests whether the expected beeline command is generated from CDHHook's parameters without isolation.
    """
    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(
            conn_id='fake',
            conn_type='hive_cli',
            host='beeline.host',
            login='user',
            password='pass',
            schema='hello',
            port='10001',
            extra=None,
            uri=None,
        ),
    )
    hook = CdwHook(cli_conn_id='anything', query_isolation=False)
    beeline_command = hook.get_cli_cmd()
    assert (
        ' '.join(beeline_command) == 'beeline -u jdbc:hive2://beeline.host/hello;'
        'transportMode=http;httpPath=cliservice;ssl=true -n user -p pass'
    ), 'invalid beeline command'
