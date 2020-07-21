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

from airflow.api_connexion.schemas.config_schema import Config, ConfigOption, ConfigSection, config_schema


class TestConfigSchema:
    def test_serialize(self):
        config = Config(
            sections=[
                ConfigSection(
                    name='sec1',
                    options=[
                        ConfigOption(key='apache', value='airflow'),
                        ConfigOption(key='hello', value='world'),
                    ]
                ),
                ConfigSection(
                    name='sec2',
                    options=[
                        ConfigOption(key='foo', value='bar'),
                    ]
                ),
            ]
        )
        result = config_schema.dump(config)
        expected = {
            'sections': [
                {
                    'name': 'sec1',
                    'options': [
                        {'key': 'apache', 'value': 'airflow'},
                        {'key': 'hello', 'value': 'world'},
                    ]
                },
                {
                    'name': 'sec2',
                    'options': [
                        {'key': 'foo', 'value': 'bar'},
                    ]
                },
            ]
        }
        assert result == expected
