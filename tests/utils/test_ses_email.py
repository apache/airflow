# -*- coding: utf-8 -*-
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

import unittest
import boto3
import moto

from airflow import configuration
from airflow.contrib.utils.ses import send_ses_email


class SesEmailTest(unittest.TestCase):
    @moto.mock_ses
    def test_send_ses(self):
        configuration.conf.add_section('ses')
        configuration.conf.set('ses', 'ses_mail_from', 'someguy@whattimeisitrightnow.com')
        configuration.conf.set('ses', 'ses_region', 'us-east-1')
        boto3.client('ses', region_name='us-east-1').verify_email_identity(EmailAddress='someguy@whattimeisitrightnow.com')
        send_ses_email('whocares@example.com', 'Test', '<h1>Hello world</h1>')
