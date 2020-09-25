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

import datetime
import unittest

import pendulum

from airflow.utils import timezone

CET = pendulum.tz.timezone("Europe/Paris")
EAT = pendulum.tz.timezone('Africa/Nairobi')  # Africa/Nairobi
ICT = pendulum.tz.timezone('Asia/Bangkok')  # Asia/Bangkok
UTC = timezone.utc


class TestTimezone(unittest.TestCase):
    def test_is_aware(self):
        self.assertTrue(timezone.is_localized(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT)))
        self.assertFalse(timezone.is_localized(datetime.datetime(2011, 9, 1, 13, 20, 30)))

    def test_is_naive(self):
        self.assertFalse(timezone.is_naive(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT)))
        self.assertTrue(timezone.is_naive(datetime.datetime(2011, 9, 1, 13, 20, 30)))

    def test_utcnow(self):
        now = timezone.utcnow()
        self.assertTrue(timezone.is_localized(now))
        self.assertEqual(now.replace(tzinfo=None), now.astimezone(UTC).replace(tzinfo=None))

    def test_convert_to_utc(self):
        naive = datetime.datetime(2011, 9, 1, 13, 20, 30)
        utc = datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=UTC)
        self.assertEqual(utc, timezone.convert_to_utc(naive))

        eat = datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT)
        utc = datetime.datetime(2011, 9, 1, 10, 20, 30, tzinfo=UTC)
        self.assertEqual(utc, timezone.convert_to_utc(eat))

    def test_make_naive(self):
        self.assertEqual(
            timezone.make_naive(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT), EAT),
            datetime.datetime(2011, 9, 1, 13, 20, 30))
        self.assertEqual(
            timezone.make_naive(datetime.datetime(2011, 9, 1, 17, 20, 30, tzinfo=ICT), EAT),
            datetime.datetime(2011, 9, 1, 13, 20, 30))

        with self.assertRaises(ValueError):
            timezone.make_naive(datetime.datetime(2011, 9, 1, 13, 20, 30), EAT)

    def test_make_aware(self):
        self.assertEqual(
            timezone.make_aware(datetime.datetime(2011, 9, 1, 13, 20, 30), EAT),
            datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT))
        with self.assertRaises(ValueError):
            timezone.make_aware(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT), EAT)
