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

from backports.configparser import DuplicateSectionError
from datetime import timedelta
import signal
import sys
import time
from threading import Thread, Event
import unittest

import boto3
import moto
from python_dynamodb_lock.python_dynamodb_lock import (
    DynamoDBLockClient,
    DynamoDBLockError)

from airflow import configuration
from airflow.configuration import AirflowConfigException
from airflow.jobs import SchedulerJob
from airflow.utils.lock import acquire_lock, initialize


class TestTimeout(Exception):
    pass


class test_timeout:
    """
    Context manager that raises a TestTimeout exception if the
    code it wraps takes longer than the number of seconds configured.
    """
    def __init__(self, seconds, error_message=None):
        if error_message is None:
            error_message = 'test timed out after {}s.'.format(seconds)
            self.seconds = seconds
            self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TestTimeout(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)


class TestNullLocks(unittest.TestCase):
    def setUp(self):
        try:
            configuration.conf.add_section('scheduler')
        except DuplicateSectionError:
            pass
        configuration.conf.remove_option('scheduler', 'lock_backend')

    def test_null_lock(self):
        """
        Tests that a null lock is a no-op
        """
        # acquire the lock within a lock. Since default behavior is to no op the lock, this should pass
        # If it locks, we timeout after a second.
        with test_timeout(1):
            with acquire_lock():
                with acquire_lock():
                    pass

    def test_null_initialize(self):
        """
        Tests that initializing the null lock is a no-op
        """
        initialize()


class TestDynamoDbLock(unittest.TestCase):
    def _python_version_supported(self):
        """
        Return true if dynamodb lock supports the current python version, else false.
        """
        version = sys.version_info
        major = version[0]
        minor = version[1]

        return not (major < 3 or (major == 3 and minor < 6))

    def setUp(self):
        if not self._python_version_supported():
            self.skipTest("DynamoDB lock tests only apply to python >= 3.6")
            return

        self.table_name = 'airflow-lock-test'
        self.ddb_lock_section = 'dynamodb_lock'
        try:
            configuration.conf.add_section('scheduler')
        except DuplicateSectionError:
            pass
        try:
            configuration.conf.add_section(self.ddb_lock_section)
        except DuplicateSectionError:
            pass
        configuration.conf.set('scheduler', 'lock_backend', 'airflow.utils.dynamodb_lock')
        configuration.conf.remove_option(self.ddb_lock_section, 'LOCK_NAME')
        configuration.conf.remove_option(self.ddb_lock_section, 'ACCESS_KEY')
        configuration.conf.remove_option(self.ddb_lock_section, 'SECRET_KEY')
        configuration.conf.remove_option(self.ddb_lock_section, 'AWS_REGION')
        configuration.conf.set(self.ddb_lock_section, 'TABLE_NAME', self.table_name)

    def test_ddb_lock_no_region_fail(self):
        """
        Tests that an error is raised if the dynamodb lock backend is configured
        without a region when acquire_lock is used.
        """
        with self.assertRaises(AirflowConfigException):
            with acquire_lock():
                pass

    def test_ddb_lock_initialize_no_region(self):
        """
        Tests that an error is raised if the dynamodb lock backend is configured
        without a region when initialize is used.
        """
        with self.assertRaises(AirflowConfigException):
            initialize()

    @moto.mock_dynamodb2
    def test_ddb_lock_initialize(self):
        """
        Tests that initialize creates the dyanamodb table for the dynamodb lock backend
        """
        access_key = 'foo'
        secret_key = 'bar'
        configuration.conf.set(self.ddb_lock_section, 'AWS_REGION', 'us-east-1')
        configuration.conf.set(self.ddb_lock_section, 'ACCESS_KEY', access_key)
        configuration.conf.set(self.ddb_lock_section, 'SECRET_KEY', secret_key)
        initialize()
        table = boto3.client('dynamodb',
                             region_name='us-east-1',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key).describe_table(TableName=self.table_name)
        self.assertIsNotNone(table)

        # tests that initialize() when the table is created is a no-op
        initialize()

    @moto.mock_dynamodb2
    def test_ddb_lock(self):
        """
        Tests basic functionality with dynamodb lock backend
        """
        access_key = 'foo'
        secret_key = 'bar'
        DynamoDBLockClient.create_dynamodb_table(boto3.client('dynamodb',
                                                              region_name='us-east-1',
                                                              aws_access_key_id=access_key,
                                                              aws_secret_access_key=secret_key),
                                                 table_name=self.table_name)
        configuration.conf.set(self.ddb_lock_section, 'AWS_REGION', 'us-east-1')
        configuration.conf.set(self.ddb_lock_section, 'ACCESS_KEY', access_key)
        configuration.conf.set(self.ddb_lock_section, 'SECRET_KEY', secret_key)
        with acquire_lock():
            pass

    @moto.mock_dynamodb2
    def test_ddb_acquire_lock_when_locked(self):
        """
        Tests that taking the dynamodb lock prevents subsequent access to the lock.
        """
        access_key = 'foo'
        secret_key = 'bar'
        DynamoDBLockClient.create_dynamodb_table(boto3.client('dynamodb',
                                                              region_name='us-east-1',
                                                              aws_access_key_id=access_key,
                                                              aws_secret_access_key=secret_key),
                                                 table_name=self.table_name)
        configuration.conf.set(self.ddb_lock_section, 'AWS_REGION', 'us-east-1')
        configuration.conf.set(self.ddb_lock_section, 'ACCESS_KEY', access_key)
        configuration.conf.set(self.ddb_lock_section, 'SECRET_KEY', secret_key)

        # take a lock
        dblockclient = DynamoDBLockClient(boto3.resource('dynamodb',
                                                         region_name='us-east-1',
                                                         aws_access_key_id=access_key,
                                                         aws_secret_access_key=secret_key),
                                          table_name=self.table_name)
        dblockclient.acquire_lock('airflow')

        # Some configs for ddb locking that make testing faster:
        configuration.conf.set(self.ddb_lock_section, "RETRY_PERIOD", "0.5")
        configuration.conf.set(self.ddb_lock_section, 'RETRY_TIMEOUT', "2")

        with self.assertRaises(DynamoDBLockError):
            with acquire_lock():
                # Shouldn't happen
                pass

    @moto.mock_dynamodb2
    def test_ddb_acquire_lock_when_locked_then_released(self):
        """
        Tests that after a lock is released, another client can acquire it.
        """
        access_key = 'foo'
        secret_key = 'bar'
        DynamoDBLockClient.create_dynamodb_table(boto3.client('dynamodb',
                                                              region_name='us-east-1',
                                                              aws_access_key_id=access_key,
                                                              aws_secret_access_key=secret_key),
                                                 table_name=self.table_name)
        configuration.conf.set(self.ddb_lock_section, 'AWS_REGION', 'us-east-1')
        configuration.conf.set(self.ddb_lock_section, 'ACCESS_KEY', access_key)
        configuration.conf.set(self.ddb_lock_section, 'SECRET_KEY', secret_key)

        dblockclient = DynamoDBLockClient(boto3.resource('dynamodb',
                                                         region_name='us-east-1',
                                                         aws_access_key_id=access_key,
                                                         aws_secret_access_key=secret_key),
                                          table_name=self.table_name)

        # Acquire the lock
        lock = dblockclient.acquire_lock('airflow')

        # Some configs for ddb locking that make testing faster:
        configuration.conf.set(self.ddb_lock_section, "RETRY_PERIOD", "0.1")
        configuration.conf.set(self.ddb_lock_section, 'RETRY_TIMEOUT', "0.3")

        def basic_locking(ev):
            with acquire_lock():
                ev.set()

        # Make sure lock is engaged
        got_lock = Event()
        with self.assertRaises(DynamoDBLockError):
            basic_locking(got_lock)

        self.assertFalse(got_lock.is_set())

        got_lock = Event()
        thread = Thread(target=basic_locking, args=(got_lock,))
        thread.start()
        lock.release()
        thread.join()
        self.assertTrue(got_lock.is_set())

    @moto.mock_dynamodb2
    def test_heartbeat_timeout(self):
        """
        Tests that another client can acquire the lock if the original client
        stops and the heartbeats time out.
        """
        access_key = 'foo'
        secret_key = 'bar'
        DynamoDBLockClient.create_dynamodb_table(boto3.client('dynamodb',
                                                              region_name='us-east-1',
                                                              aws_access_key_id=access_key,
                                                              aws_secret_access_key=secret_key),
                                                 table_name=self.table_name)
        configuration.conf.set(self.ddb_lock_section, 'AWS_REGION', 'us-east-1')
        configuration.conf.set(self.ddb_lock_section, 'ACCESS_KEY', access_key)
        configuration.conf.set(self.ddb_lock_section, 'SECRET_KEY', secret_key)

        dblockclient = DynamoDBLockClient(boto3.resource('dynamodb',
                                                         region_name='us-east-1',
                                                         aws_access_key_id=access_key,
                                                         aws_secret_access_key=secret_key),
                                          heartbeat_period=timedelta(seconds=0.1),
                                          safe_period=timedelta(seconds=0.1),
                                          lease_duration=timedelta(seconds=0.5),
                                          table_name=self.table_name)
        # Represents an abandoned lock
        dblockclient.acquire_lock('airflow')
        dblockclient.close()

        # Some configs for ddb locking that make testing faster:
        configuration.conf.set(self.ddb_lock_section, "HEARBEAT_PERIOD", "0.1")
        configuration.conf.set(self.ddb_lock_section, "SAFE_PERIOD", "0.1")
        configuration.conf.set(self.ddb_lock_section, "LEASE_DURATION", "0.5")
        configuration.conf.set(self.ddb_lock_section, "RETRY_PERIOD", "0.1")
        configuration.conf.set(self.ddb_lock_section, 'RETRY_TIMEOUT', "2")

        start_time = time.time()
        end_time = None
        with acquire_lock():
            # We should be able to get a lock
            end_time = time.time()

        self.assertTrue(end_time is not None)
        self.assertTrue(end_time - start_time >= 0.5)

    @moto.mock_dynamodb2
    def test_scheduler_killed_with_callback(self):
        """
        End to end test of scheduler locking using the dynamodb backend
        """
        access_key = 'foo'
        secret_key = 'bar'
        DynamoDBLockClient.create_dynamodb_table(boto3.client('dynamodb',
                                                              region_name='us-east-1',
                                                              aws_access_key_id=access_key,
                                                              aws_secret_access_key=secret_key),
                                                 table_name=self.table_name)
        configuration.conf.set(self.ddb_lock_section, 'AWS_REGION', 'us-east-1')
        configuration.conf.set(self.ddb_lock_section, 'ACCESS_KEY', access_key)
        configuration.conf.set(self.ddb_lock_section, 'SECRET_KEY', secret_key)
        configuration.conf.set(self.ddb_lock_section, "RETRY_PERIOD", "0.1")

        dblockclient = DynamoDBLockClient(boto3.resource('dynamodb',
                                                         region_name='us-east-1',
                                                         aws_access_key_id=access_key,
                                                         aws_secret_access_key=secret_key),
                                          table_name=self.table_name)

        dblockclient.close()
        scheduler = SchedulerJob()
        scheduler_thread = Thread(target=scheduler.run)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        existing_lock = None

        with test_timeout(10):
            while existing_lock is None:
                existing_lock = dblockclient._get_lock_from_dynamodb(
                    'airflow',
                    DynamoDBLockClient._DEFAULT_SORT_KEY_VALUE)
                time.sleep(1)

            existing_lock.owner_name = 'itsminenow'
            record_version = existing_lock.record_version_number
            existing_lock.record_version_number = 'ROBBED'
            dblockclient._overwrite_existing_lock_in_dynamodb(existing_lock, record_version)
            scheduler_thread.join()
