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

from collections import namedtuple
import contextlib
from datetime import timedelta
import logging
import sys

import boto3
from python_dynamodb_lock import python_dynamodb_lock

from airflow import configuration
from airflow.configuration import AirflowConfigException
from airflow.exceptions import AirflowException


logger = logging.getLogger(__name__)


def _check_python_version():
    """
    The python_dynamodb_lock only supports python>= 3.6.
    Checks the version of python and raises an exception if it
    is too low.
    """
    version = sys.version_info
    major = version[0]
    minor = version[1]

    if major < 3 or (major == 3 and minor < 6):
        raise AirflowException('airflow.utils.dynamodb_lock backend requires python >= 3.6')


def _get_configs():
    ddb_lock_section = 'dynamodb_lock'
    table_name = 'airflow_scheduler_lock' if \
        not configuration.conf.has_option(ddb_lock_section, 'TABLE_NAME') \
        else configuration.conf.get(ddb_lock_section, 'TABLE_NAME', fallback='airflow_scheduler_lock')
    lock_name = 'airflow' if not configuration.conf.has_option(ddb_lock_section, 'LOCK_NAME') \
        else configuration.conf.get(ddb_lock_section, 'LOCK_NAME', fallback='airflow')
    access_key = None if not configuration.conf.has_option(ddb_lock_section, 'ACCESS_KEY') \
        else configuration.conf.get(ddb_lock_section, 'ACCESS_KEY', fallback=None)
    secret_key = None if not configuration.conf.has_option(ddb_lock_section, 'SECRET_KEY') \
        else configuration.conf.get(ddb_lock_section, 'SECRET_KEY', fallback=None)
    aws_region = None if not configuration.conf.has_option(ddb_lock_section, 'AWS_REGION') \
        else configuration.conf.get(ddb_lock_section, 'AWS_REGION', fallback=None)

    # How often to update DynamoDB to note that the
    # instance is still running. It is recommended to make this at least 4 times smaller
    # than the leaseDuration. Defaults to 5 seconds.
    heartbeat_period = 5 if not configuration.conf.has_option(ddb_lock_section, 'HEARTBEAT_PERIOD') \
        else configuration.conf.getfloat(ddb_lock_section, 'HEARTBEAT_PERIOD')

    # How long is it okay to go without a heartbeat before
    # considering a lock to be in "danger". Defaults to 20 seconds
    safe_period = 20 if not configuration.conf.has_option(ddb_lock_section, 'SAFE_PERIOD') \
        else configuration.conf.getfloat(ddb_lock_section, 'SAFE_PERIOD')

    # The length of time that the lease for the lock
    # will be granted for. i.e. if there is no heartbeat for this period of time, then
    # the lock will be considered as expired. Defaults to 30 seconds.
    lease_duration = 30 if not configuration.conf.has_option(ddb_lock_section, 'LEASE_DURATION') \
        else configuration.conf.getfloat(ddb_lock_section, 'LEASE_DURATION')

    # The fallback expiry timestamp to allow DynamoDB
    # to cleanup old locks after a server crash. This value should be significantly larger
    # than the _lease_duration to ensure that clock-skew etc. are not an issue. Defaults
    # to 1 hour.
    expiry_period = 3600 if not configuration.conf.has_option(ddb_lock_section, 'EXPIRY_PERIOD') \
        else configuration.conf.getint(ddb_lock_section, 'EXPIRY_PERIOD')

    # The number of heartbeats to execute per second (per node) - this
    # will have direct correlation to DynamoDB provisioned throughput for writes. If set
    # to -1, the client will distribute the heartbeat calls evenly over the _heartbeat_period
    # which uses lower throughput for smaller number of locks. However, if you want a more
    # deterministic heartbeat-call-rate, then specify an explicit TPS value. Defaults to -1.
    heartbeat_tps = 5 if not configuration.conf.has_option(ddb_lock_section, 'HEARTBEAT_TPS') \
        else configuration.conf.getfloat(ddb_lock_section, 'HEARTBEAT_TPS')

    # If the lock is not immediately available, how long
    # should we wait between retries? Defaults to heartbeat_period.
    retry_period = heartbeat_period if not configuration.conf.has_option(ddb_lock_section, 'RETRY_PERIOD') \
        else configuration.conf.getfloat(ddb_lock_section, 'RETRY_PERIOD')

    # If the lock is not available for an extended period,
    # how long should we keep trying before giving up and timing out? This value should be set
    # higher than the lease_duration to ensure that other clients can pick up locks abandoned
    # by one client. Defaults to indefinite retries without ever exiting, so that any failure in
    # another scheduler can be quickly recovered into the one waiting for the lock.
    retry_timout = None if not configuration.conf.has_option(ddb_lock_section, 'RETRY_TIMEOUT') \
        else configuration.conf.getfloat(ddb_lock_section, 'RETRY_TIMEOUT')

    if not aws_region:
        raise AirflowConfigException('dynamodb_lock.AWS_REGION must be set to use locking')

    Configs = namedtuple('Configs', [
        'table_name',
        'lock_name',
        'access_key',
        'secret_key',
        'aws_region',
        'heartbeat_period',
        'safe_period',
        'lease_duration',
        'expiry_period',
        'heartbeat_tps',
        'retry_period',
        'retry_timout'
    ])

    return Configs(
        table_name=table_name,
        lock_name=lock_name,
        access_key=access_key,
        secret_key=secret_key,
        aws_region=aws_region,
        heartbeat_period=heartbeat_period,
        safe_period=safe_period,
        lease_duration=lease_duration,
        expiry_period=expiry_period,
        heartbeat_tps=heartbeat_tps,
        retry_period=retry_period,
        retry_timout=retry_timout
    )


def initialize():
    """
    Initializes the lock backend by creating the table if necessary
    """
    _check_python_version()
    configs = _get_configs()

    # get a reference to the DynamoDB resource
    dynamodb_resource = boto3.client('dynamodb',
                                     region_name=configs.aws_region,
                                     aws_access_key_id=configs.access_key,
                                     aws_secret_access_key=configs.secret_key)
    # create the lock db
    try:
        python_dynamodb_lock.DynamoDBLockClient.create_dynamodb_table(
            dynamodb_resource,
            table_name=configs.table_name)
        logger.info('Created dynamodb table {0}'.format(configs.table_name))
    except dynamodb_resource.exceptions.ResourceInUseException:
        logger.info('The dynamodb table {0} already exists'.format(configs.table_name))


@contextlib.contextmanager
def acquire_lock(callback=None):
    _check_python_version()
    configs = _get_configs()

    # get a reference to the DynamoDB resource
    dynamodb_resource = boto3.resource('dynamodb',
                                       region_name=configs.aws_region,
                                       aws_access_key_id=configs.access_key,
                                       aws_secret_access_key=configs.secret_key)
    # create the lock-client
    lock_client = python_dynamodb_lock.DynamoDBLockClient(
        dynamodb_resource,
        table_name=configs.table_name,
        heartbeat_period=timedelta(seconds=configs.heartbeat_period),
        safe_period=timedelta(seconds=configs.safe_period),
        lease_duration=timedelta(seconds=configs.lease_duration),
        expiry_period=timedelta(seconds=configs.expiry_period),
        heartbeat_tps=configs.heartbeat_tps)

    # If no retry timeout, we want infinite timeout.
    # Set to 5214 weeks which is 100 years, which is essentially infinite
    # If you don't bounce your servers by then something is horribly wrong.
    retry_timeout_timedelta = timedelta(seconds=configs.retry_timout) \
        if configs.retry_timout else timedelta(weeks=5214)
    with lock_client.acquire_lock(
            configs.lock_name,
            retry_period=timedelta(seconds=configs.retry_period),
            retry_timeout=retry_timeout_timedelta,
            app_callback=callback):
        yield
