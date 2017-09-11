"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
from airflow import configuration
import logging
import ssl
from airflow.exceptions import AirflowConfigException, AirflowException

# Broker settings.
CELERY_ACCEPT_CONTENT = ['json', 'pickle']
CELERY_EVENT_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_TASK_SERIALIZER = 'pickle'
CELERYD_PREFETCH_MULTIPLIER = 1
CELERY_ACKS_LATE = True
BROKER_URL = configuration.get('celery', 'BROKER_URL')
CELERY_RESULT_BACKEND = configuration.get('celery', 'CELERY_RESULT_BACKEND')
# Warning! CELERYD_CONCURRENCY should be modified in airflow.cfg only (if you run workers via airflow worker)
CELERYD_CONCURRENCY = configuration.getint('celery', 'CELERYD_CONCURRENCY')
CELERY_DEFAULT_QUEUE = configuration.get('celery', 'DEFAULT_QUEUE')
CELERY_DEFAULT_EXCHANGE = configuration.get('celery', 'DEFAULT_QUEUE')

celery_ssl_active = False
try:
    celery_ssl_active = configuration.getboolean('celery', 'CELERY_SSL_ACTIVE')
except AirflowConfigException as e:
    logging.warning("Celery Executor will run without SSL")

try:
    if celery_ssl_active:
        BROKER_USE_SSL = {'keyfile': configuration.get('celery', 'CELERY_SSL_KEY'),
                            'certfile': configuration.get('celery', 'CELERY_SSL_CERT'),
                            'ca_certs': configuration.get('celery', 'CELERY_SSL_CACERT'),
                            'cert_reqs': ssl.CERT_REQUIRED}
except AirflowConfigException as e:
    raise AirflowException('AirflowConfigException: CELERY_SSL_ACTIVE is True, please ensure CELERY_SSL_KEY, '
                            'CELERY_SSL_CERT and CELERY_SSL_CACERT are set')
except Exception as e:
    raise AirflowException('Exception: There was an unknown Celery SSL Error.  Please ensure you want to use '
                            'SSL and/or have all necessary certs and key.')
