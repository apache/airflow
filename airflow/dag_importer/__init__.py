# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from airflow import configuration


def _integrate_plugins():
    pass


dag_import_spec = {}


def import_dags():
    logging.info("importing dags")
    if configuration.has_option('core', 'k8s_mode'):
        mode = configuration.get('core', 'k8s_mode')
        dag_import_func(mode)()
    else:
        _import_hostpath()


def dag_import_func(mode):
    return {
        'git': _import_git,
        'cinder': _import_cinder,
    }.get(mode, _import_hostpath)


def _import_hostpath():

    logging.info("importing dags locally")
    spec = {'name': 'shared-data', 'hostPath': {}}
    spec['hostPath']['path'] = '/tmp/dags'
    global dag_import_spec
    dag_import_spec = spec


def _import_cinder():
    '''
    kind: StorageClass
    apiVersion: storage.k8s.io/v1
    metadata:
        name: gold
    provisioner: kubernetes.io/cinder
    parameters:
        type: fast
    availability: nova
    :return: 
    '''
    global dag_import_spec
    spec = {}

    spec['kind'] = 'StorageClass'
    spec['apiVersion'] = 'storage.k8s.io/v1'
    spec['metatdata']['name'] = 'gold'
    spec['provisioner'] = 'kubernetes.io/cinder'
    spec['parameters']['type'] = 'fast'
    spec['availability'] = 'nova'


def _import_git():
    logging.info("importing dags from github")
    global dag_import_spec
    git_link = configuration.get('core', 'k8s_git_link')
    spec = {'name': 'shared-data', 'gitRepo': {}}
    spec['gitRepo']['repository'] = git_link
    if configuration.has_option('core','k8s_git_revision'):
        revision = configuration.get('core', 'k8s_git_revision')
        spec['gitRepo']['revision'] = revision
    dag_import_spec = spec
