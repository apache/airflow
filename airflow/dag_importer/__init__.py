from airflow import AirflowException
import logging
print("starting dag integration")
logging.info("starting dag integ")
from airflow import configuration

def _integrate_plugins():
    pass

def GetDagImporter():
    global DAG_IMPORTER

    _integrate_plugins()
    dag_importer_path = DAG_IMPORTER.split('.')

    if dag_importer_path[0] in globals():
        dag_importer_plugin = globals()[dag_importer_path[0]].__dict__[dag_importer_path[1]]()
        return dag_importer_plugin
    else:
        raise AirflowException("dag importer {0} not supported.".format(DAG_IMPORTER))

dag_import_spec = {}


def import_dags():
    # self._import_cinder()
    _import_hostpath()
    # _import_git()


def _import_hostpath():
    logging.info("importing dags locally")
    global dag_import_spec
    spec = {'name': 'shared-data', 'hostPath': {}}
    spec['hostPath']['path'] = '/tmp/dags'
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
    revision = configuration.get('core', 'k8s_git_revision')
    spec = {'name': 'shared-data', 'gitRepo': {}}
    spec['gitRepo']['repository'] = git_link
    spec['gitRepo']['revision'] = revision
    dag_import_spec = spec

import_dags()
