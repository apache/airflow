from airflow import configuration


dag_import_spec = {}


def import_dags():
    if configuration.has_option('core','kube_mode'):
        mode = configuration.get('core','kube_mode')
        dag_function(mode=mode)()


def dag_function(mode):
    return {
        'git': _import_git,
        'cinder': _import_cinder
    }.get(mode, _import_hostpath)


def _import_hostpath():
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
    dag_import_spec = spec


def _import_nfs():
    pass


def _import_git():
    global dag_import_spec
    git_link = configuration.get('core', 'k8s_git_link')
    revision = configuration.get('core', 'k8s_git_revision')
    spec = {'name': 'shared-data', 'gitRepo': {}}
    spec['gitRepo']['repository'] = git_link
    spec['gitRepo']['revision'] = revision
    dag_import_spec = spec
