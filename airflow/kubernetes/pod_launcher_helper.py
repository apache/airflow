import kubernetes.client.models as k8s  # noqa

from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.kubernetes.pod import Port


def extract_env_vars(env_vars):
    """

    @param env_vars:
    @type env_vars: list
    @return: result
    @rtype: dict
    """
    result = {}
    for e in env_vars:
        env_var = e  # type: k8s.V1EnvVar
        result[env_var.name] = env_var.value
    return result


def extract_volumes(volumes):
    result = []
    for v in volumes:
        volume = v  # type: k8s.V1Volume
        result.append(Volume(name=volume.name, configs=volume.__dict__))
    return result


def extract_volume_mounts(volume_mounts):
    result = []
    for v in volume_mounts:
        volume = v  # type: k8s.V1VolumeMount
        result.append(VolumeMount(name=volume.name,
                                  mount_path=volume.mount_path,
                                  sub_path=volume.sub_path,
                                  read_only=volume.read_only))

    return result

def extract_ports(ports):
    result = []
    for p in ports:
        port = p  # type: k8s.V1ContainerPort
        result.append(Port(name=port.name, container_port=port.container_port))
    return result
