# Airflow Operator Custom Resource (API)
The Airflow Operator uses these [CustomResourceDefinitions](https://kubernetes.io/docs/concepts/api-n/custom-resources/):

`AirflowBase` includes MySQL, UI, NFS(DagStore).  
`AirflowCluster` includes Airflow Scheduler, Workers, Redis.  

Multiple `AirflowCluster` could use the same `AirflowBase`. The way custom resources are defined allows multi-single-tenant (multiple single users) usecases, where users use different airflow plugins (opeartors, packages etc) in their set
up. This improves cluster utilization and provide multiple users (in same trust domain) with some isolation.

## AirflowBase API
 
| **Field** | **json field**| **Type** | **Info** |
| --- | --- | --- | --- |
| Spec | `spec` | [AirflowBaseSpec](#AirflowBaseSpec) | The specfication for Airflow Base cusotm resource |
| Status | `status` | AirflowBaseStatus | The status for the custom resource |

#### AirflowBaseSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| NodeSelector | map[string]string | `nodeSelector` | [Selector](https://kubernetes.io/docs/concepts/configuration/assign-pod-node) for fitting pods to nodes whose labels match the selector |
| Affinity | \*corev1.Affinity | `affinity` | Define scheduling constraints for pods |
| Annotations | map[string]string | `annotations` | Custom annotations to be added to the pods |
| Labels | map[string]string | `labels` | Custom labels to be added to the pods |
| MySQL | \*MySQLSpec | `mysql` | Spec for MySQL component |
| Storage | \*NFSStoreSpec | `storage` | Spec for NFS component |
| UI | \*AirflowUISpec | `ui` | Spec for Airflow UI component |
| SQLProxy | \*SQLProxySpec | `sqlproxy` | Spec for SQLProxy component. Ignored if SQL(MySQLSpec) is specified |


#### MySQLSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image` | Image defines the MySQL Docker image name |
| Version | string  | `version` | Version defines the MySQL Docker image version |
| Replicas | int32 | `replicas` | Replicas defines the number of running MySQL instances in a cluster |
| VolumeClaimTemplate | \*corev1.PersistentVolumeClaim | `volumeClaimTemplate` | VolumeClaimTemplate allows a user to specify volume claim for MySQL Server files |
| BackupVolumeClaimTemplate | \*corev1.PersistentVolumeClaim | `backupVolumeClaimTemplate` | BackupVolumeClaimTemplate allows a user to specify a volume to temporarily store the data for a backup prior to it being shipped to object storage |
| Operator | bool  | `operator` | Flag when True generates MySQLOperator CustomResource to be handled by MySQL Operator If False, a StatefulSet with 1 replica is created (not for production setups) |
| Backup | \*MySQLBackup | `backup` | Spec defining the Backup Custom Resource to be handled by MySQLOperator Ignored when Operator is False |
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods |
| Options | map[string]string | ` ` | command line options for mysql |


#### MySQLBackup 
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Schedule | string | `schedule` | Schedule is the cron string used to schedule backup|
| Storage | StorageSpec | `storage` | Storage has the s3 compatible storage spec|


##### StorageSpec 
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| StorageProvider | string | `storageprovider` | Provider is the storage type used for backup and restore e.g. s3, oci-s3-compat, aws-s3, gce-s3, etc |
| SecretRef | \*corev1.LocalObjectReference | `secretRef` | SecretRef is a reference to the Kubernetes secret containing the configuration for uploading the backup to authenticated storage |
| Config | map[string]string | `config` | Config is generic string based key-value map that defines non-secret configuration values for uploading the backup to storage w.r.t the configured storage provider |

#### NFSStoreSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image` | Image defines the NFS Docker image.|
| Version | string | `version` | Version defines the NFS Server Docker image version.|
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods.|
| Volume | \*corev1.PersistentVolumeClaim | `volumeClaimTemplate` | Volume allows a user to specify volume claim template to be used for fileserver|

#### SQLProxySpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image` | Image defines the SQLProxy Docker image name|
| Version | string | `version` | Version defines the SQL Proxy docker image version.  example: myProject:us-central1:myInstance=tcp:3306|
| Project | string | `project` | Project defines the SQL instance project|
| Region | string | `region` | Region defines the SQL instance region|
| Instance | string | `instance` | Instance defines the SQL instance name|
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods.|


#### AirflowBaseStatus
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| ObservedGeneration | int64 | `observedGeneration` |	ObservedGeneration is the last generation of the AirflowBase as observed by the controller |
| MySQL | ComponentStatus | `mysql` | MySQL is the status of the MySQL component |
| UI | ComponentStatus | `ui` | UI is the status of the Airflow UI component |
| Storage | ComponentStatus | `storage` | Storage is the status of the NFS component |
| SQLProxy | ComponentStatus | `sqlproxy` | SQLProxy is the status of the SQLProxy component |
| LastError | string | `lasterror` | LastError |
| Status | string | `status`| 	Reaedy or Pending |

##  AirflowCluster

| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Spec  | AirflowClusterSpec | `spec` | |
| Status | AirflowClusterStatus | `status` | |

#### AirflowClusterSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| NodeSelector | map[string]string | `nodeSelector` | [Selector](https://kubernetes.io/docs/concepts/configuration/assign-pod-node) for fitting pods to nodes whose labels match the selector |
| Affinity | \*corev1.Affinity | `affinity` | Define scheduling constraints for pods. |
| Annotations | map[string]string | `annotations` | Custom annotations to be added to the pods. |
| Labels | map[string]string | `labels` | Custom labels to be added to the pods. |
| Executor | string | `executor` | Airflow Executor desired: local,celery,kubernetes |
| Redis | \*RedisSpec | `redis` | Spec for Redis component. |
| Scheduler | \*SchedulerSpec | `scheduler` | Spec for Airflow Scheduler component. |
| Worker | \*WorkerSpec | `worker` | Spec for Airflow Workers |
| UI | \*AirflowUISpec | `ui` | Spec for Airflow UI component. |
| Flower | \*FlowerSpec | `flower` | Spec for Flower component. |
| DAGs | \*DagSpec | `dags` | Spec for DAG source and location |
| AirflowBaseRef | \*corev1.LocalObjectReference | `airflowbase` | AirflowBaseRef is a reference to the AirflowBase CR |

#### AirflowClusterStatus
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| ObservedGeneration | int64 | `observedGeneration"` | ObservedGeneration is the last generation of the AirflowCluster as observed by the controller. |
| Redis | ComponentStatus | `redis` | Redis is the status of the Redis component |
| Scheduler | SchedulerStatus | `scheduler` | Scheduler is the status of the Airflow Scheduler component |
| Worker | ComponentStatus | `worker` | Worker is the status of the Workers |
| UI | ComponentStatus | `ui` | UI is the status of the Airflow UI component |
| Flower | ComponentStatus | `flower` | Flower is the status of the Airflow UI component |
| LastError | string | `lasterror` | LastError |
| Status | string | `status` | Status |

#### RedisSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image"` | Image defines the Redis Docker image name |
| Operator | bool | `operator` | Version defines the Redis Docker image version.  Flag when True generates RedisReplica CustomResource to be handled by Redis Operator If False, a StatefulSet with 1 replica is created |
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods. |
| VolumeClaimTemplate | \*corev1.PersistentVolumeClaim | `volumeClaimTemplate` | VolumeClaimTemplate allows a user to specify volume claim for MySQL Server files |
| AdditionalArgs | string | `additionalargs` | AdditionalArgs for redis-server |

#### RedisSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image"` | Image defines the Redis Docker image name |
| Version | string | `version"` | Version defines the Redis Docker image version. |
| Operator | bool | `operator` | Flag when True generates RedisReplica CustomResource to be handled by Redis Operator If False, a StatefulSet with 1 replica is created |
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods. |
| VolumeClaimTemplate | \*corev1.PersistentVolumeClaim | `volumeClaimTemplate` | VolumeClaimTemplate allows a user to specify volume claim for MySQL Server files |
| AdditionalArgs | string | `additionalargs` | AdditionalArgs for redis-server |

#### FlowerSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image"` | Image defines the Flower Docker image. |
| Version | string | `version"` | Version defines the Flower Docker image version. |
| Replicas | int32 | `replicas` | Replicas defines the number of running Flower instances in a cluster |
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods. |

#### SchedulerSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image"` | Image defines the Airflow custom server Docker image. |
| Version | string | `version"` | Version defines the Airflow Docker image version |
| DBName | string | `database"` | DBName defines the Airflow Database to be used |
| DBUser | string | `dbuser"` | DBUser defines the Airflow Database user to be used |
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods. |

#### WorkerSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image"` | Image defines the Airflow worker Docker image. |
| Version | string | `version"` | Version defines the Airflow worker Docker image version |
| Replicas | int32 | `replicas` | Replicas is the count of number of workers |
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods. |

#### AirflowUISpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Image | string | `image` | Image defines the AirflowUI Docker image.|
| Version | string | `version` | Version defines the AirflowUI Docker image version.|
| Replicas | int32 | `replicas` | Replicas defines the number of running Airflow UI instances in a cluster|
| Resources | corev1.ResourceRequirements | `resources` | Resources is the resource requests and limits for the pods.|

#### GCSSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Bucket | string | `bucket` | Bucket describes the GCS bucket |
| Once | bool | `once` | Once syncs initially and quits (use init container instead of sidecar) |

#### GitSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Repo | string | `repo,"` | Repo describes the http/ssh uri for git repo |
| Branch | string | `branch` | Branch describes the branch name to be synced |
| Rev | string | `rev` | Rev is the git hash to be used for syncing |
| User | string | `user` | User for git access |
| Once | bool | `once` | Once syncs initially and quits (use init container instead of sidecar) |
| CredSecretRef | \*corev1.LocalObjectReference | `cred` | Reference to a Secret that has git credentials in field `password`. It is injected as env `GIT_SYNC_PASSWORD` in [git-sync](https://github.com/kubernetes/git-sync) container.Refer to how the `password` is [used in git-sync](https://github.com/kubernetes/git-sync/blob/40e188fb26ecad2d8174e486fc104939c6b1271d/cmd/git-sync/main.go#L477:6) |

#### DagSpec
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| DagSubdir | string | `subdir` | DagSubdir is the directory under source where the dags are present |
| Git | \*GitSpec | `git` | GitSpec defines details to pull DAGs from a git repo using github.com/kubernetes/git-sync sidecar |
| NfsPV | \*corev1.PersistentVolumeClaim | `nfspv` | NfsPVSpec |
| Storage | \*StorageSpec | `storage` | Storage has s3 compatible storage spec for copying files from |
| GCS | \*GCSSpec | `gcs` | Gcs config which uses storage spec |

#### SchedulerStatus
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Resources | ComponentStatus | `resources` | Status is a string describing Scheduler status |
| DagCount | int32 | `dagcount` | DagCount is a count of number of Dags observed |
| RunCount | int32 | `runcount` | RunCount is a count of number of Dag Runs observed |

## Common

#### ComponentStatus
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| STS | []StsStatus | `sts` | StatefulSet status|
| SVC | []SvcStatus | `svc` | Service status|
| PDB | []PdbStatus | `pdb` | PDB status|
| LastError | string | `lasterror` | LastError|
| Status | string | `status` | Status|

#### StsStatus 
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Link | string | `link` | Link to sts|
| Name | string | `name` | Name of sts|
| Status | string | `status` | Status to rsrc|
| Replicas | int32 | `replicas` | Replicas defines the no of MySQL instances desired|
| ReadyReplicas | int32 | `readycount` | ReadyReplicas defines the no of MySQL instances that are ready|
| CurrentReplicas | int32 | `currentcount` | CurrentReplicas defines the no of MySQL instances that are created|

#### SvcStatus
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Link | string | `link` | Link to rsrc|
| Name | string | `name` | service name|
| Status | string | `status` | Status to rsrc|

#### PdbStatus
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| Link | string | `link` | Link to rsrc|
| Name | string | `name` | Name of pdb|
| Status | string | `status` | Status to rsrc|
| CurrentHealthy | int32 | `currenthealthy` | currentHealthy|
| DesiredHealthy | int32 | `desiredhealthy` | desiredHealthy|

#### ResourceRequests
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| CPU | string | `cpu` | Cpu is the amount of CPU requested for a pod.|
| Memory | string | `memory` | Memory is the amount of RAM requested for a Pod.|
| Disk | string | `disk` | Disk is the amount of Disk requested for a pod.|
| DiskStorageClass | string | `diskStorageClass` | DiskStorageClass is the storage class for Disk.  Disk must be present or this field is invalid.|

#### ResourceLimits
| **Field** | **Type** | **json field** | **Info** |
| --- | --- | --- | --- |
| CPU | string | `cpu` | Cpu is the CPU limit for a pod.|
| Memory | string | `memory` | Memory is the RAM limit for a pod.|
