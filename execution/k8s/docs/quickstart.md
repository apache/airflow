# Quick Start

## Deploy from GCP Marketplace
[One Click Deployment](https://pantheon.corp.google.com/marketplace/details/google/airflow-operator) from Google Cloud Marketplace to your [GKE cluster](https://cloud.google.com/kubernetes-engine/). The marketplace may not have the latest version of the operator. If you need to deploy from latest master continue reading.

## Running from source
Refer to the [Development Guide](https://github.com/GoogleCloudPlatform/airflow-operator/blob/master/docs/development.md).

## Installing on any cluster
Ensure kubeconfig points to your cluster.
Due to a [known issue](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control#defining_permissions_in_a_role) in GKE, you will need to first grant yourself cluster-admin privileges before you can create custom roles and role bindings on a GKE cluster versioned 1.6 and up.
```bash
# grant admin 
$ kubectl create clusterrolebinding cluster-admin-binding --clusterrole cluster-admin --user your-account-email
```

### Install CRDs
Install the AirflowBase and AirflowCluster CRDs.
The AirflowBase and AirflowCluster CRs result in [Application CRs](https://github.com/kubernetes-sigs/application) being created. Install Application CRD to see the Applications in the [GCP console](https://pantheon.corp.google.com/kubernetes/application)
```bash
# install Application and Airflow CRD
$ make install
```

### Build operator docker image

```bash
# First we need to build the docker image for the controller
# Set this to the name of the docker registry and image you want to use
$ export IMG=gcr.io/myproject/airflow-controller:latest 

# Build and push
$ make docker-push
```

### Deploying Airflow Operator using manifests

Installing the airflow operator creates the 'airflowop-system' namespace and creates stateful set in that namespace for the operator.

```bash
# deploy the airflow operator
$ make deploy

# follow airflow controller logs in a terminal session
$ kubectl logs -f airflowop-controller-manager-0 -n airflowop-system

# to undeploy
$ #make undeploy
```

## Create Airflow clusters using samples

The `hack/sample/` directory contains sample Airflow CRs

#### Deploy MySQL based samples

```bash
# deploy base components first
$ kubectl apply -f hack/sample/mysql-celery/base.yaml
# after 30-60s deploy cluster components 
# using celery + git as DAG source
$ kubectl apply -f hack/sample/mysql-celery/cluster.yaml
# port forward to access the UI
$ kubectl port-forward mc-cluster-airflowui-0 8080:8080
# port forward to access the Flower
$ kubectl port-forward mc-cluster-flower-0 5555:5555
# get status of the CRs
$ kubectl get airflowbase/mc-base -o yaml 
$ kubectl get airflowcluster/mc-cluster -o yaml 

# Against the same mc-base, we could deploy another cluster.
# celery + gcs as DAG source (you need to update to point to your gcs bucket)
$ kubectl apply -f hack/sample/mysql-celery-gcs/cluster.yaml
$ kubectl port-forward mcg-cluster-airflowui-0 8081:8080
$ kubectl get airflowcluster/mcg-cluster -o yaml 
```

#### Deploy Postgres based samples

```bash
# deploy base components first
$ kubectl apply -f hack/sample/postgres-celery/base.yaml
# after 30-60s deploy cluster components
# using celery + git as DAG source
$ kubectl apply -f hack/sample/postgres-celery/cluster.yaml
# port forward to access the UI
$ kubectl port-forward pc-cluster-airflowui-0 8080:8080
# port forward to access the Flower
$ kubectl port-forward pc-cluster-flower-0 5555:5555
# get status of the CRs
$ kubectl get airflowbase/pc-base -o yaml
$ kubectl get airflowcluster/pc-cluster -o yaml

# Against the same mc-base, we could deploy another cluster.
# celery + gcs as DAG source (you need to update to point to your gcs bucket)
$ kubectl apply -f hack/sample/mysql-celery-gcs/cluster.yaml
$ kubectl port-forward mcg-cluster-airflowui-0 8081:8080
$ kubectl get airflowcluster/mcg-cluster -o yaml
```

#### Running CloudSQL based samples
CloudSQL(mysql)  needs to be setup on your project.
A root password needs to be created for the CloudSQL.
The information about the project, region, instance needs to be updated in hack/samples/cloudsql-celery/base.yaml.
A secret containing the root password as "rootpassword" needs to be created with the name "cc-base-sql" (base.name + "-sql"). Update the hack/sample/cloudsql-celery/sqlproxy-secret.yaml

```bash
# create secret
$ kubectl apply -f hack/sample/cloudsql-celery/sqlproxy-secret.yaml
# deploy base components first
$ kubectl apply -f hack/sample/cloudsql-celery/base.yaml
# after 30-60s deploy cluster components
$ kubectl apply -f hack/sample/cloudsql-celery/cluster.yaml
# port forward to access the UI (port 8082)
$ kubectl port-forward cc-cluster-airflowui-0 8082:8080
# get status of the CRs
$ kubectl get airflowbase/cc-base -o yaml 
$ kubectl get airflowcluster/cc-cluster -o yaml 
```

## Next steps

For more information check the [Design](https://github.com/GoogleCloudPlatform/airflow-operator/blob/master/docs/design.md) and detailed [User Guide](https://github.com/GoogleCloudPlatform/airflow-operator/blob/master/docs/userguide.md) to create your own cluster specs.
