# Development
You should have kubeconfig setup to point to your cluster.
In case you want to build the Airflow Operator from the source code, e.g., to test a fix or a feature you write, you can do so following the instructions below.

## Cloning the repo:
```bash
$ mkdir -p $GOPATH/src/k8s.io
$ cd $GOPATH/src/k8s.io
$ git clone git@github.com:GoogleCloudPlatform/airflow-operator.git
```

## Building and running locally:
```bash
# build
make build

# run locally
make run
```
## Building docker image 
#### GCP
When working with GCP ensure that gcloud is setup and gcr(container registry) is enabled for the current project.
If not set IMG env to point to the desired registry image.
```bash
# building docker image
make docker-build

# push docker image
make docker-push
```


### Non GCP
Set IMG env to point to the desired registry image.
```bash
# building docker image
make docker-build NOTGCP=true

# push docker image
make docker-push NOTGCP=true
```
## Running in cluster
```bash
# assumes kubeconfig is setup correctly
make deploy
```


## Tests

### Running local tests
Runs unit-tests locally

```bash
make test
```

### Running e2e tests
Before running e2e tests ensure that the desrired version is running on the cluster or locally.

```bash
# Start controller in cluster:
#   make docker-push
#   make deploy
# OR locally:
#   make install
#   make run
# and then run the tests

make e2e-test
```
