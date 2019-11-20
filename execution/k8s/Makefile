ifndef NOTGCP
  PROJECT_ID := $(shell gcloud config get-value project)
  ZONE := $(shell gcloud config get-value compute/zone)
  SHORT_SHA := $(shell git rev-parse --short HEAD)
  IMG ?= gcr.io/${PROJECT_ID}/airflow-operator:${SHORT_SHA}
endif

# Image URL to use all building/pushing image targets

all: test manager

# Run tests
test: generate fmt vet manifests
	ln -s ../../../templates/ pkg/controller/airflowbase/ || true
	ln -s ../../../templates/ pkg/controller/airflowcluster/ || true
	go test ./pkg/... ./cmd/... -coverprofile cover.out

# Build manager binary
manager: generate fmt vet
	go build -o bin/manager k8s.io/airflow-operator/cmd/manager

# Run against the configured Kubernetes cluster in ~/.kube/config
run: generate fmt vet
	go run ./cmd/manager/main.go

# Run against the configured Kubernetes cluster in ~/.kube/config
debug: generate fmt vet
	dlv debug cmd/manager/main.go

# Install CRDs into a cluster
install: manifests
	kubectl apply -f config/crds
	kubectl apply -f hack/appcrd.yaml

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
deploy: install
	kustomize build config/default | kubectl apply -f -

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
undeploy: manifests
	kustomize build config/default | kubectl delete -f -
	kubectl delete -f config/crds || true
	kubectl delete -f hack/appcrd.yaml || true

# Generate manifests e.g. CRD, RBAC etc.
manifests:
	go run vendor/sigs.k8s.io/controller-tools/cmd/controller-gen/main.go all

# Run go fmt against code
fmt:
	go fmt ./pkg/... ./cmd/...

# Run go vet against code
vet:
	go vet ./pkg/... ./cmd/...

# Generate code
generate:
	echo ${IMG}
	go generate ./pkg/... ./cmd/...

# Build the docker image
docker-build: test
	docker build . -t ${IMG}
	@echo "updating kustomize image patch file for manager resource"
	sed -i'' -e 's@image: .*@image: '"${IMG}"'@' ./config/default/manager_image_patch.yaml

# Push the docker image
docker-push: docker-build
	docker push ${IMG}


e2e-test:
	kubectl get namespace airflowop-system || kubectl create namespace airflowop-system
	go test -v -timeout 20m test/e2e/base_test.go --namespace airflowop-system
	go test -v -timeout 20m test/e2e/cluster_test.go --namespace airflowop-system

e2e-test-gcp:
	kubectl get namespace airflowop-system || kubectl create namespace airflowop-system
	kubectl apply -f hack/sample/cloudsql-celery/sqlproxy-secret.yaml -n airflowop-system
	go test -v -timeout 20m test/e2e/gcp_test.go --namespace airflowop-system
