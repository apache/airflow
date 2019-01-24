# ----------------
#    Docker
# ----------------
IMAGE ?= quay.io/postmates/airflow
TAG ?= 1.10.2rc2-dagconfigmap

build:
	docker build -t $(IMAGE):$(TAG) .

push:
	docker push $(IMAGE):$(TAG)

upgrade: build push

pull:
	docker pull $(IMAGE):$(TAG)

clean-image:
	docker rmi $(IMAGE):$(TAG) -f

# ----------------
#    Minikube
# ----------------

minikube-start:
	minikube start \
		--vm-driver=virtualbox \
		--cpus=6 \
		--memory=6144

minikube-init:
	$(MAKE) minikube-start
	helm init --service-account default

env-init:
	@eval ${minikube docker-env} && \
	docker login quay.io

# ----------------
#    Helm
# ----------------
HELM_RELEASE ?= datafall
HELM_LOCAL_RELEASE ?= localdatafall
HELM_DRY_RUN ?= ""

helm-template-datafall:
	helm template \
		-f deployment/environments/dev/values.yaml \
		--set datafall.devHome=${HOME} \
		-n $(HELM_RELEASE) \
		deployment/helm/datafall

helm-upgrade-datafall:
		helm install deployment/helm/datafall --name $(HELM_RELEASE) \
	  --values deployment/environments/dev/values.yaml \
		--kube-context minikube \
		--set datafall.devHome=${HOME} \
		--set datafall.image.tag=$(BASE_TAG) \
		--set secrets.postalMainPassword=$(shell bash deployment/base/get-flow-secret.sh POSTAL_MAIN_PASSWORD | base64) \
		--set secrets.airflowdbPassword=cm9vdA== \
		--set secrets.airflowFernetKey=NDZCS0pvUVlsUFBPZXhxME9oRFpuSWxOZXBLRmY4N1dGd0xiZnpxRERobz0K \
		--set secrets.airflowSecretKey=dGVtcG9yYXJ5LWtleQo= \
		--set secrets.githubUsername=$(GITHUB_FLOW_USER) \
	  --set secrets.githubPassword=$(GITHUB_FLOW_PASSWORD) \
		--set secrets.gitCookieFile=$(GIT_COOKIE) \
		$${HELM_DRY_RUN:+"--dry-run"} --namespace default

helm-delete-datafall:
	@if helm list | grep -e ^$(HELM_RELEASE) >/dev/null; then helm delete --purge $(HELM_RELEASE); fi

# ----------------
#    Commands
# ----------------

hooks:
	bin/setup_hooks.sh

init:
	@echo "### Initializing local Flow development environment..." && echo
	@if ! brew list | grep kubernetes-cli >/dev/null; then brew install kubernetes-cli; else brew upgrade kubernetes-cli || : ; fi
	@if ! brew list | grep kubernetes-helm >/dev/null; then brew install kubernetes-helm; else brew upgrade kubernetes-helm || : ; fi
	@if ! brew list | grep stern >/dev/null; then brew install stern; else brew upgrade stern || : ; fi
	$(MAKE) minikube-init
	$(MAKE) pull-base
	@echo && echo "### ... environment initialized."

start: minikube-start

pull-images: pull-base pull-worker

deploy: helm-upgrade-datafall

refresh:
	$(MAKE) undeploy
	$(MAKE) delete-pods
	$(MAKE) delete-pods
	@echo && echo "### Waiting while deploy is purged...."
	@sleep 30
	@echo "### ... redeploying..." && echo
	$(MAKE) deploy

status:
	@bash scripts/local-status.sh

undeploy: helm-delete-datafall

delete-pods:
	kubectl delete --all pods --namespace=default

ui:
	minikube service $(HELM_RELEASE)

exec:
	@eval ${minikube docker-env} && \
		docker exec -it $(shell docker ps | grep webserver | awk '{print $$1}') bash

mock-test:
	@echo "TODO: This is a mock test step that will always pass: it is a placeholder till we get lightweight airflow tests that can run in the drone pipeline"

test:
	@eval ${minikube docker-env} && \
		docker exec -it $(shell docker ps | grep webserver | awk '{print $$1}') bash -c "cd /postmates/datafall/tests && bash run_tests.sh"

stop:
	minikube stop

