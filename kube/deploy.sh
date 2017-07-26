IMAGE=${1:-grantnicholas/kubeairflow}
TAG=${2:-latest}

mkdir -p .generated
kubectl apply -f postgres.yaml
sed "s#{{docker_image}}#$IMAGE:$TAG#g" airflow.yaml.template > .generated/airflow.yaml && kubectl apply -f .generated/airflow.yaml
