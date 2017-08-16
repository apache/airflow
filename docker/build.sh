IMAGE=grantnicholas/kubeairflow
TAG=${1:-latest}

if [ -f airflow.tar.gz ]; then
    echo "Not rebuilding airflow source"
else
    cd ../ && python setup.py sdist && cd docker && \
    cp ../dist/apache-airflow-1.9.0.dev0+incubating.tar.gz airflow.tar.gz
fi

docker build . --tag=${IMAGE}:${TAG}
docker push ${IMAGE}:${TAG}
