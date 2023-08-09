#!/bin/bash

echo "Downloading DAGs from S3 bucket"
aws s3 sync "$S3_URL" "$CONTAINER_DAG_PATH"

exec "$@"

