#!/bin/bash


cluster_name="$1"
# Run the Python script and capture the output
output=$(python -m airflow.providers.amazon.aws.utils.eks_get_token --cluster-name "$cluster_name" 2>&1)

# Check for errors during script execution
if [ $? -ne 0 ]; then
    echo "Error running the script"
    exit 1
fi

expiration_timestamp=$(echo "$output" | grep -oP 'expirationTimestamp:\s*\K[^,]+')
token=$(echo "$output" | grep -oP 'token:\s*\K[^,]+')

json_string='{"kind": "ExecCredential","apiVersion": "client.authentication.k8s.io/v1alpha1","spec": {},"status": {"expirationTimestamp": "'"$expiration_timestamp"'","token": "'"$token"'"}}'

echo "$json_string"