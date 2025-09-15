#!/bin/bash

echo "🚀 Setting up complete Airflow environment for PR #55680..."

# Set environment variables
export PATH="/Users/alphaskynet/Library/Python/3.9/bin:$PATH"
export AIRFLOW_HOME=/tmp/airflow_test
export AIRFLOW__CORE__TEST_CONNECTION=Enabled
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__DAGS_FOLDER=/tmp/airflow_test/dags
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow_test/airflow.db

# Clean up previous runs
echo "🧹 Cleaning up previous runs..."
rm -rf /tmp/airflow_test
mkdir -p /tmp/airflow_test/dags

# Install HTTP provider
echo "📦 Installing HTTP provider..."
pip install -U apache-airflow-providers-http

# Initialize Airflow database
echo "🗄️ Initializing Airflow database..."
cd /Users/alphaskynet/Downloads/Github\ Contributions/airflow
airflow db init

# Create test connections
echo "🔗 Creating test connections..."
python3 -c "
import os
import sys
from airflow.models import Connection
from airflow.utils.session import create_session

def create_test_connections():
    with create_session() as session:
        # Delete existing test connections
        session.query(Connection).filter(Connection.conn_id.in_(['test_connection_success', 'test_connection_failure'])).delete()
        
        # Create success connection
        success_conn = Connection(
            conn_id='test_connection_success',
            conn_type='http',
            host='https://httpbin.org/anything',
            port=443,
            schema='https',
        )
        
        # Create failure connection
        failure_conn = Connection(
            conn_id='test_connection_failure',
            conn_type='http',
            host='https://invalid.invalid',
            port=443,
            schema='https',
        )
        
        session.add_all([success_conn, failure_conn])
        session.commit()
        print('✅ Test connections created successfully!')
        print(f'  - {success_conn.conn_id}: {success_conn.host} (should work)')
        print(f'  - {failure_conn.conn_id}: {failure_conn.host} (should fail)')

create_test_connections()
"

# Start Airflow backend
echo "🚀 Starting Airflow backend..."
airflow standalone &
AIRFLOW_PID=$!

# Wait for Airflow to start
echo "⏳ Waiting for Airflow to start..."
sleep 30

# Get the generated password
echo "🔑 Getting login credentials..."
if [ -f "/tmp/airflow_test/simple_auth_manager_passwords.json.generated" ]; then
    PASSWORD=$(cat /tmp/airflow_test/simple_auth_manager_passwords.json.generated | grep -o '"admin": "[^"]*"' | cut -d'"' -f4)
    echo "✅ Airflow is ready!"
    echo "🌐 Backend UI: http://localhost:8080/"
    echo "👤 Username: admin"
    echo "🔑 Password: $PASSWORD"
    echo ""
    echo "📸 Ready to test your PR #55680!"
    echo "1. Go to http://localhost:8080/"
    echo "2. Login with admin / $PASSWORD"
    echo "3. Navigate to Admin → Connections"
    echo "4. Test the connections and take screenshots"
else
    echo "❌ Failed to get password. Check Airflow logs."
fi

echo "✅ Complete setup finished!"
echo "Backend PID: $AIRFLOW_PID"

