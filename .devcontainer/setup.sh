#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

echo "Setting up Airflow development environment..."

# Install Node Version Manager (NVM)
echo "Installing NVM..."
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.6/install.sh | bash

# Source bashrc to make nvm available
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"

# Add NVM to bashrc and bash_profile for persistent access
echo 'export NVM_DIR="$HOME/.nvm"' >> ~/.bashrc
echo '[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"' >> ~/.bashrc
echo '[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"' >> ~/.bashrc

echo 'export NVM_DIR="$HOME/.nvm"' >> ~/.bash_profile
echo '[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"' >> ~/.bash_profile
echo '[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"' >> ~/.bash_profile

# Install and use Node.js 20
echo "Installing Node.js 20..."
nvm install 20
nvm use 20

# Install pnpm globally
echo "Installing pnpm..."
npm install -g pnpm

# Add pnpm to PATH in shell profiles
PNPM_HOME="/root/.local/share/pnpm"
echo "export PNPM_HOME=\"$PNPM_HOME\"" >> ~/.bashrc
echo 'case ":$PATH:" in *":$PNPM_HOME:"*) ;; *) export PATH="$PNPM_HOME:$PATH" ;; esac' >> ~/.bashrc

echo "export PNPM_HOME=\"$PNPM_HOME\"" >> ~/.bash_profile  
echo 'case ":$PATH:" in *":$PNPM_HOME:"*) ;; *) export PATH="$PNPM_HOME:$PATH" ;; esac' >> ~/.bash_profile

# Install uv for Python package management
echo "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH
export PATH="$HOME/.local/bin:$PATH"
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bash_profile

echo "Syncing Python dependencies with uv..."
uv sync
# Setup Airflow UI
echo "Setting up Airflow UI..."
cd /opt/airflow/airflow-core/src/airflow/ui
if [ -f "package.json" ]; then
    echo "Installing UI dependencies..."
    pnpm install
    echo "UI dependencies installed successfully!"
else
    echo "Warning: package.json not found in UI directory"
fi


# Setup Simple Auth Manager UI
echo "Setting up Simple Auth Manager UI..."
cd /opt/airflow/airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui
if [ -f "package.json" ]; then
    echo "Installing Simple Auth Manager UI dependencies..."
    pnpm install
    echo "Simple Auth Manager UI dependencies installed successfully!"
else
    echo "Warning: package.json not found in Simple Auth Manager UI directory"
fi

#install all dependencies
echo "Installing backend dependencies"



echo "Setup completed successfully!"
echo ""
echo "To start the development servers:"
echo "1. For main UI: cd /opt/airflow/airflow-core/src/airflow/ui && pnpm run dev --host 0.0.0.0"
echo "2. For Simple Auth Manager UI: cd /opt/airflow/airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui && pnpm run dev --host"

bash /opt/airflow/scripts/docker/entrypoint_ci.sh