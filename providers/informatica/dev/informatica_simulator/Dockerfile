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

# Get the image
FROM python:3.10.10

# Set the working directory
WORKDIR /code

# Copy the requirements package
COPY ./requirements.txt /code/requirements.txt

# Download the python package requirements in build process
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy the application
COPY ./app /code/app

# Expose port 8082
EXPOSE 8082

# Start the server when the container is launched
CMD ["uvicorn", "app.main:app","--proxy-headers", "--host", "0.0.0.0", "--port", "8082"]
