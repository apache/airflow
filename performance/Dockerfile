FROM python:3.7-slim-buster

RUN apt-get update --allow-unauthenticated \
    && apt-get install --no-install-recommends -y \
        apt-transport-https \
        ca-certificates \
        curl \
        git \
        unzip \
    && apt-get autoremove -yqq --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies
COPY performance_scripts/README.md /app/performance_scripts/
COPY streaming/requirements.txt /app/streaming/
COPY requirements.txt /app/
COPY setup.py /app/

RUN pip install "pip==20.2.4"
RUN pip install -e .

# Install app with python namespace i.e. performance_scripts, streaming, scripts
COPY . /app/
RUN pip install -e .
