FROM apache/airflow:3.1.8 AS builder
ARG VERSION

# Install build dependencies
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
USER airflow
COPY requirements.txt /opt/airflow/      
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir "apache-airflow[celery]==3.1.8" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.10.txt"

# cleanup
RUN find /home/airflow/.local \( -type d -a -name test -o -name tests \) \
    -o \( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
    -exec rm -rf '{}' +

# Stage 2: Runtime stage
FROM apache/airflow:3.1.8
ARG VERSION

# Copy only necessary files from builder
COPY --from=builder /home/airflow/.local /home/airflow/.local

# Install system dependencies
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git git-lfs libpq-dev tzdata \
    && git lfs install \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /workspace && chown -R airflow: /workspace
USER airflow

# Copy DAGs and set permissions (if not use gitSync)
# COPY --chown=airflow:root ./dags /opt/airflow/dags

# Set working directory
WORKDIR /opt/airflow

# Make sure all components can run
RUN airflow version
