# FROM apache/airflow:2.10.2-python3.9

# USER root
# COPY requirements.txt /requirements.txt

# USER airflow
# RUN pip install --no-cache-dir -r /requirements.txt

# FROM apache/airflow:2.10.2-python3.9

# USER root

# COPY requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt

# USER airflow
# FROM apache/airflow:2.10.2-python3.9

# USER airflow

# COPY requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt
FROM apache/airflow:2.10.2-python3.9

USER root

# Install system dependencies required by ML libraries
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libgomp1 \
    libopenblas-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt