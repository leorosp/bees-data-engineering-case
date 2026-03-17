FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-dev.txt ./
COPY pyproject.toml ./
COPY README.md ./
COPY src ./src
COPY scripts ./scripts
COPY examples ./examples
COPY dashboard ./dashboard
COPY orchestration ./orchestration
COPY docs ./docs
COPY tests ./tests

RUN pip install --upgrade pip \
    && pip install -r requirements-dev.txt

EXPOSE 8501

CMD ["python", "scripts/run_local_pyspark_demo.py"]
